#include <assert.h>
#include "imhotep_native.h"
#include "remote_output.h"
#include "varintdecode.h"

#define TGS_BUFFER_SIZE	     1024
#define PREFETCH_BUFFER_SIZE 32


/* No need to share the group stats buffer, so just keep one per session*/
/* Make sure the one we have is large enough */
static unpacked_table_t *allocate_grp_stats(struct worker_desc *desc,
                                            struct session_desc *session,
                                            packed_table_t *metric_desc)
{
    int gs_size = 2048;//row_size * session->num_groups;

    if (desc->grp_stats == NULL) {
        desc->buffer_size = gs_size;
        desc->grp_stats = unpacked_table_create(metric_desc, session->num_groups);

        if (session->temp_buf) unpacked_table_destroy(session->temp_buf);
        session->temp_buf = unpacked_table_copy_layout(desc->grp_stats, PREFETCH_BUFFER_SIZE);

        return desc->grp_stats;
    }

    assert(1 == 1);  /* we should never get here */

    if (desc->buffer_size >= gs_size) {
        // our buffer is large enough already;
        return desc->grp_stats;
    }

    unpacked_table_destroy(desc->grp_stats);
    // TODO: maybe resize smarter
    desc->buffer_size = gs_size;
    desc->grp_stats = unpacked_table_create(metric_desc, session->num_groups);

    if (session->temp_buf) unpacked_table_destroy(session->temp_buf);
    session->temp_buf = unpacked_table_copy_layout(desc->grp_stats, PREFETCH_BUFFER_SIZE);

    return desc->grp_stats;
}


void tgs_init(struct worker_desc *worker,
              struct tgs_desc *desc,
              uint8_t term_type,
              union term_union *term,
              union term_union *previous_term,
              long *addresses,
              int *docs_per_shard,
              int *shard_handles,
              int num_shard,
              struct buffered_socket *socket,
              struct session_desc *session)
{
    struct index_slice_info *infos;
    desc->term_type = term_type;
    desc->term = term;
    desc->previous_term = previous_term;
    desc->n_slices = num_shard;
    desc->socket = socket;
    infos = (struct index_slice_info *)
        calloc(num_shard, sizeof(struct index_slice_info));
    for (int i = 0; i < num_shard; i++) {
        int handle = shard_handles[i];
        infos[i].n_docs_in_slice = docs_per_shard[i];
        infos[i].doc_slice = (uint8_t *)addresses[i];
        infos[i].packed_metrics = session->shards[handle];
    }
    desc->slices = infos;
    desc->grp_buf = worker->grp_buf;
}

void tgs_destroy(struct tgs_desc *desc)
{
    free(desc->slices);
}

int tgs_execute_pass(struct worker_desc *worker,
                     struct session_desc *session,
                     struct tgs_desc *desc)
{
    uint32_t doc_id_buf[TGS_BUFFER_SIZE];
    unpacked_table_t *group_stats;
    int n_slices = desc->n_slices;
    struct index_slice_info *infos = desc->slices;

    if (desc->n_slices <= 0) {
        return -1;
    }

    group_stats = allocate_grp_stats(worker, session, infos[0].packed_metrics);
    session->current_tgs_pass->group_stats = group_stats;

    for (int i = 0; i < n_slices; i++) {
        struct index_slice_info *slice;
        int remaining;      /* num docs remaining */
        uint8_t *read_addr;
        int last_value;     /* delta decode tracker */

        slice = &infos[i];
        remaining = slice->n_docs_in_slice;
        read_addr = slice->doc_slice;
        last_value = 0;
        while (remaining > 0) {
            int count;
            int bytes_read;

            count = (remaining > TGS_BUFFER_SIZE) ? TGS_BUFFER_SIZE : remaining;
            bytes_read = masked_vbyte_read_loop_delta(read_addr, doc_id_buf, count, last_value);
            read_addr += bytes_read;
            remaining -= count;

            packed_table_t* shard_data = slice->packed_metrics;
            lookup_and_accumulate_grp_stats(shard_data,
                                            group_stats,
                                            doc_id_buf,
                                            count,
                                            desc->grp_buf,
                                            session->temp_buf);
            last_value = doc_id_buf[count - 1];
        }
    }

    struct bit_tree* non_zero_rows    = unpacked_table_get_non_zero_rows(group_stats);;
    const size_t     n_rows           = unpacked_table_get_rows(group_stats);
    uint32_t*        groups           = calloc(n_rows, sizeof(uint32_t));
    const int32_t    term_group_count = bit_tree_dump(non_zero_rows, groups, n_rows);
    int result = write_term_group_stats(session, desc, groups, term_group_count);
    free(groups);
    return result;
}
