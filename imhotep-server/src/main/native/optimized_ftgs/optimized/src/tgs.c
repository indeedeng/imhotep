#include <assert.h>
#include <string.h>
#include <stdio.h>
#include "imhotep_native.h"
#include "remote_output.h"
#include "varintdecode.h"

#include "high_perf_timer.h"


#define TGS_BUFFER_SIZE	     1024


void tgs_init(struct worker_desc *worker,
              struct tgs_desc *desc,
              uint8_t term_type,
              long *addresses,
              int *docs_per_shard,
              int num_shard,
              struct ftgs_outstream *stream,
              struct session_desc *session)
{
    struct index_slice_info *infos = desc->slices;

    desc->term_type = term_type;
    desc->n_slices = num_shard;
    desc->stream = stream;
    for (int i = 0; i < num_shard; i++) {
        infos[i].n_docs_in_slice = docs_per_shard[i];
        infos[i].doc_slice = (uint8_t *)addresses[i];
        infos[i].packed_metrics = session->shards[i];
    }
    desc->grp_buf = session->grp_buf;
    desc->updated_groups = session->nz_grps_buf;
//    memset(session->nz_grps_buf, 0, session->num_groups * sizeof(uint32_t));
    desc->temp_table = session->temp_buf;
//    unpacked_table_clear(session->temp_buf);
    desc->group_stats = session->grp_stats;
//    unpacked_table_clear(session->grp_stats);
}

void tgs_destroy(struct tgs_desc *desc)
{
}

int tgs_execute_pass(struct worker_desc *worker,
                     const struct session_desc *session,
                     struct tgs_desc *desc)
{
    uint32_t doc_id_buf[TGS_BUFFER_SIZE];
    const unpacked_table_t *group_stats = desc->group_stats;
    const int n_slices = desc->n_slices;
    const struct index_slice_info *infos = desc->slices;

    if (desc->n_slices <= 0) {
        /* error */
        worker->error.code = -1;
        strcpy(worker->error.str, "tgs_execute_pass: No shards.");
        return -1;
    }

    for (int i = 0; i < n_slices; i++) {
        const struct index_slice_info *slice = &infos[i];
        int remaining;      /* num docs remaining */
        uint8_t *read_addr;
        long last_value;     /* delta decode tracker */

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

            if (session->only_binary_metrics) {
                binary_lookup_and_accumulate_grp_stats(worker,
                                                       shard_data,
                                                       group_stats,
                                                       doc_id_buf,
                                                       count,
                                                       desc->grp_buf);
            } else {
                lookup_and_accumulate_grp_stats(worker,
                                                shard_data,
                                                group_stats,
                                                doc_id_buf,
                                                count,
                                                desc->grp_buf,
                                                session->temp_buf);
            }
            last_value = doc_id_buf[count - 1];
        }
    }

    struct bit_tree* non_zero_rows    = unpacked_table_get_non_zero_rows(group_stats);
    const int     n_rows           = unpacked_table_get_rows(group_stats);
    const int32_t    term_group_count = bit_tree_dump(non_zero_rows, desc->updated_groups, n_rows);
    int result;
    if (term_group_count == 0) {
        result = 0;
    } else {
        result = write_term_group_stats(session, desc, desc->updated_groups, term_group_count);
    }
    return result;
}
