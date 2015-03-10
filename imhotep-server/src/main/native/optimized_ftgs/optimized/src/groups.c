#include <stdlib.h>
#include "imhotep_native.h"
#include "varintdecode.h"

#include <stdio.h>

#define likely(x)   __builtin_expect((x),1)
#define unlikely(x) __builtin_expect((x),0)

#define TGS_BUFFER_SIZE 2048

static int multi_remap_core(packed_table_t* doc_id_group,
                            int*            results,
                            uint32_t*       doc_ids,
                            int             n_docs,
                            int*            remappings,
                            long            placeholder_group)
{
  for (int count = 0; count < n_docs; ++count) {

    if (likely(n_docs - count > PREFETCH_DISTANCE)) {
      const int prefetch_doc_id = doc_ids[count + PREFETCH_DISTANCE];
      __v16qi * prefetch_addr   = packed_table_get_row_addr(doc_id_group, prefetch_doc_id);
      _mm_prefetch(prefetch_addr, _MM_HINT_T0);
      _mm_prefetch(&results[prefetch_doc_id], _MM_HINT_T0);
    }

    const int  doc_id    = doc_ids[count];
    const long old_group = packed_table_get_group(doc_id_group, doc_id);
    if (likely(old_group != 0)) {
      const long current_group = results[doc_id];
      if (unlikely(placeholder_group > 0 && current_group != placeholder_group)) {
        return -1;
      }
      if (likely(current_group < remappings[old_group])) {
        results[doc_id] = current_group;
      }
      else {
        results[doc_id] = remappings[old_group];
      }
    }
  }
  return 0;
}

int remap_docs_in_target_groups(packed_table_t* doc_id_group,
                                int*            results,
                                uint8_t*        delta_compressed_doc_ids,
                                size_t          n_doc_ids,
                                int*            remappings,
                                long            placeholder_group)
{
  uint32_t doc_id_buf[TGS_BUFFER_SIZE];
  int      n_docs_remaining = n_doc_ids;
  int      last_value       = 0;
  uint8_t* read_addr        = delta_compressed_doc_ids;

  while (n_docs_remaining > 0) {
    const int n_docs     = n_docs_remaining > TGS_BUFFER_SIZE ? TGS_BUFFER_SIZE : n_docs_remaining;
    const int bytes_read = masked_vbyte_read_loop_delta(read_addr, doc_id_buf, n_docs, last_value);
    read_addr += bytes_read;
    n_docs_remaining -= n_docs;
    if (multi_remap_core(doc_id_group, results, doc_id_buf, n_docs, remappings, placeholder_group) != 0) {
      return -1;
    }
    last_value = doc_id_buf[n_docs - 1];
  }
  return 0;
}


