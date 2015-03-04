//#include <stdlib.h>
//#include "imhotep_native.h"
//
//#define TGS_BUFFER_SIZE                         2048
//
//void remapDocsInTargetGroups(struct index_slice_info *shard,
//						int *remappings,
//						int placeHolderGroup)
//{
//	int n = 0;
//	while (n < shard->n_docs_in_slice) {
//		int n = docIdStream.fillDocIdBuffer(docIdBuf);
//
//		multiRemapCore(shard->packed_metrics, new_grp_ids, doc_ids, num_docs, remappings, placeHolderGroup);
//	}
//}
//
//int multiRemapCore(packed_table_t *table,
//                    int *new_grp_ids,
//                    int *doc_ids,
//                    int num_docs,
//                    int *remappings,
//                    int placeHolderGroup)
//{
//	int row_size = table->row_size;
//
//	for (int i = 0; i < num_docs; i++) {
//		int doc_id = doc_ids[i];
//		int old_group = packed_table_get_group(table, doc_id);
//
//		if (old_group != 0) {
//			int currentGroup = new_grp_ids[doc_id];
//			if (placeHolderGroup > 0) {
//				if (currentGroup != placeHolderGroup) {
//					return -1;
//				}
//			}
//			new_grp_ids[doc_id] = (currentGroup < remappings[old_group])
//			                        ? currentGroup
//			                        : remappings[old_group];
//		}
//	}
//
//	return 0;
//}
//
//void multiRemap()
//{
//	uint32_t buffer[TGS_BUFFER_SIZE];
//	int remaining;      /* num docs remaining */
//	uint8_t *read_addr;
//	int last_value;     /* delta decode tracker */
//	struct index_slice_info *infos = desc->slices;
//	struct index_slice_info *slice;
//
//	slice = &infos[i];
//	remaining = slice->n_docs_in_slice;
//	read_addr = slice->slice;
//	last_value = 0;
//	while (remaining > 0) {
//		int count;
//		int bytes_read;
//
//		count = (remaining > TGS_BUFFER_SIZE) ? TGS_BUFFER_SIZE : remaining;
//		bytes_read = read_ints(last_value, read_addr, buffer, count);
//		read_addr += bytes_read;
//		remaining -= count;
//
//		multiRemapCore(shard, new_grp_ids, buffer, count, remappings, placeHolderGroup);
//		last_value = buffer[count - 1];
//	}
//
//}
