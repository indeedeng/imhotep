#include <stdlib.h>
#include "imhotep_native.h"

void remapDocsInTargetGroups(struct shard_data *shard,
						int *remappings,
						int placeHolderGroup)
{
	int n = 0;
	while (n < shard->num_docs) {
		int n = docIdStream.fillDocIdBuffer(docIdBuf);

		multiRemapCore(shard, new_grp_ids, doc_ids, num_docs, remappings, placeHolderGroup);
	}
}

void multiRemapCore(struct shard_data *shard,
                    int *new_grp_ids,
                    int *doc_ids,
                    int num_docs,
                    int *remappings,
                    int placeHolderGroup)
{
	__v16qi* grp_metrics = shard->groups_and_metrics;
	struct packed_metric_desc *packing_desc = shard->metrics_layout;
	int n_vecs_per_doc = packing_desc->n_vectors_per_doc;

	for (int i = 0; i < num_docs; i++) {
		int doc_id = doc_ids[i];
		uint32_t start_idx = doc_id * n_vecs_per_doc;

		/* load group id and unpack metrics */
		struct bit_fields_and_group packed_bf_grp;
		uint32_t old_group;

		/* get group */
		packed_bf_grp = *((struct bit_fields_and_group *)&grp_metrics[start_idx]);
		old_group = packed_bf_grp.grp;

		if (old_group != 0) {
			int currentGroup = new_grp_ids[doc_id];
			if (placeHolderGroup > 0) {
				if (currentGroup != placeHolderGroup) {
					throw new
					IllegalArgumentException("Regrouping on a multi-valued field doesn't work correctly so the operation is rejected.");
				}
			}
			new_grp_ids[doc_id] = Math.min(currentGroup, remappings[old_group]);
		}
	}

}

void multiRemap()
{
	uint32_t buffer[TGS_BUFFER_SIZE];
	int remaining;      /* num docs remaining */
	uint8_t *read_addr;
	int last_value;     /* delta decode tracker */
	struct index_slice_info *infos = desc->trm_slice_infos;
	struct index_slice_info *slice;

	slice = &infos[i];
	remaining = slice->n_docs_in_slice;
	read_addr = slice->slice;
	last_value = 0;
	while (remaining > 0) {
		int count;
		int bytes_read;

		count = (remaining > TGS_BUFFER_SIZE) ? TGS_BUFFER_SIZE : remaining;
		bytes_read = read_ints(last_value, read_addr, buffer, count);
		read_addr += bytes_read;
		remaining -= count;

		multiRemapCore(shard, new_grp_ids, buffer, count, remappings, placeHolderGroup);
		last_value = buffer[count - 1];
	}

}
