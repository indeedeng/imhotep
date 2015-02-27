
// element indexer
#define LOOP_CORE(data_desc, vector_type, save_buffer_type, data_array, row_size, row_idx, element_idx)  \
{                                                                                                        \
    vector_type data_element;                                                                            \
    save_buffer_type save_buffer_start;                                                                  \
                                                                                                         \
    /* load data  */                                                                                     \
    data_element = row_element_extract(data_array, row_size, row_idx, element_idx);                      \
    /* calculate where to write the data */                                                              \
    save_buffer_start = calc_save_addr(data_desc, row_idx, element_idx);                                 \
    /* process data */                                                                                   \
    process_data(data_desc, data_element, save_buffer_start, element_idx);                               \
}


#define ROW_LOOP_WITH_PREFETCH(data_desc, vector_type, save_buffer_type, data_array, row_size, row_idx)  \
{                                                                                                        \
    /* loop through row elements */                                                                      \
    int element_idx;                                                                                     \
    for (element_idx = 0; element_idx < row_size - 4; element_idx += 4)                                  \
    {                                                                                                    \
        LOOP_CORE(data_desc, vector_type, save_buffer_type,                                              \
                  data_array, row_size, row_idx, element_idx + 0);                                       \
                                                                                                         \
        LOOP_CORE(data_desc, vector_type, save_buffer_type,                                              \
                  data_array, row_size, row_idx, element_idx + 1);                                       \
                                                                                                         \
        LOOP_CORE(data_desc, vector_type, save_buffer_type,                                              \
                  data_array, row_size, row_idx, element_idx + 2);                                       \
                                                                                                         \
        LOOP_CORE(data_desc, vector_type, save_buffer_type,                                              \
                  data_array, row_size, row_idx, element_idx + 3);                                       \
                                                                                                         \
        /* prefetch once per cache line */                                                               \
        PREFETCH(data_array, row_size, row_idx, element_idx);                                            \
    }                                                                                                    \
                                                                                                         \
    /* prefetch the final cache line */                                                                  \
    if (element_idx < row_size) {                                                                        \
        PREFETCH(data_array, row_size, row_idx, element_idx);                                            \
    }                                                                                                    \
    /* loop through the remaining row elements */                                                        \
    for (; element_idx < row_size; element_idx ++)                                                       \
    {                                                                                                    \
        LOOP_CORE(data_desc, vector_type, save_buffer_type,                                              \
                  data_array, row_size, row_idx, element_idx);                                           \
    }                                                                                                    \
}

#define ROW_LOOP_NO_PREFETCH(data_desc, vector_type, save_buffer_type, data_array, row_size, row_idx)    \
{                                                                                                        \
    /* loop through row elements */                                                                      \
    int element_idx;                                                                                     \
    for (element_idx = 0; element_idx < row_size - 4; element_idx += 4)                                  \
    {                                                                                                    \
        LOOP_CORE(data_desc, vector_type, save_buffer_type,                                              \
                  data_array, row_size, row_idx, element_idx + 0);                                       \
                                                                                                         \
        LOOP_CORE(data_desc, vector_type, save_buffer_type,                                              \
                  data_array, row_size, row_idx, element_idx + 1);                                       \
                                                                                                         \
        LOOP_CORE(data_desc, vector_type, save_buffer_type,                                              \
                  data_array, row_size, row_idx, element_idx + 2);                                       \
                                                                                                         \
        LOOP_CORE(data_desc, vector_type, save_buffer_type,                                              \
                  data_array, row_size, row_idx, element_idx + 3);                                       \
    }                                                                                                    \
                                                                                                         \
    /* loop through the remaining row elements */                                                        \
    for (; element_idx < row_size; element_idx ++)                                                       \
    {                                                                                                    \
        LOOP_CORE(data_desc, vector_type, save_buffer_type,                                              \
                  data_array, row_size, row_idx, element_idx);                                           \
    }                                                                                                    \
}


{
    /*
     * caclulate the number of rows to prefetch to keep the total number
     * of prefetches to PREFETCH_CACHE_LINES
     */
    const int prefetch_rows = PREFETCH_CACHE_LINES / ((row_size + 3) / 4);

    /* loop through rows, prefetching */
	for (int row_counter = 0; row_counter < num_rows - prefetch_rows; row_counter ++) {
    {
        int row_idx = LOAD_ROW_IDX(row_counter);

        /* loop through row elements */
        ROW_LOOP_WITH_PREFETCH(data_desc, vector_type, save_buffer_type,
                               data_array, row_size, row_idx);
    }

    /* loop through final rows with no prefect */
	for (int row_counter = num_rows - prefetch_rows; row_counter < num_rows; row_counter ++) {
    {
        int row_idx = LOAD_ROW_IDX(row_counter);

        /* loop through row elements */
        ROW_LOOP_NO_PREFETCH(data_desc, vector_type, save_buffer_type,
                               data_array, row_size, row_idx);
    }
}




/*
 * Two array loop
 */
{
    /*
     * caclulate the number of rows to prefetch to keep the total number
     * of prefetches to PREFETCH_CACHE_LINES
     */
    const int prefetch_rows = PREFETCH_CACHE_LINES / ((row_size + 3) / 4);


    // read a, process a, save to buffer, prefetch a, prefetch b
    // read a, process a, save to buffer, read b, process b, save b, prefetch a, prefetch b
    // read a, process a, save to buffer, read b, process b, save b, prefetch b
    // read b, process b, save b

    /* loop through A rows, prefetching */
    // load value from A, save, prefetch B
	for (int row_counter = 0; row_counter < num_rows - prefetch_rows; row_counter ++) {
    {
        int row_idx = LOAD_ROW_IDX(row_counter);

        /* loop through row elements */
        ROW_LOOP_WITH_PREFETCH(data_desc, vector_type, save_buffer_type,
                               data_array, row_size, row_idx);
    }

    /* loop through final A rows with no prefetch */
    // load value from A, save, prefetch B
	for (int row_counter = num_rows - prefetch_rows; row_counter < num_rows; row_counter ++) {
    {
        int row_idx = LOAD_ROW_IDX(row_counter);

        /* loop through row elements */
        ROW_LOOP_NO_PREFETCH(data_desc, vector_type, save_buffer_type,
                               data_array, row_size, row_idx);
    }

    /* loop through B rows, prefetching */
	for (int row_counter = 0; row_counter < num_rows - prefetch_rows; row_counter ++) {
    {
        int row_idx = LOAD_ROW_IDX(row_counter);

        /* loop through row elements */
        ROW_LOOP_WITH_PREFETCH(data_desc, vector_type, save_buffer_type,
                               data_array, row_size, row_idx);
    }

    /* loop through final B rows with no prefetch */
	for (int row_counter = num_rows - prefetch_rows; row_counter < num_rows; row_counter ++) {
    {
        int row_idx = LOAD_ROW_IDX(row_counter);

        /* loop through row elements */
        ROW_LOOP_NO_PREFETCH(data_desc, vector_type, save_buffer_type,
                               data_array, row_size, row_idx);
    }
}





/*
 * process data function for unpacking data
 *
 */
void my_process_data( data_desc_type data_desc,
                      vector_type data_element,
                      save_buffer_type save_buffer_start,
                      int element_idx)
{
    uint32_t vector_index = desc->unpacked_idx_for_packed_idx[element_idx];
	
	uint32_t * restrict n_metrics_per_vector = desc->n_metrics_per_vector;
	__v2di *mins = (__v2di *)desc->metric_mins;
	__v16qi *shuffle_vecs = desc->shuffle_vecs_get2;
	
    for (int32_t k = 0; k < n_metrics_per_vector[element_idx]; k += 2) {
        __m128i data;
        __m128i decoded_data;

        data = unpack_2_metrics(data_element, shuffle_vecs[vector_index]);
        decoded_data = _mm_add_epi64(data, mins[vector_index]);

        /* save data into buffer */
        save_buffer_start[vector_index] = decoded_data;

        vector_index++;
    }
}
