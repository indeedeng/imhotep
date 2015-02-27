
#define LOOP_CORE(data_desc, data_array, row_size, row_idx, element_idx)                       \
{                                                                                              \
    __m128i data_element;                                                                      \
    __m128i* save_buffer_start;                                                                \
                                                                                               \
    /* load data  */                                                                           \
    data_element = row_element_extract(data_array, row_size, row_idx, element_idx);            \
    /* calculate where to write the data */                                                    \
    save_buffer_start = calc_save_addr(data_desc, row_idx, element_idx);                       \
    /* process data */                                                                         \
    process_data(data_desc, data_element, save_buffer_start, element_idx);                     \
}


#define PREFETCH(data_array, row_size, row_idx, element_idx)                               \
        _mm_prefetch(data_array + (row_idx*row_size + element_idx), _MM_HINT_T0);

#define ROW_LOOP_WITH_PREFETCH(data_desc, data_array, row_size, row_idx, prefetch_idx)         \
{                                                                                              \
    /* loop through row elements */                                                            \
    int element_idx;                                                                           \
    for (element_idx = 0; element_idx < row_size - 4; element_idx += 4)                        \
    {                                                                                          \
        LOOP_CORE(data_desc, data_array, row_size, row_idx, element_idx + 0);                  \
                                                                                               \
        LOOP_CORE(data_desc, data_array, row_size, row_idx, element_idx + 1);                  \
                                                                                               \
        LOOP_CORE(data_desc, data_array, row_size, row_idx, element_idx + 2);                  \
                                                                                               \
        LOOP_CORE(data_desc, data_array, row_size, row_idx, element_idx + 3);                  \
                                                                                               \
        /* prefetch once per cache line */                                                     \
        PREFETCH(data_array, row_size, prefetch_idx, element_idx);                             \
    }                                                                                          \
                                                                                               \
    /* prefetch the final cache line */                                                        \
    if (element_idx < row_size) {                                                              \
        PREFETCH(data_array, row_size, prefetch_idx, element_idx);                             \
    }                                                                                          \
    /* loop through the remaining row elements */                                              \
    for (; element_idx < row_size; element_idx ++)                                             \
    {                                                                                          \
        LOOP_CORE(data_desc, data_array, row_size, row_idx, element_idx);                      \
    }                                                                                          \
}

#define ROW_LOOP_NO_PREFETCH(data_desc, data_array, row_size, row_idx)                         \
{                                                                                              \
    /* loop through row elements */                                                            \
    int element_idx;                                                                           \
    for (element_idx = 0; element_idx < row_size - 4; element_idx += 4)                        \
    {                                                                                          \
        LOOP_CORE(data_desc, data_array, row_size, row_idx, element_idx + 0);                  \
                                                                                               \
        LOOP_CORE(data_desc, data_array, row_size, row_idx, element_idx + 1);                  \
                                                                                               \
        LOOP_CORE(data_desc, data_array, row_size, row_idx, element_idx + 2);                  \
                                                                                               \
        LOOP_CORE(data_desc, data_array, row_size, row_idx, element_idx + 3);                  \
    }                                                                                          \
                                                                                               \
    /* loop through the remaining row elements */                                              \
    for (; element_idx < row_size; element_idx ++)                                             \
    {                                                                                          \
        LOOP_CORE(data_desc, data_array, row_size, row_idx, element_idx);                      \
    }                                                                                          \
}


static void prefetch_and_process_arrays(DESC_TYPE data_desc,
                                     __m128i* data_array,
                                     int* doc_id_buffer,
                                     int num_rows,
                                     int row_size)
{
    /*
     * calculate the number of rows to prefetch to keep the total number
     * of prefetches to PREFETCH_CACHE_LINES
     */
    const int prefetch_rows = PREFETCH_CACHE_LINES / ((row_size + 3) / 4);

    /* loop through rows, prefetching */
	for (int row_counter = 0; row_counter < num_rows - prefetch_rows; row_counter ++) {
    {
        int row_idx = LOAD_ROW_IDX(row_counter, doc_id_buffer);
        int prefetch_idx = LOAD_ROW_IDX(row_counter + prefetch_rows, doc_id_buffer);

        /* loop through row elements */
        ROW_LOOP_WITH_PREFETCH(data_desc, data_array, row_size, row_idx, prefetch_idx);
    }

    /* loop through final rows with no prefect */
	for (int row_counter = num_rows - prefetch_rows; row_counter < num_rows; row_counter ++) {
    {
        int row_idx = LOAD_ROW_IDX(row_counter);

        /* loop through row elements */
        ROW_LOOP_NO_PREFETCH(data_desc, data_array, row_size, row_idx);
    }
}
