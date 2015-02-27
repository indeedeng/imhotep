static __m128i row_element_extract(__m128i* data_array, int row_size, int row_idx, int element_idx);
static void process_arrayA_data(packed_shard_t* data_desc, __m128i data_element,
                                __m128i* save_buffer_start, int element_idx);
static __m128i* calc_save_addr(packed_shard_t* data_desc, int row_idx, int element_idx);
static void process_arrayB_data(packed_shard_t* data_desc, __m128i data_element,
                                __m128i* save_buffer_start, int element_idx);


static inline void arrayA_loop_core(packed_shard_t* data_desc,
                                    __m128i* data_array,
                                    __m128i* store_array,
                                    int row_size,
                                    int row_idx,
                                    int element_idx)
{
    __m128i data_element;
    __m128i* save_buffer_start;

    /* load data  */
    data_element = row_element_extract(data_array, row_size, row_idx, element_idx);
    /* calculate where to write the data */
    int save_buf_offset = data_desc->metrics_layout->unpacked_offset[element_idx];
    save_buffer_start = &store_array[save_buf_offset];
    /* process data */
    process_arrayA_data(data_desc, data_element, save_buffer_start, element_idx);
}

static inline void arrayB_loop_core(packed_shard_t* data_desc,
                                    __m128i *data_array,
                                    __m128i* store_array,
                                    int row_size,
                                    int row_idx,
                                    int element_idx)
{
    __m128i data_element;
    __m128i *save_buffer_start;

    /* load data  */
    data_element = data_array[element_idx];
    /* calculate where to write the data */
    save_buffer_start = calc_save_addr(data_desc, row_idx, element_idx);
    /* process data */
    process_arrayB_data(data_desc, data_element, save_buffer_start, element_idx);
}

#define PREFETCH(data_array, row_size, row_idx, element_idx)                               \
        _mm_prefetch(data_array + (row_idx*row_size + element_idx), _MM_HINT_T0);

//static inline void row_loop_with_prefetch(DESC_TYPE data_desc,
//                                          __m128i *data_array,
//                                          __m128i* store_array,
//                                          int row_size,
//                                          int row_idx)
#define ROW_LOOP_WITH_PREFETCH(data_desc, data_array, store_array, row_size, row_idx, prefetch_idx) \
{                                                                                             \
    /* loop through row elements */                                                           \
    int element_idx;                                                                          \
    for (element_idx = 0; element_idx < row_size - 4; element_idx += 4)                       \
    {                                                                                         \
        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx + 0);    \
                                                                                              \
        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx + 1);    \
                                                                                              \
        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx + 2);    \
                                                                                              \
        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx + 3);    \
                                                                                              \
        /* prefetch once per cache line */                                                    \
        PREFETCH(data_array, row_size, prefetch_idx, element_idx);                            \
    }                                                                                         \
                                                                                              \
    /* prefetch the final cache line */                                                       \
    if (element_idx < row_size) {                                                             \
        PREFETCH(data_array, row_size, prefetch_idx, element_idx);                            \
    }                                                                                         \
    /* loop through the remaining row elements */                                             \
    for (; element_idx < row_size; element_idx ++)                                            \
    {                                                                                         \
        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx);        \
    }                                                                                         \
}

//static inline void row_loop_no_prefetch(DESC_TYPE data_desc,
//                                        __m128i *data_array,
//                                        __m128i* store_array,
//                                        int row_size,
//                                        int row_idx)
#define ROW_LOOP_NO_PREFETCH(data_desc, data_array, store_array, row_size, row_idx)           \
{                                                                                             \
    /* loop through row elements */                                                           \
    int element_idx;                                                                          \
    for (element_idx = 0; element_idx < row_size - 4; element_idx += 4)                       \
    {                                                                                         \
        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx + 0);    \
                                                                                              \
        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx + 1);    \
                                                                                              \
        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx + 2);    \
                                                                                              \
        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx + 3);    \
    }                                                                                         \
                                                                                              \
    /* loop through the remaining row elements */                                             \
    for (; element_idx < row_size; element_idx ++)                                            \
    {                                                                                         \
        LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx);        \
    }                                                                                         \
}


static inline void arrayA_rlwp(DESC_TYPE data_desc,
                                        __m128i *data_array,
                                        __m128i* store_array,
                                        int row_size,
                                        int row_idx,
                                        int prefetch_idx)
{
    #undef LOOP_CORE
    #define LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx)      \
    {                                                                                          \
        arrayA_loop_core(data_desc, data_array, store_array, row_size, row_idx, element_idx);  \
    }
    ROW_LOOP_WITH_PREFETCH(data_desc, data_array, store_array, row_size, row_idx, prefetch_idx);
}

static inline void arrayA_rlnp(DESC_TYPE data_desc,
                                        __m128i *data_array,
                                        __m128i* store_array,
                                        int row_size,
                                        int row_idx)
{
    #undef LOOP_CORE
    #define LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx)      \
    {                                                                                          \
        arrayA_loop_core(data_desc, data_array, store_array, row_size, row_idx, element_idx);  \
    }
    ROW_LOOP_NO_PREFETCH(data_desc, data_array, store_array, row_size, row_idx);
}

static inline void arrayB_rlwp(DESC_TYPE data_desc,
                                        __m128i *data_array,
                                        __m128i* store_array,
                                        int row_size,
                                        int row_idx,
                                        int prefetch_idx)
{
    #undef LOOP_CORE
    #define LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx)      \
    {                                                                                          \
        arrayB_loop_core(data_desc, data_array, store_array, row_size, row_idx, element_idx);  \
    }
    ROW_LOOP_WITH_PREFETCH(data_desc, data_array, store_array, row_size, row_idx, prefetch_idx);
}

static inline void arrayB_rlnp(DESC_TYPE data_desc,
                                        __m128i *data_array,
                                        __m128i* store_array,
                                        int row_size,
                                        int row_idx)
{
    #undef LOOP_CORE
    #define LOOP_CORE(data_desc, data_array, store_array, row_size, row_idx, element_idx)      \
    {                                                                                          \
        arrayB_loop_core(data_desc, data_array, row_size, row_idx, element_idx);               \
    }
    ROW_LOOP_NO_PREFETCH(data_desc, data_array, store_array, row_size, row_idx);
}

/*
 * Two array loop
 */
static void prefetch_and_process_2_arrays(DESC_TYPE data_desc,
                                     __m128i *data_arrayA,
                                     __m128i *data_arrayB,
                                     __m128i* temp_buffer,
                                     int* doc_id_buffer,
                                     int num_rows,
                                     int arrayA_row_size,
                                     int arrayB_row_size)
{
    /*
     * calculate the number of rows to prefetch to keep the total number
     * of prefetches to PREFETCH_CACHE_LINES
     */
    int array_cache_lines_per_row = (arrayA_row_size + 3) / 4
    const int prefetch_rows = (PREFETCH_CACHE_LINES + array_cache_lines_per_row - 1)
                                         / array_cache_lines_per_row;

    /* loop through A rows, prefetching */
	for (int arrayA_row_counter = 0;
	     arrayA_row_counter < prefetch_rows;
	     arrayA_row_counter ++) {
    {
        int row_idx = LOAD_ROW_IDX(arrayA_row_counter, doc_id_buffer);
        int prefetch_idx = LOAD_ROW_IDX(arrayA_row_counter + prefetch_rows, doc_id_buffer);

        /* load value from A, save, prefetch B */
        int arrayB_row_idx = LOAD_B_ROW_IDX(data_desc, data_arrayA, arrayA_row_size, row_idx);
		/* save group into buffer */
		circular_buffer_int_put(grp_buf, arrayB_row_idx);
//        PREFETCH_ARRAYB(data_desc, arrayB_row_idx);

        /* loop through A row elements */
        arrayA_rlwp(data_desc, data_arrayA, temp_buffer, arrayA_row_size, row_idx, prefetch_idx);
    }

    /* loop through A rows, prefetching; loop through B rows */
    int arrayB_row_counter = 0;
	for (int arrayA_row_counter = prefetch_rows;
	     arrayA_row_counter < num_rows - prefetch_rows;
	     arrayA_row_counter ++, arrayB_row_counter ++) {
    {
        int row_idx = LOAD_ROW_IDX(arrayA_row_counter, doc_id_buffer);
        int prefetch_idx = LOAD_ROW_IDX(arrayA_row_counter + prefetch_rows, doc_id_buffer);

        /* load value from A, save, prefetch B */
        int arrayB_row_idx;
        LOAD_B_ROW_IDX(data_desc, data_arrayA, arrayA_row_size, row_idx, arrayB_row_idx);
		/* save group into buffer */
		circular_buffer_int_put(grp_buf, arrayB_row_idx);
//        PREFETCH_ARRAYB(data_desc, arrayB_row_idx);

        /* loop through A row elements */
        arrayA_rlwp(data_desc, data_arrayA, temp_buffer, arrayA_row_size, row_idx, prefetch_idx);

        /* get load idx */
        int arrayB_curr_idx = circular_buffer_int_get(grp_buf);
        
        /* loop through B row elements */
        arrayB_rlwp(data_desc, data_arrayB, NULL, arrayB_row_size, arrayB_curr_idx, arrayB_row_idx);
    }

    /* loop through A rows; loop through B rows */
	for (int arrayA_row_counter = num_rows - prefetch_rows;
	     arrayA_row_counter < num_rows;
	     arrayA_row_counter ++, arrayB_row_counter ++) {
    {
        int row_idx = LOAD_ROW_IDX(arrayA_row_counter, doc_id_buffer);

        /* load value from A, save, prefetch B */
        int arrayB_row_idx;
        LOAD_B_ROW_IDX(data_desc, data_arrayA, arrayA_row_size, row_idx, arrayB_row_idx);
		/* save group into buffer */
		circular_buffer_int_put(grp_buf, arrayB_row_idx);
//        PREFETCH_ARRAYB(data_desc, arrayB_row_idx);

        /* loop through A row elements */
        arrayA_rlnp(data_desc, data_arrayA, temp_buffer, arrayA_row_size, row_idx);

        /* get load idx */
        int arrayB_curr_idx = circular_buffer_int_get(grp_buf);
        
        /* loop through B row elements */
        arrayB_rlwp(data_desc, data_arrayB, NULL, arrayB_row_size, arrayB_curr_idx, arrayB_row_idx);
    }

    /* loop through final B rows with no prefetch */
	for (; arrayB_row_counter < num_rows; arrayB_row_counter ++) {
    {
        /* get load idx */
        int arrayB_curr_idx = circular_buffer_int_get(grp_buf);
        
        /* loop through B row elements */
        arrayB_rlnp(data_desc, data_arrayB, NULL, arrayB_row_size, arrayB_curr_idx);
    }
}
