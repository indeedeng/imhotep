static __v16qi row_element_extract(__v16qi* data_array, int row_size, int row_idx,
                                   int element_idx);
static void process_arrayA_data(packed_shard_t* data_desc, __v16qi data_element,
                                __m128i* save_buffer_start, int element_idx);
static __m128i* calc_save_addr(packed_shard_t* data_desc, __m128i* data_array, int row_size,
                               int row_idx, int element_idx);
static void process_arrayB_data(packed_shard_t* data_desc, __m128i data_element,
                                __m128i* save_buffer_start, int element_idx);


static inline void arrayA_loop_core(packed_shard_t* data_desc,
                                    __v16qi* data_array,
                                    __m128i* store_array,
                                    int row_size,
                                    int row_idx,
                                    int element_idx)
{
    __v16qi data_element;
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
    save_buffer_start = calc_save_addr(data_desc, data_array, row_size, row_idx, element_idx);
    /* process data */
    process_arrayB_data(data_desc, data_element, save_buffer_start, element_idx);
}

#define PREFETCH(data_array, row_size, row_idx, element_idx)                               \
        _mm_prefetch(data_array + (row_idx*row_size + element_idx), _MM_HINT_T0);

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
                               __v16qi *data_array,
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
                               __v16qi *data_array,
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
        arrayB_loop_core(data_desc, data_array, store_array, row_size, row_idx, element_idx);               \
    }
    ROW_LOOP_NO_PREFETCH(data_desc, data_array, store_array, row_size, row_idx);
}

