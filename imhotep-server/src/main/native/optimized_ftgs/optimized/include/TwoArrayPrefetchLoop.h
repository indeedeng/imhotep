
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


