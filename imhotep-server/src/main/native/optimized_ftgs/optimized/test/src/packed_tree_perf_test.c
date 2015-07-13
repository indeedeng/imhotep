#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include "imhotep_native.h"


#define CHUNCK_SIZE                                 4096

#define LONG 1
#define INT 2
#define SHORT 3
#define BYTE 4
#define BIT 5

static long val_buf[CHUNCK_SIZE];

int *createRandIntArray(int num_elements) {
    int *result = malloc(num_elements * sizeof(int));

    for (int i = 0; i < num_elements; i++) {
        result[i] = rand();
    }

    return result;
}

int8_t *createRandByteArray(int num_elements) {
    int8_t *result = malloc(num_elements * sizeof(int));

    for (int i = 0; i < num_elements; i++) {
        result[i] = (int8_t) (rand() % 128);
    }

    return result;
}

short *createRandShortArray(int num_elements) {
    short *result = malloc(num_elements * sizeof(short));

    for (int i = 0; i < num_elements; i++) {
        result[i] = (short) (rand() % (1 << 16));
    }

    return result;
}

long *createRandLongArray(int num_elements) {
    long *result = malloc(num_elements * sizeof(long));

    for (int i = 0; i < num_elements; i++) {
        result[i] = (((long)rand()) << 32) + rand();
    }

    return result;
}

long *createBitset(int num_elements) {
    int len = (num_elements + 63) / 64;
    long *result = malloc(len * sizeof(long));

    for (int i = 0; i < len; i++) {
        result[i] = (((long)rand()) << 32) + rand();
    }

    return result;
}



void lookup_long_range(long *arr, int start, int count, long *buffer) {
    for (int i = 0; i < count; i++) {
        buffer[i] = arr[i + start];
    }
}

void lookup_int_range(int *arr, int start, int count, long *buffer) {
    for (int i = 0; i < count; i++) {
        buffer[i] = arr[i + start];
    }
}

void lookup_short_range(short *arr, int start, int count, long *buffer) {
    for (int i = 0; i < count; i++) {
        buffer[i] = arr[i + start];
    }
}

void lookup_byte_range(int8_t *arr, int start, int count, long *buffer) {
    for (int i = 0; i < count; i++) {
        buffer[i] = arr[i + start];
    }
}

void lookup_bitset_range(long *arr, int start, int count, long *buffer) {
    for (int i = 0; i < count; i++) {
        int dword_idx = (i + start) / 64;
        int bit_idx = (i + start) & 63;
        buffer[i] = !!(arr[dword_idx] & (1 << bit_idx));
    }
}

void simulate_packing(
                      packed_table_t *table,
                      void **lookups_arr,
                      int* lookup_types,
                      int n_metrics,
                      int n_docs)
{
    int i;

    for (i = 0; (i + CHUNCK_SIZE) < n_docs; i += CHUNCK_SIZE) {
        for (int j = 0; j < n_metrics; j++) {
            switch (lookup_types[j]) {
                case LONG:
                    lookup_long_range((long *)lookups_arr[j], i, CHUNCK_SIZE, val_buf);
                    break;
                case INT:
                    lookup_int_range((int *)lookups_arr[j], i, CHUNCK_SIZE, val_buf);
                    break;
                case SHORT:
                    lookup_short_range((short *)lookups_arr[j], i, CHUNCK_SIZE, val_buf);
                    break;
                case BYTE:
                    lookup_byte_range((int8_t *)lookups_arr[j], i, CHUNCK_SIZE, val_buf);
                    break;
                case BIT:
                    lookup_bitset_range((long *)lookups_arr[j], i, CHUNCK_SIZE, val_buf);
                    break;
            }
            packed_table_set_col_range(table, i, val_buf, CHUNCK_SIZE, j);
        }
    }

    int remaining = n_docs - i;
    if (remaining > 0) {
        for (int j = 0; j < n_metrics; j++) {
            switch (lookup_types[j]) {
                case LONG:
                    lookup_long_range((long *)lookups_arr[j], i, remaining, val_buf);
                    break;
                case INT:
                    lookup_int_range((int *)lookups_arr[j], i, remaining, val_buf);
                    break;
                case SHORT:
                    lookup_short_range((short *)lookups_arr[j], i, remaining, val_buf);
                    break;
                case BYTE:
                    lookup_byte_range((int8_t *)lookups_arr[j], i, remaining, val_buf);
                    break;
                case BIT:
                    lookup_bitset_range((long *)lookups_arr[j], i, remaining, val_buf);
                    break;
            }
            packed_table_set_col_range(table, i, val_buf, remaining, j);
        }
    }
}

double diff(struct timespec start, struct timespec end)
{
    struct timespec temp;
    if ((end.tv_nsec-start.tv_nsec)<0) {
        temp.tv_sec = end.tv_sec-start.tv_sec-1;
        temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
    } else {
        temp.tv_sec = end.tv_sec-start.tv_sec;
        temp.tv_nsec = end.tv_nsec-start.tv_nsec;
    }

    double time = (double)(temp.tv_sec);
    time += (double)(temp.tv_nsec) / 1000000000.0;

    return time;
}

int main(int argc, char** argv) {

    int num_metrics = 16;
    int num_docs = 10000000;
    int metric_types[num_metrics];
    void *lookups[num_metrics];

    int8_t original_idx[num_metrics];
    int64_t column_mins[num_metrics];
    int64_t column_maxes[num_metrics];
    int32_t sizes[num_metrics];

    int32_t vec_nums[num_metrics];
    int32_t offsets_in_vecs[num_metrics];

    /* create metric types */
    metric_types[0] = BIT;
    metric_types[1] = BIT;
    metric_types[2] = BIT;
    metric_types[3] = LONG;

    metric_types[4] = LONG;
    metric_types[5] = LONG;

    metric_types[6] = INT;
    metric_types[7] = BYTE;
    metric_types[8] = BYTE;
    metric_types[9] = SHORT;
    metric_types[10] = INT;
    metric_types[11] = INT;

    metric_types[12] = SHORT;
    metric_types[13] = BYTE;
    metric_types[14] = INT;
    metric_types[15] = SHORT;

    /* set size, min, max */
    int32_t current_offset = 0;
    int32_t curr_vec_num = 0;
    for (int i = 0; i < num_metrics; i++) {
        int sz;

        original_idx[i] = i;
        column_mins[i] = 0;
        column_maxes[i] = 0; //unused
        switch (metric_types[i]) {
            case LONG:
                sz = 8;
                break;
            case INT:
                sz = 4;
                break;
            case SHORT:
                sz = 2;
                break;
            case BYTE:
                sz = 1;
                break;
            case BIT:
                sz = 0;
                break;
            default:
                sz = 0;
        }
        sizes[i] = sz;

        if (current_offset + sz > 16) {
            current_offset = 0;
            curr_vec_num ++;
        }
        vec_nums[i] = curr_vec_num;
        offsets_in_vecs[i] = current_offset;

        current_offset += sz;
    }

    /* create metric lookups */
    struct timespec start;
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    for (int i = 0; i < num_metrics; i++) {
        switch (metric_types[i]) {
            case LONG:
                lookups[i] = createRandLongArray(num_docs);
                break;
            case INT:
                lookups[i] = createRandIntArray(num_docs);
                break;
            case SHORT:
                lookups[i] = createRandShortArray(num_docs);
                break;
            case BYTE:
                lookups[i] = createRandByteArray(num_docs);
                break;
            case BIT:
                lookups[i] = createBitset(num_docs);
                break;
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    fprintf(stderr, "created lookups: %f \n", diff(start,end));

    /* create packed table */
    clock_gettime(CLOCK_MONOTONIC, &start);

    packed_table_t *table = packed_table_create(num_docs,
                                                column_mins,
                                                column_maxes,
                                                sizes,
                                                vec_nums,
                                                offsets_in_vecs,
                                                original_idx,
                                                num_metrics,
                                                0);

    clock_gettime(CLOCK_MONOTONIC, &end);
    fprintf(stderr, "created packed table: %f \n", diff(start,end));


    /* packe the table */
    clock_gettime(CLOCK_MONOTONIC, &start);

    simulate_packing(table, lookups, metric_types, num_metrics, num_docs);

    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("runtime: %f \n", diff(start,end));

    return 0;
}
