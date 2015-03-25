/*
 * test_invert_metrics.c
 *
 *  Created on: Mar 24, 2015
 *      Author: darren
 */
#include <inttypes.h>
#include <stdio.h>
#include "metrics_inverter.h"

int main(int argc, char **argv) {
    uint8_t docids[21000];
    const int16_t terms[10] = {
            264,
            888,
            9585,
            16417,
            17124,
            56301,
            57077,
            63735,
            63900,
            64080,
    };
    const int freq[10] = {
            1,
            1,
            2,
            38,
            74,
            6188,
            10,
            13678,
            1,
            7
    };
    const long offsets[10] = {
            0,
            2,
            4,
            8,
            76,
            197,
            6385,
            6404,
            20082,
            20084
    };
    int16_t data[20000];

    docids[0] = 0;
    int j = 0;
    int next_id = 0;
    for (int i = 1; i < 20000; i++) {
        if (i == next_id) {
            next_id += freq[j];
            j++;
            docids[i] = i;
        } else {
            docids[i] = 1;
        }
    }

    invert_short_metric(data, terms, freq, docids, offsets, 10);
    // metrics, terms, n_docs, delta_compressed_doc_ids, offsets, terms_len);

//    printf("bitfields: 0x%" PRIu64 "\n", bitfield);
}

