/*
 * metrics_inverter.h
 *
 *  Created on: Mar 11, 2015
 *      Author: darren
 */
#pragma once

#include <stdint.h>


int invert_long_metric(int64_t* restrict buffer,
                        const int64_t * restrict terms,
                        const int32_t* restrict n_docs,
                        const uint8_t* restrict doc_list_address,
                        const int64_t* restrict offsets,
                        int n_terms);
int invert_int_metric(int32_t* restrict buffer,
                        const int32_t * restrict terms,
                        const int32_t* restrict n_docs,
                        const uint8_t* restrict doc_list_address,
                        const int64_t* restrict offsets,
                        int n_terms);
int invert_short_metric(int16_t* restrict buffer,
                        const int16_t * restrict terms,
                        const int32_t* restrict n_docs,
                        const uint8_t* restrict doc_list_address,
                        const int64_t* restrict offsets,
                        int n_terms);
int invert_byte_metric(int8_t* restrict buffer,
                        const int8_t * restrict terms,
                        const int32_t* restrict n_docs,
                        const uint8_t* restrict doc_list_address,
                        const int64_t* restrict offsets,
                        int n_terms);
int invert_bitfield_metric(int64_t* restrict buffer,
                           const int32_t n_docs,
                           const uint8_t* restrict doc_list_address,
                           const int64_t offset);
