#!/usr/bin/perl
`gcc -I$ENV{'JAVA_HOME'}/include/ -I$ENV{'JAVA_HOME'}/include/linux/ -c -std=c99 -mssse3 -msse3 -msse2 -O3 -funroll-loops -fPIC com_indeed_flamdex_simple_NativeDocIdBuffer.c varintdecode.c`;
`gcc -shared -W1,-soname,libvarint.so.1 -o libvarint.so.1.0.1 com_indeed_flamdex_simple_NativeDocIdBuffer.o varintdecode.o`;
`/usr/local/gcc-4.7.1/bin/gcc -I$ENV{'JAVA_HOME'}/include/ -I$ENV{'JAVA_HOME'}/include/linux/ -c -std=c99 -msse2 -DLIBDIVIDE_USE_SSE2 -O3 -funroll-loops -fPIC com_indeed_imhotep_local_NativeMetricRegroupInternals.c metricregroup.c`;
`/usr/local/gcc-4.7.1/bin/gcc -shared -o libmetricregroup.so.1.0.1 com_indeed_imhotep_local_NativeMetricRegroupInternals.o metricregroup.o`;
