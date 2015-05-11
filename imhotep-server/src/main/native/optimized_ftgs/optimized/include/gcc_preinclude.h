/*
 * gcc_preinclude.h
 *
 *  Created on: May 8, 2015
 *      Author: darren
 */

/*
 * Needed to ensure the old version of memcpy is used.
 * Production servers currently run libc v2.12
 */

__asm__(".symver memcpy, memcpy@GLIBC_2.2.5");

