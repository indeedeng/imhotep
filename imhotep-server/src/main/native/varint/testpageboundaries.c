#include <stdlib.h>
#include <sys/mman.h>
#include <stdint.h>
#include <assert.h>
#include "varintdecode.h"

int main(int argc, char** argv) {
	uint8_t* addr = mmap(NULL, 8192, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if (addr == MAP_FAILED) exit(1);
	int err = mprotect(addr+4096, 4096, PROT_NONE);
	if (err != 0) exit(2);
	uint32_t* mem = malloc(sizeof(uint32_t) * 4096);
	addr[0] = 0x81;
	addr[1] = 0x80;
	addr[2] = 0x80;
	addr[3] = 0x00;
	addr[4] = 0x81;
	addr[5] = 0x80;
	addr[6] = 0x80;
	addr[7] = 0x00;
	addr[8] = 0x81;
	addr[9] = 0x80;
	addr[10] = 0x80;
	addr[11] = 0x00;
	for (int i = 12; i < 4096; i++) {
		addr[i] = 1;
	}
	init();
	read_ints(0, addr, mem, 4087);
	assert(4087 == mem[4086]);
	for (int i = 0; i < 4096; i++) {
		addr[i] = 1;
	}
	read_ints(0, addr, mem, 4096);
	assert(4096 == mem[4095]);
	return 0;
}
