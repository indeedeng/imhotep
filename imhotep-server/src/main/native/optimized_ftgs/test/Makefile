MODE=dbg

CC=/usr/bin/gcc
CPP=/usr/bin/g++
BASE_CFLAGS=-Wall -Werror -I./include -I../include  -std=gnu99 -msse4.2 -fPIC -flax-vector-conversions -fno-omit-frame-pointer -flto
BASE_CPPFLAGS=-Wall -Werror -I./include -I../include -I../cpp/include -I/usr/include/boost -msse4.2 -std=c++11 -fPIC -flto

DBG_CFLAGS= -g -fvar-tracking -fvar-tracking-assignments
OPT_CFLAGS= -g -O3 -funroll-loops

DBG_CPPFLAGS= -g -fvar-tracking -fvar-tracking-assignments
OPT_CPPFLAGS= -g -std=c++11 -O3 -funroll-loops -fno-omit-frame-pointer -fvisibility=hidden

DBG_LFLAGS= -g
OPT_LFLAGS= -static-libgcc -static-libstdc++ -O3 -flto -flto-partition=none

ifeq ($(MODE),opt)
	CFLAGS=$(OPT_CFLAGS) $(BASE_CFLAGS)
	CPPFLAGS=$(OPT_CPPFLAGS) $(BASE_CPPFLAGS)
	LFLAGS=$(OPT_LFLAGS) $(BASE_LFLAGS)
else
	CFLAGS= $(DBG_CFLAGS) $(BASE_CFLAGS)
	CPPFLAGS= $(DBG_CPPFLAGS) $(BASE_CPPFLAGS)
	LFLAGS= $(DBG_LFLAGS) $(BASE_LFLAGS)
endif

LIBS=\
	-lboost_filesystem      \
	-lboost_program_options \
	-lboost_system          \
	-lprofiler              \
	-lyaml-cpp              \
	-lm                     \
#	-lm -lstdc++

TESTS := \
	test_varintdecode

all: $(TESTS)

.PHONY: clean

obj/%.o: src/%.cpp $(H_FILES)
	$(CPP) $(CPPFLAGS) -c -o $@ $< $(CPPFLAGS)

test_varintdecode: obj/test_varintdecode.o ../obj/varintdecode.o
	$(CPP) $ -o test_varintdecode $(LIBS) $^

clean:
	rm -f obj/*.o *~ core $(INCDIR)/*~
	rm -f $(TESTS)
