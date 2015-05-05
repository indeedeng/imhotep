CC=/usr/bin/gcc-4.9
CPP=/usr/bin/g++-4.9
CFLAGS=-Wall -Werror -I./include -I../include  -std=gnu11 -msse4.2 -fPIC -flax-vector-conversions 
CPPFLAGS=-g -O3 -Wall -Werror -I./include -I../include -I/usr/include/boost -msse4.2 -std=c++0x -fPIC  -flto
#CFLAGS=-Wall -Werror -I./include -I../include  -std=gnu11 -msse4.2 -fPIC -flax-vector-conversions -fsanitize=address -fno-omit-frame-pointer -D_SANITIZE_
#CPPFLAGS=-g -Wall -Werror -I./include -I../include -I/usr/include/boost -msse4.2 -std=c++0x -fPIC -fsanitize=address -fno-omit-frame-pointer -D_SANITIZE_

LFLAGS=-g -O3 -flto

OPT_CFLAGS= -g -O3 -flto -funroll-loops $(CFLAGS)
DBG_CFLAGS= -g $(CFLAGS)

LIBS=-lm -lstdc++

TESTS := test_bit_tree test_circ_buf test_groups test_packed_shard test_tgs test_varintdecode packed_perf

all: $(TESTS)

.PHONY: clean

obj/%.o: src/%.cpp $(H_FILES)
	$(CPP) -c -o $@ $< $(CPPFLAGS)

obj/test_patch.o: src/test_patch.c
	$(CC) -c -o $@ $< $(CFLAGS)

obj/packed_tree_perf_test.o: src/packed_tree_perf_test.c
	$(CC) -c -o $@ $< $(OPT_CFLAGS)

test_bit_tree: obj/test_bit_tree.o ../obj/bit_tree.o
	$(CPP) $ -o test_bit_tree $(LIBS) $^

test_circ_buf: obj/test_circ_buf.o ../obj/circ_buf.o
	$(CPP) $ -o test_circ_buf $(LIBS) $^

GROUPS_OBJS = \
    obj/test_patch.o          \
    ../obj/bit_tree.o         \
    ../obj/circ_buf.o         \
    ../obj/groups.o           \
    ../obj/local_session.o    \
    ../obj/packed_table.o     \
    ../obj/remote_output.o    \
    ../obj/socket.o           \
    ../obj/table_accumulate.o \
    ../obj/tgs.o              \
    ../obj/unpacked_table.o   \
    ../obj/varintdecode.o

test_groups: obj/test_groups.o $(GROUPS_OBJS)
	$(CPP) $ -o test_groups $(LIBS) $^

test_packed_shard: obj/test_packed_shard.o obj/test_patch.o ../obj/packed_table.o
	$(CPP) $ -o test_packed_shard $(LIBS) $^

TGS_OBJS = $(GROUPS_OBJS) ../obj/metrics_inverter.o

test_tgs: obj/test_tgs.o $(TGS_OBJS)
	$(CPP) $ -o test_tgs $(LIBS) $^

test_varintdecode: obj/test_varintdecode.o ../obj/varintdecode.o
	$(CPP) $ -o test_varintdecode $(LIBS) $^

dump_term: obj/dump_term.o $(TGS_OBJS)
	$(CPP) $(LFLAGS) $ -o dump_term $(LIBS) $^ -L/usr/lib/x86_64-linux-gnu -lboost_system -lpthread #-lasan

packed_perf: obj/packed_tree_perf_test.o ../obj/packed_table.o
	$(CC) $ -flto -funroll-loops -O3 -o packed_perf $^ -L/usr/lib/x86_64-linux-gnu -lrt 


clean:
	rm -f $(ODIR)/*.o *~ core $(INCDIR)/*~
	rm -f $(TESTS)
	rm -f dump_term

