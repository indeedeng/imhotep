MODE=dbg

CC=/usr/bin/gcc-5
CPP=/usr/bin/g++-5
BASE_CFLAGS=-Wall -Werror -I./include -I../include  -std=gnu99 -msse4.2 -fPIC -flax-vector-conversions -fno-omit-frame-pointer -flto
BASE_CPPFLAGS=-Wall -Werror -I./include -I../include -I/usr/include/boost -msse4.2 -std=c++11 -fPIC -flto

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
	-lyaml-cpp              \
	-lm                     \
#	-lprofiler              \
#	-lm -lstdc++

TESTS := \
	packed_tree_perf_test     \
	test_bit_tree             \
	test_circ_buf             \
	test_field                \
	test_field_op_iterator    \
	test_ftgs_runner          \
	test_int_term_iterator    \
	test_merge_iterator       \
	test_packed_shard         \
	test_split                \
	test_split_iterator       \
	test_string_term_iterator \
	test_memory               \
	test_term_provider        \
	test_varintdecode
#	test_tgs                  \

all: $(TESTS)

.PHONY: clean

obj/%.o: src/%.cpp $(H_FILES)
	$(CPP) $(CPPFLAGS) -c -o $@ $< $(CPPFLAGS)

obj/test_patch.o: src/test_patch.c
	$(CC) -c -o $@ $< $(CFLAGS)

obj/packed_tree_perf_test.o: src/packed_tree_perf_test.c
	$(CC) -c -o $@ $< $(CFLAGS)

test_bit_tree: obj/test_bit_tree.o ../obj/bit_tree.o
	$(CPP) $ -o test_bit_tree $(LIBS) $^

test_circ_buf: obj/test_circ_buf.o ../obj/circ_buf.o
	$(CPP) $ -o test_circ_buf $(LIBS) $^

test_split: obj/test_split.o ../cppobj/shard.o ../cppobj/splitter.o ../cppobj/term_iterator.o
	$(CPP) -pthread $(CPPFLAGS) $ -o test_split $(LIBS) $^

test_merge_iterator: obj/test_merge_iterator.o ../cppobj/log.o
	$(CPP) $(CPPFLAGS) $ -o test_merge_iterator $(LIBS) $^

test_split_iterator: obj/test_split_iterator.o ../cppobj/log.o
	$(CPP) $(CPPFLAGS) $ -o test_split_iterator $(LIBS) $^

TEST_IT_OBJS =                \
	../cppobj/shard.o         \
	../cppobj/term_index.o    \
	../cppobj/term_iterator.o \
	../obj/varintdecode.o

test_int_term_iterator: obj/test_int_term_iterator.o $(TEST_IT_OBJS)
	$(CPP) $(CPPFLAGS) $ -o test_int_term_iterator $(LFLAGS) $(LIBS) $^

test_string_term_iterator: obj/test_string_term_iterator.o $(TEST_IT_OBJS)
	$(CPP) $(CPPFLAGS) $ -o test_string_term_iterator $(LIBS) $^

test_field: obj/test_field.o $(TEST_IT_OBJS)
	$(CPP) $(CPPFLAGS) $ -o test_field $(LFLAGS) $(LIBS) $^

TERM_PROVIDER_OBJS = \
	../cppobj/executor_service.o \
	../cppobj/field_op_iterator.o \
	../cppobj/log.o \
	../cppobj/operation.o \
	../cppobj/shard.o \
	../cppobj/splitter.o \
	../cppobj/term_iterator.o \
	../cppobj/term_provider.o \
	../cppobj/term_providers.o

test_term_provider: obj/test_term_provider.o $(TERM_PROVIDER_OBJS)
	$(CPP) -pthread $(CPPFLAGS) $ -o test_term_provider $^ $(LIBS)

test_field_op_iterator: obj/test_field_op_iterator.o $(TERM_PROVIDER_OBJS)
	$(CPP) -pthread $(CPPFLAGS) $ -o test_field_op_iterator $^ $(LIBS)

FTGS_RUNNER_OBJS =\
	$(TERM_PROVIDER_OBJS) \
	../cppobj/ftgs_runner.o	\
	../cppobj/log.o \
	../cppobj/shard.o \
	../cppobj/task_iterator.o \
	../cppobj/term_providers.o \
	../obj/binary_table_accumulate.o \
	../obj/bit_tree.o \
	../obj/circ_buf.o \
	../obj/local_session.o \
	../obj/packed_table.o \
	../obj/packed_table.o \
	../obj/remote_output.o \
	../obj/socket.o \
	../obj/table_accumulate.o \
	../obj/tgs.o \
	../obj/unpacked_table.o \
	../obj/varintdecode.o \
	obj/test_patch.o

test_ftgs_runner: obj/test_ftgs_runner.o $(FTGS_RUNNER_OBJS)
	$(CPP) $(CPPFLAGS) -O3 -pthread $ -o test_ftgs_runner $^ $(LIBS)

test_packed_shard: obj/test_packed_shard.o obj/test_patch.o ../obj/packed_table.o
	$(CPP) $ -o test_packed_shard $(LIBS) $^

TGS_OBJS = \
    obj/test_patch.o          \
    ../obj/bit_tree.o         \
    ../obj/circ_buf.o         \
    ../obj/groups.o           \
    ../obj/local_session.o    \
    ../obj/metrics_inverter.o \
    ../obj/packed_table.o     \
    ../obj/remote_output.o    \
    ../obj/socket.o           \
    ../obj/table_accumulate.o \
    ../obj/table_accumulate.o \
    ../obj/tgs.o              \
    ../obj/unpacked_table.o   \
    ../obj/varintdecode.o


test_tgs: obj/test_tgs.o $(TGS_OBJS)
	$(CPP) $ -o test_tgs $(LIBS) $^

test_varintdecode: obj/test_varintdecode.o ../obj/varintdecode.o
	$(CPP) $ -o test_varintdecode $(LIBS) $^

dump_term: obj/dump_term.o $(TGS_OBJS)
#	$(CPP) $(LFLAGS) $ -o dump_term $(LIBS) $^ -L/usr/lib/x86_64-linux-gnu -lboost_system -lpthread -lasan
	$(CPP) $(LFLAGS) $ -o dump_term $(LIBS) $^ -L/usr/lib/x86_64-linux-gnu -lboost_system -lpthread

packed_tree_perf_test: obj/packed_tree_perf_test.o ../obj/packed_table.o
	$(CC) $ -flto -funroll-loops -O3 -o packed_tree_perf_test $^ -L/usr/lib/x86_64-linux-gnu -lrt

test_memory: src/test_memory.cpp ../cppobj/memory.o
	$(CPP) $ -O3 -I ../include -o test_memory src/test_memory.cpp ../cppobj/memory.o -L/usr/lib/x86_64-linux-gnu -lrt

clean:
	rm -f obj/*.o *~ core $(INCDIR)/*~
	rm -f $(TESTS)
	rm -f dump_term
