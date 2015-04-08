IDIR =include
CC=/usr/bin/gcc-4.9
CPP=/usr/bin/g++-4.9
JNI_HEADER_LOC=$(JAVA_HOME)/include
JNI_HEADER_LOC2=$(JNI_HEADER_LOC)/linux
#CFLAGS= -Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -std=gnu11 -msse4.2 -fPIC -fvisibility=hidden -flax-vector-conversions
CFLAGS= -Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -std=gnu11 -msse4.2 -fPIC -flax-vector-conversions #-fsanitize=address -fno-omit-frame-pointer -D_SANITIZE_
CPPFLAGS= -g -Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -I/usr/include/boost -std=c++0x -msse4.2 -fPIC
#LFLAGS=-flto -O3 -shared
LFLAGS= -g -shared #-fsanitize=address

OPT_CFLAGS= -O3 $(CFLAGS)
DBG_CFLAGS= -g $(CFLAGS)

ODIR=obj
TEST_ODIR=test/obj
LDIR =../lib

LIBS=-lm

C_FILES := $(wildcard src/*.c)
H_FILES := $(wildcard include/*.h)
OBJ_FILES := $(addprefix obj/,$(notdir $(C_FILES:.c=.o)))

TEST_OBJS=obj/bit_tree.o         \
          obj/circ_buf.o         \
          obj/groups.o           \
          obj/local_session.o    \
          obj/packed_table.o     \
          obj/remote_output.o    \
          obj/socket.o           \
          obj/table_accumulate.o \
          test/obj/test_patch.o  \
          obj/tgs.o              \
          obj/unpacked_table.o   \
          obj/varintdecode.o

TEST_BIT_TREE_OBJS=$(TEST_OBJS) test/obj/test_bit_tree.o
TEST_CIRC_BUF_OBJS=$(TEST_OBJS) test/obj/test_circ_buf.o
TEST_GROUPS_OBJS=$(TEST_OBJS) test/obj/test_groups.o
TEST_PACKED_SHARD_OBJS=$(TEST_OBJS) test/obj/test_packed_shard.o
TEST_TGS_OBJS=$(TEST_OBJS) test/obj/test_tgs.o
TEST_VARINTDECODE_OBJS=$(TEST_OBJS) test/obj/test_varintdecode.o
TESTS := test_bit_tree test_circ_buf test_groups test_packed_shard test_tgs test_varintdecode

$(TEST_ODIR)/%.o: test/src/%.c $(H_FILES)
	$(CC) -c -o $@ $< $(DBG_CFLAGS)

$(TEST_ODIR)/%.o: test/src/%.cpp $(H_FILES)
	$(CPP) -c -o $@ $< $(CPPFLAGS)

$(ODIR)/%.o: src/%.c $(H_FILES)
	$(CC) -c -o $@ $< $(DBG_CFLAGS)

$(ODIR)/%.o: src/%.cpp $(H_FILES)
	$(CPP) -c -o $@ $< $(CPPFLAGS)

.PHONY: clean

all: native_lib tests

native_lib: $(filter-out $(ODIR)/test_invert_metrics.o,$(OBJ_FILES))
	$(CC) $(LFLAGS) -Wl,-export-dynamic -Wl,-init,simdvbyteinit $(LIBS) -Wl,-soname,libftgs.so.1 -o libftgs.so.1.0.1 $^

dump_term: $(filter-out $(ODIR)/test_invert_metrics.o,$(OBJ_FILES)) test/obj/dump_term.o
	$(CPP) $(CFLAGS) $ -o dump_term test/obj/dump_term.o test/obj/test_patch.o $(filter-out $(ODIR)/test_invert_metrics.o,$(OBJ_FILES)) -lstdc++

test_bit_tree: $(TEST_BIT_TREE_OBJS)
	$(CPP) $ -o test_bit_tree $(TEST_BIT_TREE_OBJS) $(LIBS) -lstdc++

test_circ_buf: $(TEST_CIRC_BUF_OBJS)
	$(CPP) $ -o test_circ_buf $(TEST_CIRC_BUF_OBJS) $(LIBS) -lstdc++

test_groups: $(TEST_GROUPS_OBJS)
	$(CPP) $ -o test_groups $(TEST_GROUPS_OBJS) $(LIBS) -lstdc++

test_packed_shard: $(TEST_PACKED_SHARD_OBJS)
	$(CPP) $ -o test_packed_shard $(TEST_PACKED_SHARD_OBJS) $(LIBS) -lstdc++

test_tgs: $(TEST_TGS_OBJS)
	$(CPP) $ -o test_tgs $(TEST_TGS_OBJS) $(LIBS) -L/usr/lib/x86_64-linux-gnu -lboost_system -lpthread -lstdc++

test_varintdecode: $(TEST_VARINTDECODE_OBJS)
	$(CPP) $ -o test_varintdecode $(TEST_VARINTDECODE_OBJS) $(LIBS) -lstdc++

tests: $(TESTS)

clean:
	rm -f $(TEST_ODIR)/*.o $(ODIR)/*.o *~ core $(INCDIR)/*~
	rm -f libftgs.so.1.0.1
	rm -f $(TESTS)
	rm -f dump_term

