IDIR =include
CC=/usr/bin/clang
CPP=/usr/bin/g++
JNI_HEADER_LOC=$(JAVA_HOME)/include
JNI_HEADER_LOC2=$(JNI_HEADER_LOC)/linux
CFLAGS=-g  -Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -std=gnu11 -msse4.2 -fvisibility=hidden -fPIC
CPPFLAGS=-g  -Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -I/usr/include/boost -std=c++0x -msse4.2 -fvisibility=hidden -fPIC
LFLAGS=-shared

ODIR=obj
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
          obj/test_patch.o       \
          obj/tgs.o              \
          obj/unpacked_table.o   \
          obj/varintdecode.o

TEST_CIRC_BUF_OBJS=$(TEST_OBJS) obj/test_circ_buf.o
TEST_GROUPS_OBJS=$(TEST_OBJS) obj/test_groups.o
TEST_PACKED_SHARD_OBJS=$(TEST_OBJS) obj/test_packed_shard.o
TEST_TGS_OBJS=$(TEST_OBJS) obj/test_tgs.o
TEST_VARINTDECODE_OBJS=$(TEST_OBJS) obj/test_varintdecode.o
TESTS := test_circ_buf test_groups test_packed_shard test_tgs test_varintdecode

$(ODIR)/%.o: src/%.c $(H_FILES)
	$(CC) -c -o $@ $< $(CFLAGS)

$(ODIR)/%.o: src/%.cpp $(H_FILES)
	$(CPP) -c -o $@ $< $(CPPFLAGS)

.PHONY: clean

all: native_lib tests

native_lib: $(OBJ_FILES)
	$(CC) $(LFLAGS) $(LIBS) -o $@ $^

dump_term: $(OBJ_FILES) obj/dump_term.o
	$(CPP) $(CFLAGS) $ -o dump_term obj/dump_term.o $(OBJ_FILES) -lstdc++

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
	rm -f $(ODIR)/*.o *~ core $(INCDIR)/*~
	rm -f $(TESTS)
	rm -f dump_term

