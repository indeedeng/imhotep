IDIR =include
CC=/usr/bin/clang
JNI_HEADER_LOC=$(JAVA_HOME)/include
JNI_HEADER_LOC2=$(JNI_HEADER_LOC)/linux
CFLAGS=-g -Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -std=c11 -msse4.2 -fvisibility=hidden -fPIC
CPPFLAGS=-g -Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -std=c++0x -msse4.2 -fvisibility=hidden -fPIC
LFLAGS=-shared

ODIR=obj
LDIR =../lib

LIBS=-lm

C_FILES := $(wildcard src/*.c)
H_FILES := $(wildcard include/*.h)
OBJ_FILES := $(addprefix obj/,$(notdir $(C_FILES:.c=.o)))

TEST_OBJS=obj/bit_tree.o obj/circ_buf.o obj/packed_table.o obj/unpacked_table.o obj/tgs.o obj/varintdecode.o obj/table_accumulate.o obj/local_session.o obj/socket.o
TEST_CIRC_BUF_OBJS=$(TEST_OBJS) obj/test_circ_buf.o
TEST_PACKED_SHARD_OBJS=$(TEST_OBJS) obj/test_packed_shard.o
TEST_TGS_OBJS=$(TEST_OBJS) obj/test_tgs.o
TEST_VARINTDECODE_OBJS=$(TEST_OBJS) obj/test_varintdecode.o
TESTS := test_circ_buf test_packed_shard test_tgs #test_varintdecode


$(ODIR)/%.o: src/%.c $(H_FILES)
	$(CC) -c -o $@ $< $(CFLAGS)

$(ODIR)/%.o: src/%.cpp $(H_FILES)
	$(CC) -c -o $@ $< $(CPPFLAGS)

native_lib: $(OBJ_FILES)
	$(CC) $(LFLAGS) $(LIBS) -o $@ $^ 

.PHONY: clean

test_circ_buf: $(TEST_CIRC_BUF_OBJS)
	$(CC) $ -o test_circ_buf $(TEST_CIRC_BUF_OBJS) $(LIBS) -lstdc++

test_packed_shard: $(TEST_PACKED_SHARD_OBJS)
	$(CC) $ -o test_packed_shard $(TEST_PACKED_SHARD_OBJS) $(LIBS) -lstdc++

test_tgs: $(TEST_TGS_OBJS)
	$(CC) $ -o test_tgs $(TEST_TGS_OBJS) $(LIBS) -lstdc++

test_varintdecode: $(TEST_VARINTDECODE_OBJS)
	$(CC) $ -o test_varintdecode $(TEST_VARINTDECODE_OBJS) $(LIBS) -lstdc++

tests: $(TESTS)

clean:
	rm -f $(ODIR)/*.o *~ core $(INCDIR)/*~
	rm -f $(TESTS)

all: $(ODIR)/%.o
