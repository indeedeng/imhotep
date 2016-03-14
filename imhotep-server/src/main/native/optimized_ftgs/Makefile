MODE=opt

IDIR=include
CC=gcc
JNI_HEADER_LOC=$(JAVA_HOME)/include
JNI_HEADER_LOC2=$(JNI_HEADER_LOC)/linux
BASE_CFLAGS= -Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -std=gnu99 -msse4.2 -fPIC -flax-vector-conversions -fno-omit-frame-pointer
BASE_LFLAGS= -shared  -fno-omit-frame-pointer

DBG_CFLAGS= -g -fno-omit-frame-pointer
OPT_CFLAGS= -g -O3  # -flto

DBG_LFLAGS= -g -fsanitize=address -fno-omit-frame-pointer
OPT_LFLAGS= -g -O3 # -flto-partition=none -flto

ifeq ($(MODE),opt)
	CFLAGS=$(OPT_CFLAGS) $(BASE_CFLAGS)
	LFLAGS=$(OPT_LFLAGS) $(BASE_LFLAGS)
else
	CFLAGS= $(DBG_CFLAGS) $(BASE_CFLAGS)
	LFLAGS= $(DBG_LFLAGS) $(BASE_LFLAGS)
endif

BUILD_DIR=build
OBJ_DIR=$(BUILD_DIR)/obj

LIBS=\
	-lpthread

C_FILES := $(wildcard src/*.c)
H_FILES := $(wildcard include/*.h) $(wildcard include/jni/*.h)
OBJ_FILES := $(addprefix $(OBJ_DIR)/,$(notdir $(C_FILES:.c=.o)))

LIB_FTGS=$(BUILD_DIR)/libftgs.so.1.0.1
LIB_FIELD_CACHE=$(BUILD_DIR)/libfieldcache.so.1.0.1
LIB_MULTI_CACHE=$(BUILD_DIR)/libmulticache.so.1.0.1

MULTICACHE_FILES=\
	MultiCache_jni.c            \
	packed_table.c
MULTICACHE_OBJ_FILES := $(addprefix $(OBJ_DIR)/,$(notdir $(MULTICACHE_FILES:.c=.o)))

NATIVE_FIELD_CACHER_FILES=\
	NativeFlamdexFieldCacher_jni.c      \
	metrics_inverter.c                  \
	varintdecode.c
NATIVE_FIELD_CACHER_OBJ_FILES := $(addprefix $(OBJ_DIR)/,$(notdir $(NATIVE_FIELD_CACHER_FILES:.c=.o)))

NATIVE_FTGS_C_FILES := $(filter-out $(MULTICACHE_FILES) $(NATIVE_FIELD_CACHER_FILES),$(C_FILES))
NATIVE_FTGS_OBJ_FILES := $(addprefix $(OBJ_DIR)/,$(notdir $(NATIVE_FTGS_C_FILES:.c=.o)))

LIB_TARGET_DIR := ../../resources/native/Linux-amd64

include MakefileCPP

all: build_dir ftgs_lib multicache_lib fieldcache_lib

build_dir:
	mkdir -p $(OBJ_DIR)
	mkdir -p $(CPPOBJ_DIR)

$(OBJ_DIR)/%.o: src/%.c $(H_FILES)
	$(CC) $(CFLAGS) -include $(IDIR)/gcc_preinclude.h -c -o $@ $<

.PHONY: clean

ftgs_lib: $(NATIVE_FTGS_OBJ_FILES) $(CPP_OBJ_FILES)
	$(CPP) $(LFLAGS) -Wl,-export-dynamic -Wl,-soname,libftgs.so.1 -o $(LIB_FTGS) $^ $(LIBS)
	$(CP)  $(LIB_FTGS) $(LIB_TARGET_DIR)

multicache_lib: $(MULTICACHE_OBJ_FILES)
	$(CC) $(LFLAGS) -Wl,-export-dynamic -Wl,-soname,libmulticache.so.1 -o $(LIB_MULTI_CACHE) $^ $(LIBS)
	$(CP)  $(LIB_MULTI_CACHE) $(LIB_TARGET_DIR)

fieldcache_lib: $(NATIVE_FIELD_CACHER_OBJ_FILES)
	$(CC) $(LFLAGS) -Wl,-export-dynamic -Wl,-soname,libfieldcache.so.1 -o $(LIB_FIELD_CACHE) $^ $(LIBS)
	$(CP)  $(LIB_FIELD_CACHE) $(LIB_TARGET_DIR)

clean:
	$(RM) -rf $(BUILD_DIR)


