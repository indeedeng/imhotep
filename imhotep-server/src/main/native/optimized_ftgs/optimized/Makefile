MODE=opt

IDIR=include
CC=/usr/bin/gcc-4.4
CPP=/usr/bin/g++-4.4
JNI_HEADER_LOC=$(JAVA_HOME)/include
JNI_HEADER_LOC2=$(JNI_HEADER_LOC)/linux
BASE_CFLAGS= -Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -std=gnu99 -msse4.2 -fPIC -flax-vector-conversions
BASE_CPPFLAGS=-Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -I/usr/include/boost -msse4.2 -std=c++0x -fPIC
BASE_LFLAGS= -shared  -fno-omit-frame-pointer

DBG_CFLAGS= -g3 -fno-omit-frame-pointer
OPT_CFLAGS= -g -O3  -fvisibility=hidden

DBG_CPPFLAGS= -g3 -fno-omit-frame-pointer
OPT_CPPFLAGS= -g -O3  -fvisibility=hidden

DBG_LFLAGS= -g3 -fsanitize=address -fno-omit-frame-pointer
OPT_LFLAGS= -g -O3

ifeq ($(MODE),opt)
	CFLAGS=$(OPT_CFLAGS) $(BASE_CFLAGS)
	CPPFLAGS=$(OPT_CPPFLAGS) $(BASE_CPPFLAGS)
	LFLAGS=$(OPT_LFLAGS) $(BASE_LFLAGS)
else
	CFLAGS= $(DBG_CFLAGS) $(BASE_CFLAGS)
	CPPFLAGS= $(DBG_CPPFLAGS) $(BASE_CPPFLAGS)
	LFLAGS= $(DBG_LFLAGS) $(BASE_LFLAGS)
endif

ODIR=obj
CPPODIR=cppobj
LDIR =../lib

LIBS=\
	-lpthread
#	-lprofiler \

C_FILES := $(wildcard src/*.c)
H_FILES := $(wildcard include/*.h)
OBJ_FILES := $(addprefix obj/,$(notdir $(C_FILES:.c=.o)))

CPP_FILES := $(wildcard src/*.cpp)
HPP_FILES := $(wildcard include/*.hpp)
CPP_OBJ_FILES += $(addprefix cppobj/,$(notdir $(CPP_FILES:.cpp=.o)))

MULTICACHE_FILES=\
	MultiCache_jni.c            \
	packed_table.c 
MULTICACHE_OBJ_FILES := $(addprefix obj/,$(notdir $(MULTICACHE_FILES:.c=.o)))

NATIVE_FIELD_CACHER_FILES=\
	NativeFlamdexFieldCacher_jni.c      \
	metrics_inverter.c                  \
	varintdecode.c
NATIVE_FIELD_CACHER_OBJ_FILES := $(addprefix obj/,$(notdir $(NATIVE_FIELD_CACHER_FILES:.c=.o)))

NATIVE_FTGS_C_FILES := $(filter-out $(MULTICACHE_FILES) $(NATIVE_FIELD_CACHER_FILES),$(C_FILES))
NATIVE_FTGS_OBJ_FILES := $(addprefix obj/,$(notdir $(NATIVE_FTGS_C_FILES:.c=.o)))

$(ODIR)/%.o: src/%.c $(H_FILES)
	$(CC) $(CFLAGS) -include $(IDIR)/gcc_preinclude.h -c -o $@ $<

$(CPPODIR)/%.o: src/%.cpp $(HPP_FILES) include/imhotep_native.h include/local_session.h
	$(CPP) $(CPPFLAGS) -include $(IDIR)/gcc_preinclude.h -c -o $@ $<

.PHONY: clean

all: ftgs_lib multicache_lib fieldcache_lib

ftgs_lib: $(NATIVE_FTGS_OBJ_FILES) $(CPP_OBJ_FILES)
	$(CPP) $(LFLAGS) -Wl,-export-dynamic -Wl,-soname,libftgs.so.1 -o libftgs.so.1.0.1 $^ $(LIBS)
	cp  libftgs.so.1.0.1 ../../../resources/native/Linux-amd64

multicache_lib: $(MULTICACHE_OBJ_FILES)
	$(CC) $(LFLAGS) -Wl,-export-dynamic -Wl,-soname,libmulticache.so.1 -o libmulticache.so.1.0.1 $^ $(LIBS)
	cp  libmulticache.so.1.0.1 ../../../resources/native/Linux-amd64

fieldcache_lib: $(NATIVE_FIELD_CACHER_OBJ_FILES)
	$(CC) $(LFLAGS) -Wl,-export-dynamic -Wl,-soname,libfieldcache.so.1 -o libfieldcache.so.1.0.1 $^ $(LIBS)
	cp  libfieldcache.so.1.0.1 ../../../resources/native/Linux-amd64

clean:
	rm -f $(ODIR)/*.o $(CPPODIR)/*.o *~ core $(INCDIR)/*~
	rm -f libftgs.so.1.0.1
	rm -f libfieldcache.so.1.0.1
	rm -f libmulticache.so.1.0.1
