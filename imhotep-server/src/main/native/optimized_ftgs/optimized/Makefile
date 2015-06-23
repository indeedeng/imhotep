MODE=opt

IDIR=include
CC=/usr/bin/gcc-4.8
CPP=/usr/bin/g++-4.8
JNI_HEADER_LOC=$(JAVA_HOME)/include
JNI_HEADER_LOC2=$(JNI_HEADER_LOC)/linux
BASE_CFLAGS= -Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -std=gnu11 -msse4.2 -fPIC -flax-vector-conversions
BASE_CPPFLAGS=-Wall  -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -I/usr/include/boost -msse4.2 -std=c++11 -fPIC
BASE_LFLAGS= -shared  -fno-omit-frame-pointer

DBG_CFLAGS= -g -fno-omit-frame-pointer
#OPT_CFLAGS= -flto -g -O3 -fno-omit-frame-pointer -fvisibility=hidden
OPT_CFLAGS= -g -O3 -fno-omit-frame-pointer -fvisibility=hidden

DBG_CPPFLAGS= -g -fno-omit-frame-pointer
#OPT_CPPFLAGS= -flto -funroll-loops -g -O3 -fno-omit-frame-pointer -fvisibility=hidden
OPT_CPPFLAGS= -funroll-loops -g -O3 -fno-omit-frame-pointer -fvisibility=hidden

DBG_LFLAGS= -g
# OPT_LFLAGS= -O3 -flto
OPT_LFLAGS= -O3

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
	-lpthread               \
	-lstdc++

C_FILES := $(wildcard src/*.c)
H_FILES := $(wildcard include/*.h)
OBJ_FILES := $(addprefix obj/,$(notdir $(C_FILES:.c=.o)))

CPP_FILES := $(wildcard src/*.cpp)
HPP_FILES := $(wildcard include/*.hpp)
CPP_OBJ_FILES += $(addprefix cppobj/,$(notdir $(CPP_FILES:.cpp=.o)))

$(ODIR)/%.o: src/%.c $(H_FILES)
	$(CC) $(CFLAGS) -include $(IDIR)/gcc_preinclude.h -c -o $@ $<

$(CPPODIR)/%.o: src/%.cpp $(HPP_FILES)
	$(CPP) $(CPPFLAGS) -include $(IDIR)/gcc_preinclude.h -c -o $@ $<

.PHONY: clean

all: native_lib

native_lib: $(filter-out $(ODIR)/test_invert_metrics.o,$(OBJ_FILES)) $(CPP_OBJ_FILES)
	$(CPP) $(LFLAGS) -Wl,-export-dynamic -Wl,-init,simdvbyteinit -Wl,-soname,libftgs.so.1 -o libftgs.so.1.0.1 $^ $(LIBS)
	cp  libftgs.so.1.0.1 ../../../resources/native/Linux-amd64

clean:
	rm -f $(ODIR)/*.o $(CPPODIR)/*.o *~ core $(INCDIR)/*~
	rm -f libftgs.so.1.0.1
