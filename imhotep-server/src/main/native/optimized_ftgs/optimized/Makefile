IDIR =include
CC=/usr/bin/gcc-4.9
JNI_HEADER_LOC=$(JAVA_HOME)/include
JNI_HEADER_LOC2=$(JNI_HEADER_LOC)/linux
#CFLAGS= -Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -std=gnu11 -msse4.2 -fPIC -flax-vector-conversions #-fsanitize=address -fno-omit-frame-pointer -D_SANITIZE_
#LFLAGS= -g -shared #-fsanitize=address
CFLAGS= -Wall -Werror -I$(IDIR) -I$(JNI_HEADER_LOC) -I$(JNI_HEADER_LOC2) -std=gnu11 -msse4.2 -fPIC -flax-vector-conversions 
LFLAGS= -flto -O3 -shared

#OPT_CFLAGS= -flto -O3 -funroll-loops -fvisibility=hidden $(CFLAGS)
OPT_CFLAGS= -g -flto -O3 -funroll-loops $(CFLAGS)
DBG_CFLAGS= -g $(CFLAGS)

ODIR=obj
LDIR =../lib

LIBS=-lm

C_FILES := $(wildcard src/*.c)
H_FILES := $(wildcard include/*.h)
OBJ_FILES := $(addprefix obj/,$(notdir $(C_FILES:.c=.o)))

$(ODIR)/%.o: src/%.c $(H_FILES)
	$(CC) -c -o $@ $< $(OPT_CFLAGS)

.PHONY: clean

all: native_lib

native_lib: $(filter-out $(ODIR)/test_invert_metrics.o,$(OBJ_FILES))
	$(CC) $(LFLAGS) -Wl,-export-dynamic -Wl,-init,simdvbyteinit $(LIBS) -Wl,-soname,libftgs.so.1 -o libftgs.so.1.0.1 $^

clean:
	rm -f $(ODIR)/*.o *~ core $(INCDIR)/*~
	rm -f libftgs.so.1.0.1

