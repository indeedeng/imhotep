IDIR =include
CC=clang
CFLAGS=-g -I$(IDIR) -std=c11 -msse4.2 -fvisibility=hidden -fPIC
LFLAGS=-shared

ODIR=obj
LDIR =../lib

LIBS=-lm

C_FILES := $(wildcard src/*.c)
H_FILES := $(wildcard include/*.h)
OBJ_FILES := $(addprefix obj/,$(notdir $(C_FILES:.c=.o)))

#_DEPS = hellomake.h
#DEPS = $(patsubst %,$(IDIR)/%,$(_DEPS))

#_OBJ = hellomake.o hellofunc.o 
#OBJ = $(patsubst %,$(ODIR)/%,$(_OBJ))


$(ODIR)/%.o: src/%.c $(H_FILES)
	$(CC) -c -o $@ $< $(CFLAGS)

objs: $(OBJ_FILES)
	$(CC) $(LFLAGS) $(LIBS) -o $@ $^ 

.PHONY: clean

clean:
	rm -f $(ODIR)/*.o *~ core $(INCDIR)/*~
	
all: $(ODIR)/%.o
