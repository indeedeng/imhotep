CC=/usr/bin/gcc-4.9
CPP=/usr/bin/g++-4.9

BOOST_LIBS := -lboost_program_options

INCLUDES := -I../include -I../../include
# LIBS := $(BOOST_LIBS) -lyaml-cpp -L../../build -l:libftgs.so -lasan
LIBS := $(BOOST_LIBS) -lyaml-cpp -L../../build -l:libftgs.so

CFLAGS := -O3 -Wall -Werror -std=gnu99 -msse4.2 -fPIC -flax-vector-conversions -fno-omit-frame-pointer $(INCLUDES)
# CPPFLAGS := -fPIC -g -O3 --std=c++11 -Wall -Werror $(INCLUDES) -fsanitize=address -fno-omit-frame-pointer
CPPFLAGS := -fPIC -g -O3 --std=c++11 -Wall -Werror $(INCLUDES) -fno-omit-frame-pointer

%.o: %.cpp
	$(CPP) $(CPPFLAGS) -c -o $@ $<

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

all: dump_doc dump_field

dump_doc: dump_doc.o
	$(CPP) $ -o dump_doc $^ -O3 -fno-omit-frame-pointer $(LIBS)

dump_field: dump_field.o
	$(CPP) $ -o dump_field $^ -O3 -fno-omit-frame-pointer $(LIBS)

clean:
	rm -f dump_doc
	rm -f dump_field
	rm -f *.o
