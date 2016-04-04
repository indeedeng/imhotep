CC=/usr/bin/gcc-4.9
CPP=/usr/bin/g++-4.9

BOOST_LIBS := -lboost_program_options

INCLUDES := -I../include -I../../include
LIBS := $(BOOST_LIBS) -lyaml-cpp -L../../build -l:libftgs.so -lasan

CFLAGS := -O3 -Wall -Werror -std=gnu99 -msse4.2 -fPIC -flax-vector-conversions -fno-omit-frame-pointer $(INCLUDES)
CPPFLAGS := -fPIC -g -O3 --std=c++11 -Wall -Werror $(INCLUDES) -fsanitize=address -fno-omit-frame-pointer

%.o: %.cpp
	$(CPP) $(CPPFLAGS) -c -o $@ $<

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

all: test_regroup test_regroup_condition test_term_index

test_regroup: table_sizes.o test_regroup.o
	$(CPP) $ -o test_regroup $^ -O3 -fno-omit-frame-pointer $(LIBS)

test_regroup_condition: test_regroup_condition.o
	$(CPP) $ -o test_regroup_condition $^ -O3 -fno-omit-frame-pointer $(LIBS)

test_term_index: test_term_index.o
	$(CPP) $ -o test_term_index $^ -O3 -fno-omit-frame-pointer $(LIBS)

clean:
	rm -f test_regroup
	rm -f test_regroup_condition
	rm -f test_term_index
	rm -f *.o
