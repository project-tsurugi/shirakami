T1 = kvs

CC = g++
#CFLAGS = -c -Wall -O2 -std=c++17 -Wunused-variable 
CFLAGS = -c -Wall -g -O0 -std=c++17 -Wunused-variable 
LDFLAGS = -lpthread

O1 = kvs.o xact.o cli.o lib.o

#
# Rules for make
#
all: $(T1) 

$(T1): $(O1) 
	$(CC) -o $@ $^ $(LDFLAGS)

.cc.o:
	$(CC) $(CFLAGS) $<

clean:
	rm -f *~ *.o *.exe *.stackdump $(T1) 
