CC = gcc
CFLAGS = -Wall -Wextra -O2

all: runsim dishes nolibc_tcp debugger

runsim: runsim.c
	$(CC) $(CFLAGS) runsim.c -o runsim

dishes: dishes.c
	$(CC) $(CFLAGS) dishes.c -o dishes -lpthread

nolibc_tcp: nolibc_tcp.c
	$(CC) -nostdlib -fno-stack-protector -O2 nolibc_tcp.c -o nolibc_tcp

debugger: debugger.c
	$(CC) $(CFLAGS) debugger.c -o debugger

clean:
	rm -f runsim dishes nolibc_tcp debugger