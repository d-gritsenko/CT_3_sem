CC = gcc
CFLAGS = -Wall -Wextra -O2

all: socket_demo get_page dserver dclient sstream_server sstream_client

socket_demo: socket_demo.c
 $(CC) $(CFLAGS) socket_demo.c -o socket_demo

get_page: get_page.c
 $(CC) $(CFLAGS) get_page.c -o get_page

dserver: dserver.c
 $(CC) $(CFLAGS) dserver.c -o dserver

dclient: dclient.c
 $(CC) $(CFLAGS) dclient.c -o dclient

sstream_server: sstream_server.c
 $(CC) $(CFLAGS) sstream_server.c -o sstream_server

sstream_client: sstream_client.c
 $(CC) $(CFLAGS) sstream_client.c -o sstream_client

clean:
 rm -f socket_demo get_page dserver dclient sstream_server sstream_client mysocket *.sock
