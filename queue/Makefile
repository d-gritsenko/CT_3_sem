CC = gcc
CFLAGS = -Wall -Wextra -pthread
GTK_CFLAGS = $(shell pkg-config --cflags gtk+-3.0)
GTK_LIBS = $(shell pkg-config --libs gtk+-3.0)

.PHONY: all gui clean

all: extraq gui_client

extraq: extraq.c db.c crypto.c db.h crypto.h protocol.h
	$(CC) $(CFLAGS) extraq.c db.c crypto.c -o extraq -lsqlite3 -lsodium

gui: gui_client

gui_client: gui_client.c crypto.c crypto.h protocol.h
	$(CC) $(CFLAGS) $(GTK_CFLAGS) gui_client.c crypto.c -o gui_client $(GTK_LIBS) -lsodium

clean:
	rm -f extraq gui_client