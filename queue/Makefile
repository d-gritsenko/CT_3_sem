CC = gcc
CFLAGS = -Wall -Wextra -pthread
GTK_CFLAGS = $(shell pkg-config --cflags gtk+-3.0)
GTK_LIBS = $(shell pkg-config --libs gtk+-3.0)

SRCS = $(filter-out gui_client.c db.c extraq.c, $(wildcard *.c))
BINS = $(SRCS:.c=)

.PHONY: all gui clean

all: $(BINS) extraq gui_client

$(BINS): %: %.c
	$(CC) $(CFLAGS) $< -o $@

extraq: extraq.c db.c db.h protocol.h
	$(CC) $(CFLAGS) extraq.c db.c -o extraq -lsqlite3

gui: gui_client

gui_client: gui_client.c protocol.h
	$(CC) $(CFLAGS) $(GTK_CFLAGS) gui_client.c -o gui_client $(GTK_LIBS)

clean:
	rm -f $(BINS) extraq gui_client