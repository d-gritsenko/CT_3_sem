CC := gcc
CFLAGS := -Wall -Wextra -pthread
GTK_CFLAGS := $(shell pkg-config --cflags gtk+-3.0)
GTK_LIBS := $(shell pkg-config --libs gtk+-3.0)

CONSOLE_SRCS := $(filter-out gui_client.c,$(wildcard *.c))
CONSOLE_TARGETS := $(CONSOLE_SRCS:.c=)
GUI_TARGET := gui_client

.PHONY: all gui clean

all: $(CONSOLE_TARGETS) $(GUI_TARGET)

$(CONSOLE_TARGETS): %: %.c protocol.h
	$(CC) $(CFLAGS) $< -o $@

$(GUI_TARGET): gui_client.c protocol.h
	$(CC) $(CFLAGS) $(GTK_CFLAGS) $< -o $@ $(GTK_LIBS)

gui: $(GUI_TARGET)

clean:
	rm -f $(CONSOLE_TARGETS) $(GUI_TARGET)