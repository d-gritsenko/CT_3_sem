CC := gcc
CFLAGS := -Wall -Wextra -pthread

SRCS := $(wildcard *.c)
TARGETS := $(SRCS:.c=)

.PHONY: all clean

all: $(TARGETS)

%: %.c
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -f $(TARGETS)