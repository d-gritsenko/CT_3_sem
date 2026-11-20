CC := gcc
CFLAGS := -Wall -Wextra
LDFLAGS_EXTRA := -pthread

TARGETS :=
ifneq ($(wildcard 2_Q.c),)
TARGETS += 2_Q
endif
ifneq ($(wildcard ExtraQ.c),)
TARGETS += extraq
endif

.PHONY: all clean

all: $(TARGETS)

2_Q: 2_Q.c
	$(CC) $(CFLAGS) $< -o $@

extraq: ExtraQ.c
	$(CC) $(CFLAGS) $(LDFLAGS_EXTRA) $< -o $@

clean:
	rm -f 2_Q extraq