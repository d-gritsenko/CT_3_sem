CC=gcc
CFLAGS=-Wall -pthread -O2
TARGET=chat
OBJS=main.o core.o utils.o

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS)

main.o: main.c chat.h
	$(CC) $(CFLAGS) -c main.c

core.o: core.c chat.h
	$(CC) $(CFLAGS) -c core.c

utils.o: utils.c chat.h
	$(CC) $(CFLAGS) -c utils.c

clean:
	rm -f $(OBJS) $(TARGET) chat_history.txt
