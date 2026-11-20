#include <stdio.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <string.h>

#define SOCK_NAME "mysocket"

int main(int argc, char **argv) {
    int sock;
    struct sockaddr_un addr;

    if (argc < 2) {
        fprintf(stderr, "Too few arguments\n");
        return 1;
    }

    sock = socket(PF_UNIX, SOCK_STREAM, 0);
    if (sock == -1) {
        fprintf(stderr, "socket() error\n");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCK_NAME, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, SUN_LEN(&addr)) == -1) {
        fprintf(stderr, "connect() error\n");
        close(sock);
        return 1;
    }

    if (write(sock, argv[1], strlen(argv[1])) == -1) {
        fprintf(stderr, "write() error\n");
        close(sock);
    }

    close(sock);
    0;
}

