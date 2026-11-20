#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <string.h>
#include <sys/un.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/select.h>

#define BUF_LEN 256
#define SOCK_NAME "mysocket"

int main(void) {
    int sock;
    struct sockaddr_un saddr;
    char *buf;
    int count;

    sock = socket(PF_UNIX, SOCK_DGRAM, 0);
    if (sock == -1) {
        fprintf(stderr, "socket() error\n");
        return 1;
    }

    buf = (char *)malloc(BUF_LEN);
    if (buf == NULL) {
        fprintf(stderr, "malloc() error\n");
        close(sock);
        return 1;
    }

    unlink(SOCK_NAME);
    memset(&saddr, 0, sizeof(saddr));
    saddr.sun_family = AF_UNIX;
    strncpy(saddr.sun_path, SOCK_NAME, sizeof(saddr.sun_path) - 1);

    if (bind(sock, (struct sockaddr *)&saddr, SUN_LEN(&saddr)) == -1) {
        fprintf(stderr, "bind() error\n");
        free(buf);
        close(sock);
        return 1;
    }

    while (1) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(sock, &rfds);

        struct timeval tv;
        tv.tv_sec = 3;
        tv.tv_usec = 0;

        int retval = select(sock + 1, &rfds, NULL, NULL, &tv);
        if (retval == -1) {
            fprintf(stderr, "select() error\n");
            break;
        } else if (retval == 0) {
            printf("изменений нет\n");
            fflush(stdout);
        } else {
            count = recvfrom(sock, buf, BUF_LEN - 1, 0, NULL, NULL);
            if (count == -1) {
                fprintf(stderr, "recvfrom() error\n");
                break;
            }
            buf[count] = '\0';
            printf(">> %s\n", buf);
            if (!strcmp(buf, "exit")) break;
        }
    }

    free(buf);
    close(sock);
    unlink(SOCK_NAME);
    return 0;
}

