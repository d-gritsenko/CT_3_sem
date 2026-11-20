#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <string.h>
#include <sys/un.h>
#include <unistd.h>

int main(int argc, char **argv) {
    int sock;
    struct sockaddr_un saddr;

    if (argc < 2) {
        fprintf(stderr, "Too few arguments\n");
        return 1;
    }

    sock = socket(PF_UNIX, SOCK_STREAM, 0);
    if (sock == -1) {
        fprintf(stderr, "socket() error\n");
        return 1;
    }

    memset(&saddr, 0, sizeof(saddr));
    saddr.sun_family = AF_UNIX;
    strncpy(saddr.sun_path, argv[1], sizeof(saddr.sun_path) - 1);

    if (bind(sock, (struct sockaddr *)&saddr, SUN_LEN(&saddr)) == -1) {
        fprintf(stderr, "bind() error\n");
        close(sock);
        return 1;
    }

    fprintf(stderr, "Press <Enter> to continue...");
    fgetc(stdin);

    close(sock);
    unlink(argv[1]);
    return 0;
}

