#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define MESSAGE_LENGTH 255

struct mymsgbuf {
    long mtype;
    char mtext[MESSAGE_LENGTH];
};

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <recv_id> <send_id>\n", argv[0]);
        return 1;
    }

    long recv_id = strtol(argv[1], NULL, 10);
    long send_id = strtol(argv[2], NULL, 10);

    if (recv_id <= 0 || send_id <= 0) {
        fprintf(stderr, "IDs must be positive integers\n");
        return 1;
    }

    key_t key = ftok("2_Q.c", 0);
    if (key == (key_t)-1) {
        perror("ftok");
        return 1;
    }

    int msqid = msgget(key, 0666 | IPC_CREAT);
    if (msqid < 0) {
        perror("msgget");
        return 1;
    }

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return 1;
    }

    if (pid == 0) {
        struct mymsgbuf in_buf;
        while (1) {
            ssize_t n = msgrcv(msqid, &in_buf, sizeof(in_buf.mtext), recv_id, 0);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                break;
            }
            in_buf.mtext[MESSAGE_LENGTH - 1] = '\0';
            printf("[%ld]: %s\n", recv_id, in_buf.mtext);
            fflush(stdout);
        }
        return 0;
    }

    struct mymsgbuf out_buf;
    while (fgets(out_buf.mtext, sizeof(out_buf.mtext), stdin) != NULL) {
        out_buf.mtext[strcspn(out_buf.mtext, "\n")] = '\0';
        out_buf.mtype = send_id;
        size_t len = strlen(out_buf.mtext) + 1;
        if (msgsnd(msqid, &out_buf, len, 0) < 0) {
            perror("msgsnd");
            break;
        }
    }

    kill(pid, SIGTERM);
    waitpid(pid, NULL, 0);
    return 0;
}