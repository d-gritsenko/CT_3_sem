#ifndef CHAT_H
#define CHAT_H

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/wait.h>
#include <sys/msg.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <termios.h>
#include <time.h>

#define SHMEM_SIZE 4096
#define SEM_KEY 2007
#define SHM_KEY 2007
#define MSG_KEY 2008
#define LOG_FILE "chat_history.txt"
#define SRC_FILE "QQQ.Q"

#define CLR_RESET "\033[0m"
#define CLR_GREEN "\033[1;32m"
#define CLR_CYAN "\033[1;36m"
#define CLR_YELLOW "\033[1;33m"
#define CLR_GRAY "\033[0;90m"

union semnum {
    int val;
    struct semid_ds *buf;
    unsigned short *array;
};

struct chat_memory {
    int msg_id;
    pid_t sender_pid;
    char sender_name[64];
    char text[3900];
};

struct pm_msg {
    long mtype;
    char sender_name[64];
    char text[3900];
};

extern char current_input[3900];
extern int input_pos;
extern struct termios orig_termios;
extern union semnum sem_arg;
extern int shm_id, sem_id, msg_qid;
extern struct chat_memory *shm_buf;
extern pid_t my_pid;
extern char my_name[64];
extern pthread_t recv_thread;
extern int thread_started;

long hash_name(const char *str);
void sem_lock(int semid);
void sem_unlock(int semid);
void get_time_str(char *buf, size_t size);
void disable_raw_mode();
void enable_raw_mode();
int read_chat_line(char *out_buf, int max_len);
void cleanup(int sig);
void append_to_log(const char *name, pid_t pid, const char *msg);
void *receiver_thread_func(void *arg);
void handle_sigint(int sig);

#endif
