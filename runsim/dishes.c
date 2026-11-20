#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <semaphore.h>

#define MAX_TYPES 64
#define NAME_LEN 64

typedef struct {
    char name[NAME_LEN];
    int val;
} Entry;

Entry wash_times[MAX_TYPES]; int wash_cnt = 0;
Entry wipe_times[MAX_TYPES]; int wipe_cnt = 0;

int parse_file(const char *fname, Entry *arr) {
    FILE *f = fopen(fname, "r");
    if (!f) { perror(fname); exit(1); }
    int cnt = 0;
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        char *colon = strchr(line, ':');
        if (!colon) continue;
        *colon = 0;
        char *name = line;
        while (*name == ' ' || *name == '\t') name++;
        char *end = name + strlen(name) - 1;
        while (end > name && (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r')) *end-- = 0;
        
        int val = atoi(colon + 1);
        strncpy(arr[cnt].name, name, NAME_LEN - 1);
        arr[cnt].val = val;
        cnt++;
    }
    fclose(f);
    return cnt;
}

int get_time(Entry *arr, int cnt, const char *name) {
    for (int i = 0; i < cnt; i++) {
        if (strcmp(arr[i].name, name) == 0) return arr[i].val;
    }
    return 1;
}

typedef struct {
    void (*put)(const char *dish);
    int  (*get)(char *dish_out);
    void (*cleanup)(void);
} IPCBackend;

int table_limit = 5;

sem_t *sem_empty, *sem_full, *sem_mutex;
const char *ipc_filename = "/tmp/dish_table.dat";

void file_put(const char *dish) {
    sem_wait(sem_empty);
    sem_wait(sem_mutex);
    FILE *f = fopen(ipc_filename, "a");
    fprintf(f, "%s\n", dish);
    fclose(f);
    sem_post(sem_mutex);
    sem_post(sem_full);
}

int file_get(char *dish_out) {
    static long offset = 0;
    sem_wait(sem_full);
    sem_wait(sem_mutex);
    FILE *f = fopen(ipc_filename, "r");
    fseek(f, offset, SEEK_SET);
    if (!fgets(dish_out, NAME_LEN, f)) {
        fclose(f);
        sem_post(sem_mutex);
        return 0;
    }
    offset = ftell(f);
    fclose(f);
    sem_post(sem_mutex);
    dish_out[strcspn(dish_out, "\n")] = 0;
    if (strcmp(dish_out, "__EOF__") == 0) return 0;
    sem_post(sem_empty);
    return 1;
}

int data_pipe[2], ack_pipe[2];

void pipe_put(const char *dish) {
    if (strcmp(dish, "__EOF__") != 0) {
        char token;
        read(ack_pipe[0], &token, 1);
    }
    char buf[NAME_LEN] = {0};
    strncpy(buf, dish, NAME_LEN - 1);
    write(data_pipe[1], buf, NAME_LEN);
}

int pipe_get(char *dish_out) {
    if (read(data_pipe[0], dish_out, NAME_LEN) <= 0) return 0;
    if (strcmp(dish_out, "__EOF__") == 0) return 0;
    char token = '1';
    write(ack_pipe[1], &token, 1);
    return 1;
}

int msqid;
struct msgbuf_dish {
    long mtype;
    char mtext[NAME_LEN];
};

void msg_put(const char *dish) {
    if (strcmp(dish, "__EOF__") != 0) {
        struct msgbuf_dish token;
        msgrcv(msqid, &token, NAME_LEN, 2, 0);
    }
    struct msgbuf_dish msg = { .mtype = 1 };
    strncpy(msg.mtext, dish, NAME_LEN - 1);
    msgsnd(msqid, &msg, NAME_LEN, 0);
}

int msg_get(char *dish_out) {
    struct msgbuf_dish msg;
    if (msgrcv(msqid, &msg, NAME_LEN, 1, 0) == -1) return 0;
    strncpy(dish_out, msg.mtext, NAME_LEN);
    if (strcmp(dish_out, "__EOF__") == 0) return 0;
    
    struct msgbuf_dish token = { .mtype = 2, .mtext = "slot" };
    msgsnd(msqid, &token, NAME_LEN, 0);
    return 1;
}

typedef struct {
    int head, tail;
    char slots[][NAME_LEN];
} ShmTable;

int shmid;
ShmTable *shm_ptr;

void shm_put(const char *dish) {
    sem_wait(sem_empty);
    sem_wait(sem_mutex);
    strncpy(shm_ptr->slots[shm_ptr->tail], dish, NAME_LEN - 1);
    shm_ptr->tail = (shm_ptr->tail + 1) % (table_limit + 1);
    sem_post(sem_mutex);
    sem_post(sem_full);
}

int shm_get(char *dish_out) {
    sem_wait(sem_full);
    sem_wait(sem_mutex);
    strncpy(dish_out, shm_ptr->slots[shm_ptr->head], NAME_LEN);
    shm_ptr->head = (shm_ptr->head + 1) % (table_limit + 1);
    sem_post(sem_mutex);
    if (strcmp(dish_out, "__EOF__") == 0) return 0;
    sem_post(sem_empty);
    return 1;
}

int sock_fd[2];

void sock_put(const char *dish) {
    if (strcmp(dish, "__EOF__") != 0) {
        char ack;
        recv(sock_fd[0], &ack, 1, 0);
    }
    char buf[NAME_LEN] = {0};
    strncpy(buf, dish, NAME_LEN - 1);
    send(sock_fd[0], buf, NAME_LEN, 0);
}

int sock_get(char *dish_out) {
    if (recv(sock_fd[1], dish_out, NAME_LEN, MSG_WAITALL) <= 0) return 0;
    if (strcmp(dish_out, "__EOF__") == 0) return 0;
    char ack = '1';
    send(sock_fd[1], &ack, 1, 0);
    return 1;
}

void run_washer(IPCBackend *ipc) {
    Entry dirty[MAX_TYPES * 4];
    int dirty_cnt = parse_file("dirty.txt", dirty);

    for (int i = 0; i < dirty_cnt; i++) {
        int t = get_time(wash_times, wash_cnt, dirty[i].name);
        for (int k = 0; k < dirty[i].val; k++) {
            printf("[Мойщик] Мою: %s (%d сек)...\n", dirty[i].name, t);
            fflush(stdout);
            sleep(t);
            ipc->put(dirty[i].name);
            printf("[Мойщик] Положил на стол: %s\n", dirty[i].name);
            fflush(stdout);
        }
    }
    ipc->put("__EOF__");
    printf("[Мойщик] Вся посуда вымыта.\n");
}

void run_wiper(IPCBackend *ipc) {
    char dish[NAME_LEN];
    while (ipc->get(dish)) {
        int t = get_time(wipe_times, wipe_cnt, dish);
        printf("  [Вытирающий] Взял со стола: %s. Вытираю (%d сек)...\n", dish, t);
        fflush(stdout);
        sleep(t);
        printf("  [Вытирающий] Вытер и убрал в шкаф: %s\n", dish);
        fflush(stdout);
    }
    printf("  [Вытирающий] Работа окончена.\n");
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Использование: TABLE_LIMIT=3 %s <file|pipe|msg|shm|sock>\n", argv[0]);
        return 1;
    }

    const char *env_lim = getenv("TABLE_LIMIT");
    if (env_lim) table_limit = atoi(env_lim);
    if (table_limit <= 0) table_limit = 3;

    wash_cnt = parse_file("wash.txt", wash_times);
    wipe_cnt = parse_file("wipe.txt", wipe_times);

    IPCBackend ipc = {0};
    const char *mode = argv[1];

    sem_unlink("/dish_empty"); sem_unlink("/dish_full"); sem_unlink("/dish_mutex");
    sem_empty = sem_open("/dish_empty", O_CREAT, 0644, table_limit);
    sem_full  = sem_open("/dish_full",  O_CREAT, 0644, 0);
    sem_mutex = sem_open("/dish_mutex", O_CREAT, 0644, 1);

    if (strcmp(mode, "file") == 0) {
        unlink(ipc_filename);
        fclose(fopen(ipc_filename, "w"));
        ipc.put = file_put; ipc.get = file_get;
    } else if (strcmp(mode, "pipe") == 0) {
        pipe(data_pipe); pipe(ack_pipe);
        for (int i = 0; i < table_limit; i++) write(ack_pipe[1], "1", 1);
        ipc.put = pipe_put; ipc.get = pipe_get;
    } else if (strcmp(mode, "msg") == 0) {
        msqid = msgget(IPC_PRIVATE, IPC_CREAT | 0666);
        for (int i = 0; i < table_limit; i++) {
            struct msgbuf_dish tok = { .mtype = 2, .mtext = "slot" };
            msgsnd(msqid, &tok, NAME_LEN, 0);
        }
        ipc.put = msg_put; ipc.get = msg_get;
    } else if (strcmp(mode, "shm") == 0) {
        shmid = shmget(IPC_PRIVATE, sizeof(ShmTable) + (table_limit + 1) * NAME_LEN, IPC_CREAT | 0666);
        shm_ptr = shmat(shmid, NULL, 0);
        shm_ptr->head = 0; shm_ptr->tail = 0;
        ipc.put = shm_put; ipc.get = shm_get;
    } else if (strcmp(mode, "sock") == 0) {
        socketpair(AF_UNIX, SOCK_STREAM, 0, sock_fd);
        for (int i = 0; i < table_limit; i++) send(sock_fd[1], "1", 1, 0);
        ipc.put = sock_put; ipc.get = sock_get;
    } else {
        fprintf(stderr, "Неизвестный режим: %s\n", mode);
        return 1;
    }

    printf("=== Запуск моделирования (режим: %s, TABLE_LIMIT=%d) ===\n", mode, table_limit);

    pid_t p1 = fork();
    if (p1 == 0) { run_washer(&ipc); _exit(0); }

    pid_t p2 = fork();
    if (p2 == 0) { run_wiper(&ipc); _exit(0); }

    waitpid(p1, NULL, 0);
    waitpid(p2, NULL, 0);

    sem_close(sem_empty); sem_close(sem_full); sem_close(sem_mutex);
    sem_unlink("/dish_empty"); sem_unlink("/dish_full"); sem_unlink("/dish_mutex");
    if (strcmp(mode, "msg") == 0) msgctl(msqid, IPC_RMID, NULL);
    if (strcmp(mode, "shm") == 0) { shmdt(shm_ptr); shmctl(shmid, IPC_RMID, NULL); }

    return 0;
}

