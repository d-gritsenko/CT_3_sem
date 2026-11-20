#include "chat.h"

char current_input[3900] = {0};
int input_pos = 0;
struct termios orig_termios;
union semnum sem_arg;
int shm_id = -1, sem_id = -1, msg_qid = -1;
struct chat_memory *shm_buf = (struct chat_memory *)(-1);
pid_t my_pid;
char my_name[64];
pthread_t recv_thread;
int thread_started = 0;

int main(int argc, char *argv[], char *envp[]) {
    signal(SIGINT, handle_sigint);
    struct shmid_ds ds;
    unsigned short sem_vals[1];
    int is_creator = 0;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <Your_Name> [--env]\n", argv[0]);
        return 1;
    }
    strncpy(my_name, argv[1], sizeof(my_name) - 1);

    if (argc >= 3 && strcmp(argv[2], "--env") == 0) {
        for (int i = 0; envp[i] != NULL; i++) printf("envp[%d]: %s\n", i, envp[i]);
        for (int i = 0; i < argc; i++) printf("argv[%d]: %s\n", i, argv[i]);
    }

    signal(SIGINT, cleanup);
    signal(SIGHUP, cleanup);
    signal(SIGTERM, cleanup);

    my_pid = getpid();
    pid_t my_ppid = getppid();
    
    shm_id = shmget(SHM_KEY, SHMEM_SIZE, IPC_CREAT | IPC_EXCL | 0666);
    if (shm_id >= 0) {
        is_creator = 1;
    } else {
        shm_id = shmget(SHM_KEY, SHMEM_SIZE, 0666);
        if (shm_id < 0) {
            fprintf(stderr, "shmget() error\n");
            return 1;
        }
    }

    sem_id = semget(SEM_KEY, 1, 0666 | IPC_CREAT);
    if (sem_id == -1) {
        fprintf(stderr, "semget() error\n");
        return 1;
    }

    msg_qid = msgget(MSG_KEY, IPC_CREAT | 0666);
    if (msg_qid == -1) {
        fprintf(stderr, "msgget() error\n");
        return 1;
    }

    shm_buf = (struct chat_memory *)shmat(shm_id, NULL, 0);
    if (shm_buf == (struct chat_memory *)(-1)) {
        fprintf(stderr, "shmat() error\n");
        return 1;
    }

    shmctl(shm_id, IPC_STAT, &ds);
    int active_users = (int)ds.shm_nattch;

    if (is_creator || active_users == 1) {
        sem_vals[0] = 1;
        sem_arg.array = sem_vals;
        if (semctl(sem_id, 0, SETALL, sem_arg) == -1) {
            fprintf(stderr, "semctl() SETALL error\n");
            return 1;
        }
    }

    sem_lock(sem_id);
    if (is_creator || active_users == 1) {
        shm_buf->msg_id = 0;
        shm_buf->sender_pid = 0;
        shm_buf->text[0] = '\0';
        close(open(LOG_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0666));
    }
    int initial_msg_id = shm_buf->msg_id;
    sem_unlock(sem_id);

    printf(CLR_YELLOW "============================================================\n");
    printf(" CHAT STARTED | User: %-15s | PID: %d (PPID: %d)\n", my_name, (int)my_pid, (int)my_ppid);
    printf(" SHM ID: %-4d | SEM ID: %-4d           | Active Users: %d\n", shm_id, sem_id, active_users);
    printf(" MSG QID: %-3d |\n", msg_qid);
    printf("------------------------------------------------------------\n");
    printf(" Commands: /history, /file, /calc <num>, /pm <Name> <Msg>, /exit\n");
    printf("============================================================\n\n" CLR_RESET);

    if (pthread_create(&recv_thread, NULL, &receiver_thread_func, (void *)&initial_msg_id) != 0) {
        fprintf(stderr, "Error creating thread\n");
        return 1;
    }
    thread_started = 1;

    enable_raw_mode();

    char input[3900];
    char tbuf[16];
    while (read_chat_line(input, sizeof(input))) {
        if (strlen(input) == 0) {
            printf("\r\033[K");
            continue;
        }

        get_time_str(tbuf, sizeof(tbuf));

        if (strcmp(input, "/exit") == 0) {
            break;
        }

        if (strncmp(input, "/pm ", 4) == 0) {
            char target_name[64];
            char pm_text[3900];
            
            if (sscanf(input + 4, "%63s %[^\n]", target_name, pm_text) == 2) {
                struct pm_msg pmsg;
                pmsg.mtype = hash_name(target_name);
                strncpy(pmsg.sender_name, my_name, sizeof(pmsg.sender_name));
                strcpy(pmsg.text, pm_text);
                
                msgsnd(msg_qid, &pmsg, sizeof(pmsg) - sizeof(long), 0);
                
                printf("\r\033[K" CLR_GRAY "[%s]" CLR_YELLOW " [PM to %s]:" CLR_RESET " %s\n", tbuf, target_name, pm_text);
            } else {
                printf("\r\033[K" CLR_YELLOW "[System]: Usage: /pm <Name> <Text>\n" CLR_RESET);
            }
            continue;
        }

        if (strncmp(input, "/calc ", 6) == 0) {
            printf("\r\033[K" CLR_GRAY "[%s]" CLR_RESET " " CLR_GREEN "[You]:" CLR_RESET " %s\n", tbuf, input);
            int num = atoi(input + 6);
            int pipe_to_child[2], pipe_to_parent[2];
            
            if (pipe(pipe_to_child) < 0 || pipe(pipe_to_parent) < 0) {
                printf(CLR_YELLOW "Pipe creation error\n" CLR_RESET);
                continue;
            }
            
            pid_t calc_pid = fork();
            if (calc_pid < 0) {
                printf(CLR_YELLOW "Fork error\n" CLR_RESET);
            } else if (calc_pid == 0) { 
                close(pipe_to_child[1]); 
                close(pipe_to_parent[0]); 
                
                int received_num;
                if (read(pipe_to_child[0], &received_num, sizeof(int)) > 0) {
                    int result = received_num * received_num;
                    write(pipe_to_parent[1], &result, sizeof(int));
                }
                
                close(pipe_to_child[0]);
                close(pipe_to_parent[1]);
                _exit(0); 
            } else { 
                close(pipe_to_child[0]);  
                close(pipe_to_parent[1]); 
                
                write(pipe_to_child[1], &num, sizeof(int));
                
                int final_result;
                if (read(pipe_to_parent[0], &final_result, sizeof(int)) > 0) {
                    printf(CLR_YELLOW "[System]: Result of /calc: %d^2 = %d\n" CLR_RESET, num, final_result);
                }
                
                close(pipe_to_child[1]);
                close(pipe_to_parent[0]);
            }
            continue;
        }

        if (strcmp(input, "/history") == 0) {
            printf("\r\033[K" CLR_GRAY "[%s]" CLR_RESET " " CLR_GREEN "[You]:" CLR_RESET " %s\n", tbuf, input);
            pid_t chpid = fork();
            if (chpid < 0) {
                printf("Can't fork a child process\n");
            } else if (chpid == 0) {
                printf("\n--- Message History (Child PID: %d, PPID: %d) ---\n",
                       (int)getpid(), (int)getppid());
                int res = execle("/bin/cat", "/bin/cat", LOG_FILE, NULL, envp);
                if (res < 0) {
                    printf("History is empty or /bin/cat failed to start\n");
                    exit(-1);
                }
            } else {
                waitpid(chpid, NULL, 0);
                printf("--- End of History ---\n");
            }
            continue;
        }

        if (strcmp(input, "/file") == 0) {
            printf("\r\033[K" CLR_GRAY "[%s]" CLR_RESET " " CLR_GREEN "[You]:" CLR_RESET " %s\n", tbuf, input);
            int fd = open(SRC_FILE, O_RDONLY);
            if (fd < 0) {
                printf("Failed to open source file %s\n", SRC_FILE);
                continue;
            }
            sem_lock(sem_id);
            char *p = shm_buf->text;
            int nread, total = 0;
            while ((nread = read(fd, p, 256)) > 0 && (total + nread < (int)sizeof(shm_buf->text) - 1)) {
                p += nread;
                total += nread;
            }
            close(fd);
            *p = '\0';

            shm_buf->sender_pid = my_pid;
            strncpy(shm_buf->sender_name, my_name, sizeof(shm_buf->sender_name) - 1);
            shm_buf->msg_id++;
            append_to_log(my_name, my_pid, shm_buf->text);
            sem_unlock(sem_id);

            printf("[File %s sent to shared memory]\n", SRC_FILE);
            continue;
        }

        printf("\r\033[K" CLR_GRAY "[%s]" CLR_RESET " " CLR_GREEN "[You]:" CLR_RESET " %s\n", tbuf, input);
        sem_lock(sem_id);
        shm_buf->sender_pid = my_pid;
        strncpy(shm_buf->sender_name, my_name, sizeof(shm_buf->sender_name) - 1);
        strcpy(shm_buf->text, input);
        shm_buf->msg_id++;
        append_to_log(my_name, my_pid, input);
        sem_unlock(sem_id);
    }

    cleanup(0);
    return 0;
}
