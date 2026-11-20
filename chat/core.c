#include "chat.h"

void cleanup(int sig) {
    void *thread_result;

    if (sig == 0 && thread_started) {
        pthread_cancel(recv_thread);
        if (!pthread_equal(pthread_self(), recv_thread)) {
            pthread_join(recv_thread, &thread_result);
        }
        if (thread_result == PTHREAD_CANCELED) {
            fprintf(stderr, "[Receiver thread cleanly stopped (Canceled)]\n");
        }
    }

    if (shm_buf != (struct chat_memory *)(-1)) {
        shmdt(shm_buf);
        shm_buf = (struct chat_memory *)(-1);
    }

    if (shm_id != -1) {
        struct shmid_ds ds;
        if (shmctl(shm_id, IPC_STAT, &ds) == 0 && ds.shm_nattch == 0) {
            shmctl(shm_id, IPC_RMID, NULL);
            if (sem_id != -1) {
                semctl(sem_id, 0, IPC_RMID, sem_arg);
            }
            if (msg_qid != -1) {
                msgctl(msg_qid, IPC_RMID, NULL);
            }
            remove(LOG_FILE);
            if (sig == 0) {
                printf("[IPC Resources removed from system]\n");
            }
        }
    }

    disable_raw_mode();
    exit(0);
}

void append_to_log(const char *name, pid_t pid, const char *msg) {
    int fd = open(LOG_FILE, O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (fd < 0) {
        return;
    }
    char line[4096];
    int len = snprintf(line, sizeof(line), "[%s (PID %d)]: %s\n", name, (int)pid, msg);
    (void)write(fd, line, len);
    close(fd);
}

void *receiver_thread_func(void *arg) {
    int last_seen_id = *(int *)arg;
    char tbuf[16];
    long my_mtype = hash_name(my_name);
    struct pm_msg pmsg;

    while (1) {
        usleep(100000);

        while (msgrcv(msg_qid, &pmsg, sizeof(pmsg) - sizeof(long), my_mtype, IPC_NOWAIT) > 0) {
            get_time_str(tbuf, sizeof(tbuf));
            printf("\r\033[K" CLR_GRAY "[%s]" CLR_YELLOW " [PM from %s]:" CLR_RESET " %s\n" CLR_GREEN "You > " CLR_RESET "%s",
                   tbuf, pmsg.sender_name, pmsg.text, current_input);
            fflush(stdout);
        }

        sem_lock(sem_id);
        if (shm_buf->msg_id > last_seen_id) {
            last_seen_id = shm_buf->msg_id;
            if (shm_buf->sender_pid != my_pid) {
                get_time_str(tbuf, sizeof(tbuf));
                printf("\r\033[K" CLR_GRAY "[%s]" CLR_RESET " " CLR_CYAN "[%s (PID %d)]:" CLR_RESET " %s\n" CLR_GREEN "You > " CLR_RESET "%s",
                       tbuf, shm_buf->sender_name, (int)shm_buf->sender_pid, shm_buf->text, current_input);
                fflush(stdout);
            }
        }
        sem_unlock(sem_id);
    }
    return NULL;
}

void handle_sigint(int sig) {
    fclose(stdin);
}
