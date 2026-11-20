#include "chat.h"

long hash_name(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = (unsigned char)*str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return (hash % 1000000) + 1;
}

void sem_lock(int semid) {
    struct sembuf sb[1];
    sb[0].sem_num = 0;
    sb[0].sem_op = -1;
    sb[0].sem_flg = SEM_UNDO;
    if (semop(semid, sb, 1) < 0) {
        exit(1);
    }
}

void sem_unlock(int semid) {
    struct sembuf sb[1];
    sb[0].sem_num = 0;
    sb[0].sem_op = 1;
    sb[0].sem_flg = SEM_UNDO;
    if (semop(semid, sb, 1) < 0) {
        exit(1);
    }
}

void get_time_str(char *buf, size_t size) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    strftime(buf, size, "%H:%M:%S", t);
}

void disable_raw_mode() {
    tcsetattr(STDIN_FILENO, TCSANOW, &orig_termios);
}

void enable_raw_mode() {
    tcgetattr(STDIN_FILENO, &orig_termios);
    atexit(disable_raw_mode);
    struct termios raw = orig_termios;
    raw.c_lflag &= ~(ICANON | ECHO);
    tcsetattr(STDIN_FILENO, TCSANOW, &raw);
}

int read_chat_line(char *out_buf, int max_len) {
    input_pos = 0;
    current_input[0] = '\0';
    printf(CLR_GREEN "You > " CLR_RESET);
    fflush(stdout);

    char c;
    while (read(STDIN_FILENO, &c, 1) == 1) {
        if (c == '\n' || c == '\r') {
            current_input[input_pos] = '\0';
            strcpy(out_buf, current_input);
            input_pos = 0;
            current_input[0] = '\0';
            return 1;
        } else if (c == 127 || c == '\b') {
            if (input_pos > 0) {
                input_pos--;
                while (input_pos > 0 && (current_input[input_pos] & 0xC0) == 0x80) {
                    input_pos--;
                }
                current_input[input_pos] = '\0';
                printf("\r\033[K" CLR_GREEN "You > " CLR_RESET "%s", current_input);
                fflush(stdout);
            }
        } else if ((unsigned char)c >= 32 && input_pos < max_len - 1) {
            current_input[input_pos++] = c;
            current_input[input_pos] = '\0';
            write(STDOUT_FILENO, &c, 1);
        }
    }
    return 0;
}
