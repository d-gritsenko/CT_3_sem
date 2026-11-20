#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINES 256
#define MAX_VARS  64
#define MAX_STACK 32

typedef struct {
    char name[32];
    int value;
} Var;

typedef struct {
    int return_pc;
    Var locals[MAX_VARS];
    int local_cnt;
} StackFrame;

char source[MAX_LINES][128];
int line_cnt = 0;

Var globals[MAX_VARS];
int global_cnt = 0;

StackFrame call_stack[MAX_STACK];
int sp = 0;
int pc = 0;

int find_func(const char *name) {
    char target[64];
    snprintf(target, sizeof(target), "def %s", name);
    for (int i = 0; i < line_cnt; i++) {
        if (strncmp(source[i], target, strlen(target)) == 0) return i + 1;
    }
    return -1;
}

int* get_var_ptr(const char *name, int force_local) {
    if (sp > 0) {
        StackFrame *frame = &call_stack[sp];
        for (int i = 0; i < frame->local_cnt; i++) {
            if (strcmp(frame->locals[i].name, name) == 0) return &frame->locals[i].value;
        }
        if (force_local) {
            strncpy(frame->locals[frame->local_cnt].name, name, 31);
            frame->locals[frame->local_cnt].value = 0;
            return &frame->locals[frame->local_cnt++].value;
        }
    }
    for (int i = 0; i < global_cnt; i++) {
        if (strcmp(globals[i].name, name) == 0) return &globals[i].value;
    }
    return NULL;
}

void set_var(const char *name, int val, int is_local) {
    int *ptr = get_var_ptr(name, is_local);
    if (!ptr) {
        strncpy(globals[global_cnt].name, name, 31);
        globals[global_cnt].value = val;
        global_cnt++;
    } else {
        *ptr = val;
    }
}

void execute_instruction(void) {
    char *line = source[pc];

    if (strncmp(line, "def ", 4) == 0) {
        while (pc < line_cnt && strcmp(source[pc], "end") != 0) pc++;
        pc++;
        return;
    }
    if (strcmp(line, "end") == 0) {
        if (sp > 0) {
            pc = call_stack[sp].return_pc;
            sp--;
        } else {
            pc++;
        }
        return;
    }
    if (strncmp(line, "call ", 5) == 0) {
        char fname[32];
        sscanf(line + 5, "%31s", fname);
        int target = find_func(fname);
        if (target < 0) {
            fprintf(stderr, "Ошибка: функция %s не найдена\n", fname);
            exit(1);
        }
        sp++;
        call_stack[sp].return_pc = pc + 1;
        call_stack[sp].local_cnt = 0;
        pc = target;
        return;
    }
    if (strncmp(line, "echo ", 5) == 0) {
        char vname[32];
        sscanf(line + 5, "%31s", vname);
        int *ptr = get_var_ptr(vname, 0);
        if (ptr) printf("[OUT] %s = %d\n", vname, *ptr);
        else printf("[OUT] Переменная %s не найдена\n", vname);
        pc++;
        return;
    }
    if (strncmp(line, "let ", 4) == 0) {
        char vname[32]; int val = 0;
        if (sscanf(line + 4, "%31s = %d", vname, &val) == 2) {
            set_var(vname, val, 1);
        }
        pc++;
        return;
    }
    char vname[32]; int val = 0;
    if (sscanf(line, "%31s = %d", vname, &val) == 2) {
        set_var(vname, val, 0);
    }
    pc++;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Использование: %s <script.txt>\n", argv[0]);
        return 1;
    }

    FILE *f = fopen(argv[1], "r");
    if (!f) { perror(argv[1]); return 1; }
    while (fgets(source[line_cnt], sizeof(source[0]), f)) {
        char *p = source[line_cnt];
        while (*p == ' ' || *p == '\t') p++;
        p[strcspn(p, "\r\n")] = 0;
        if (strlen(p) > 0) {
            memmove(source[line_cnt], p, strlen(p) + 1);
            line_cnt++;
        }
    }
    fclose(f);

    int run_mode = 0;
    int target_sp = 0;

    printf("=== Отладчик запущен. Команды: step, next, continue, print <VAR> ===\n");

    while (pc < line_cnt) {
        if (strncmp(source[pc], "def ", 4) == 0) {
            execute_instruction();
            continue;
        }

        int should_break = 0;
        if (run_mode == 0) should_break = 1;
        else if (run_mode == 1 && sp <= target_sp) should_break = 1;

        if (should_break) {
            run_mode = 0;
            while (1) {
                printf("\n[Строка %d | Глубина стека %d] --> %s\n(dbg) ", pc + 1, sp, source[pc]);
                char cmd[128];
                if (!fgets(cmd, sizeof(cmd), stdin)) return 0;
                cmd[strcspn(cmd, "\r\n")] = 0;

                if (strcmp(cmd, "step") == 0 || strcmp(cmd, "s") == 0) {
                    run_mode = 0;
                    break;
                } else if (strcmp(cmd, "next") == 0 || strcmp(cmd, "n") == 0) {
                    run_mode = 1;
                    target_sp = sp;
                    break;
                } else if (strcmp(cmd, "continue") == 0 || strcmp(cmd, "c") == 0) {
                    run_mode = 2;
                    break;
                } else if (strncmp(cmd, "print ", 6) == 0) {
                    char expr[32];
                    sscanf(cmd + 6, "%31s", expr);
                    int *ptr = get_var_ptr(expr, 0);
                    if (ptr) printf("%s = %d\n", expr, *ptr);
                    else printf("Переменная '%s' не определена в текущей области видимости.\n", expr);
                } else {
                    printf("Доступные команды: step (s), next (n), continue (c), print <VAR>\n");
                }
            }
        }
        execute_instruction();
    }

    printf("\n=== Программа завершена ===\n");
    return 0;
}

