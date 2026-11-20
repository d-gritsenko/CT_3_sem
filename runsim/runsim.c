#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/mount.h>
#include <sched.h>
#include <signal.h>
#include <errno.h>
#include <dirent.h>
#include <ctype.h>

volatile sig_atomic_t active_procs = 0;

void sigchld_handler(int sig) {
    (void)sig;
    int saved_errno = errno;
    while (waitpid(-1, NULL, WNOHANG) > 0) {
        active_procs--;
    }
    errno = saved_errno;
}

void list_namespace_procs(int use_ns) {
    if (!use_ns) {
        printf("[!] Опция --list работает в полную силу при запуске с флагом -p (PID namespace).\n");
    }
    DIR *dir = opendir("/proc");
    if (!dir) {
        perror("opendir /proc");
        return;
    }
    printf("%-8s %s\n", "PID", "COMMAND");
    printf("------------------------\n");
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (!isdigit(entry->d_name[0])) continue;
        
        char path[512];
        snprintf(path, sizeof(path), "/proc/%s/comm", entry->d_name);
        FILE *f = fopen(path, "r");
        if (f) {
            char comm[256] = {0};
            if (fgets(comm, sizeof(comm), f)) {
                comm[strcspn(comm, "\n")] = 0;
                printf("%-8s %s\n", entry->d_name, comm);
            }
            fclose(f);
        }
    }
    closedir(dir);
}

int run_loop(int max_procs, int use_ns) {
    if (use_ns) {
        if (mount("none", "/", NULL, MS_REC | MS_PRIVATE, NULL) == -1) {
            perror("mount MS_PRIVATE");
            return 1;
        }
        if (mount("proc", "/proc", "proc", MS_NOSUID | MS_NOEXEC | MS_NODEV, NULL) == -1) {
            perror("mount /proc");
            return 1;
        }
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    char line[1024];
    while (fgets(line, sizeof(line), stdin) != NULL) {
        line[strcspn(line, "\n")] = 0;
        if (strlen(line) == 0) continue;

        if (strcmp(line, "--list") == 0) {
            list_namespace_procs(use_ns);
            continue;
        }

        sigset_t mask, oldmask;
        sigemptyset(&mask);
        sigaddset(&mask, SIGCHLD);
        sigprocmask(SIG_BLOCK, &mask, &oldmask);

        if (active_procs >= max_procs) {
            fprintf(stderr, "Ошибка: превышен лимит одновременных процессов (%d)\n", max_procs);
            sigprocmask(SIG_SETMASK, &oldmask, NULL);
            continue;
        }

        pid_t pid = fork();
        if (pid < 0) {
            perror("fork");
            sigprocmask(SIG_SETMASK, &oldmask, NULL);
        } else if (pid == 0) {
            sigprocmask(SIG_SETMASK, &oldmask, NULL);
            execl("/bin/sh", "sh", "-c", line, (char *)NULL);
            perror("execl");
            _exit(127);
        } else {
            active_procs++;
            sigprocmask(SIG_SETMASK, &oldmask, NULL);
        }
    }

    while (active_procs > 0) {
        pause();
    }
    return 0;
}

int main(int argc, char *argv[]) {
    int use_ns = 0;
    int max_procs = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-p") == 0) {
            use_ns = 1;
        } else {
            max_procs = atoi(argv[i]);
        }
    }

    if (max_procs <= 0) {
        fprintf(stderr, "Использование: %s [-p] <N>\n", argv[0]);
        return 1;
    }

    if (use_ns) {
        if (unshare(CLONE_NEWPID | CLONE_NEWNS) == -1) {
            perror("unshare (запустите через sudo или добавьте CLONE_NEWUSER)");
            return 1;
        }
        pid_t pid = fork();
        if (pid < 0) {
            perror("fork ns init");
            return 1;
        }
        if (pid == 0) {
            return run_loop(max_procs, 1);
        }
        waitpid(pid, NULL, 0);
        return 0;
    }

    return run_loop(max_procs, 0);
}