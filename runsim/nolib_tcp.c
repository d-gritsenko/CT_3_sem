#define SYS_READ    0
#define SYS_WRITE   1
#define SYS_CLOSE   3
#define SYS_SOCKET  41
#define SYS_CONNECT 42
#define SYS_EXIT    60

#define AF_INET     2
#define SOCK_STREAM 1

struct sockaddr_in {
    unsigned short sin_family;
    unsigned short sin_port;
    unsigned int   sin_addr;
    unsigned char  sin_zero[8];
};

static inline long syscall1(long n, long a1) {
    long ret;
    __asm__ volatile ("syscall" : "=a"(ret) : "a"(n), "D"(a1) : "rcx", "r11", "memory");
    return ret;
}

static inline long syscall3(long n, long a1, long a2, long a3) {
    long ret;
    __asm__ volatile ("syscall" : "=a"(ret) : "a"(n), "D"(a1), "S"(a2), "d"(a3) : "rcx", "r11", "memory");
    return ret;
}

static void parse_two_ints(const char *buf, long len, long *a, long *b) {
    long i = 0;
    long nums[2] = {0, 0};
    for (int k = 0; k < 2; k++) {
        while (i < len && (buf[i] < '0' || buf[i] > '9') && buf[i] != '-') i++;
        long sign = 1;
        if (i < len && buf[i] == '-') { sign = -1; i++; }
        long val = 0;
        while (i < len && buf[i] >= '0' && buf[i] <= '9') {
            val = val * 10 + (buf[i] - '0');
            i++;
        }
        nums[k] = val * sign;
    }
    *a = nums[0];
    *b = nums[1];
}

static int long_to_str(long val, char *out) {
    char tmp[32];
    int pos = 0, out_pos = 0;
    if (val == 0) {
        out[0] = '0'; out[1] = '\n';
        return 2;
    }
    if (val < 0) {
        out[out_pos++] = '-';
        val = -val;
    }
    while (val > 0) {
        tmp[pos++] = '0' + (val % 10);
        val /= 10;
    }
    while (pos > 0) {
        out[out_pos++] = tmp[--pos];
    }
    out[out_pos++] = '\n';
    return out_pos;
}

void _start(void) {
    int fd = syscall3(SYS_SOCKET, AF_INET, SOCK_STREAM, 0);
    if (fd < 0) syscall1(SYS_EXIT, 1);

    struct sockaddr_in addr;
    for (int i = 0; i < 8; i++) addr.sin_zero[i] = 0;
    addr.sin_family = AF_INET;
    addr.sin_port = 0x901F;
    addr.sin_addr = 0x0100007F;

    if (syscall3(SYS_CONNECT, fd, (long)&addr, sizeof(addr)) < 0) {
        syscall1(SYS_EXIT, 2);
    }

    char buf[128];
    long n = syscall3(SYS_READ, fd, (long)buf, sizeof(buf) - 1);
    if (n > 0) {
        long a = 0, b = 0;
        parse_two_ints(buf, n, &a, &b);
        char out[32];
        int out_len = long_to_str(a + b, out);
        syscall3(SYS_WRITE, fd, (long)out, out_len);
    }

    syscall1(SYS_CLOSE, fd);
    syscall1(SYS_EXIT, 0);
}

