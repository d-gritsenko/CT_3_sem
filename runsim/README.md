# Задачи по системному программированию в Linux (КТ)

Реализация задач по управлению процессами, пространствам имён (PID namespaces), межпроцессному взаимодействию (IPC), системным вызовам без `libc`, сборке ядра Linux и созданию отладчика.

## Сборка всех программ

Для компиляции всех программ выполните:
```bash
make
```

Для очистки скомпилированных бинарных файлов:
```bash
make clean
```

---

## 1–2. Контроль количества процессов и PID Namespace (`runsim`)

| Режим запуска | Команда | Описание |
| :--- | :--- | :--- |
| Обычный режим | `./runsim 2` | Ограничение до `N` одновременных процессов |
| PID Namespace | `sudo ./runsim -p 2` | Изоляция процессов + поддержка команды `--list` |

---

## 3. Мойка посуды (5 механизмов IPC: `dishes`)

Перед запуском убедитесь, что в рабочей директории находятся файлы `wash.txt`, `wipe.txt` и `dirty.txt`. Размер стола задаётся через переменную окружения `TABLE_LIMIT`.

| Механизм IPC | Команда запуска |
| :--- | :--- |
| Файл + семафоры | `TABLE_LIMIT=3 ./dishes file` |
| Неименованный канал (pipe) | `TABLE_LIMIT=3 ./dishes pipe` |
| Очередь сообщений (SysV msg) | `TABLE_LIMIT=3 ./dishes msg` |
| Разделяемая память (SHM) | `TABLE_LIMIT=3 ./dishes shm` |
| Сокеты (`socketpair`) | `TABLE_LIMIT=3 ./dishes sock` |

---

## 4*. TCP-клиент без использования `libc` (`nolibc_tcp`)

Программа написана исключительно на прямых системных вызовах `x86_64` (`syscall`) с точкой входа `_start`. Подключается к `127.0.0.1:8080`, считывает два числа и возвращает их сумму.

**Проверка работы:**
1. В первом терминале запустите тестовый сервер через `netcat`:
```bash
echo "125 -25" | nc -l -p 8080
```
2. Во втором терминале запустите клиент:
```bash
./nolibc_tcp
```

---

## 5*. Сборка ядра Linux с патчем и запуск в QEMU

1. **Установка пакетов и скачивание исходников:**
```bash
sudo apt update
sudo apt install build-essential libncurses-dev bison flex libssl-dev libelf-dev qemu-system-x86 busybox-static
wget https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.6.50.tar.xz
tar -xf linux-6.6.50.tar.xz && cd linux-6.6.50
```

2. **Применение патча и сборка:**
```bash
make x86_64_defconfig
make -j$(nproc) bzImage
```

3. **Создание минимального `initramfs`:**
```bash
mkdir -p initramfs/{bin,proc,sys,dev}
cp /bin/busybox initramfs/bin/
cat << 'EOF' > initramfs/init
#!/bin/busybox sh
/bin/busybox mount -t proc none /proc
/bin/busybox mount -t sysfs none /sys
echo -e "\n=== Kernel loaded: $(/bin/busybox uname -r) ==="
exec /bin/busybox sh
EOF
chmod +x initramfs/init
(cd initramfs && find . | cpio -o -H newc | gzip > ../initramfs.cpio.gz)
```

4. **Запуск в QEMU:**
```bash
qemu-system-x86_64 -kernel arch/x86/boot/bzImage -initrd initramfs.cpio.gz -append "console=ttyS0 quiet" -nographic
```

---

## 6*. Отладчик для интерпретируемого языка (`debugger`)

Запуск отладчика на тестовом скрипте:
```bash
./debugger script.txt
```

**Доступные команды в интерактивном режиме `(dbg)`:**

| Команда | Сокращение | Действие |
| :--- | :--- | :--- |
| `step` | `s` | Выполнить одну строку с заходом внутрь функций (`call`) |
| `next` | `n` | Выполнить одну строку без захода внутрь вызываемых функций |
| `continue` | `c` | Выполнить программу до конца |
| `print <VAR>` | — | Вывести текущее значение локальной или глобальной переменной |