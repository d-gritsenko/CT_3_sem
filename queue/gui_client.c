#define _DEFAULT_SOURCE

#include <errno.h>
#include <gtk/gtk.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "protocol.h"

enum ui_event_kind {
    EV_CHAT = 1,
    EV_ACK = 2,
    EV_LOST = 3
};

struct ui_event {
    int kind;
    long msg_id;
    char sender[MAX_NAME];
    char text[MAX_TEXT];
};

static volatile sig_atomic_t g_running = 1;
static int g_msqid = -1;
static long g_my_id = 0;
static long g_reply_type = 0;
static long g_next_msg_id = 1;
static char g_my_name[MAX_NAME];
static pthread_mutex_t g_mtx = PTHREAD_MUTEX_INITIALIZER;

static GtkWidget *w_window = NULL;
static GtkWidget *w_nick_entry = NULL;
static GtkWidget *w_conn_btn = NULL;
static GtkWidget *w_status_lbl = NULL;
static GtkWidget *w_chat_view = NULL;
static GtkTextBuffer *w_chat_buf = NULL;
static GtkWidget *w_target_entry = NULL;
static GtkWidget *w_msg_entry = NULL;
static GtkWidget *w_send_btn = NULL;

static void get_timestamp(char *buf, size_t size) {
    time_t now = time(NULL);
    struct tm tm_now;
    localtime_r(&now, &tm_now);
    strftime(buf, size, "%H:%M:%S", &tm_now);
}

static int wait_reply(int msqid, long reply_type, struct qmsg *rep) {
    for (int i = 0; i < 200; i++) {
        ssize_t n = msgrcv(msqid, rep, MSG_SIZE, reply_type, IPC_NOWAIT);
        if (n >= 0) {
            return 0;
        }
        if (errno == ENOMSG) {
            usleep(10000);
            continue;
        }
        if (errno == EINTR) {
            continue;
        }
        return -1;
    }
    return -1;
}

static void send_logout(void) {
    pthread_mutex_lock(&g_mtx);
    long my_id = g_my_id;
    g_my_id = 0;
    g_my_name[0] = '\0';
    pthread_mutex_unlock(&g_mtx);

    if (my_id > 0 && g_msqid >= 0) {
        struct qmsg msg;
        memset(&msg, 0, sizeof(msg));
        msg.mtype = ARBITER_TYPE;
        msg.cmd = CMD_LOGOUT;
        msg.sender_id = my_id;
        msg.reply_type = g_reply_type;
        msgsnd(g_msqid, &msg, MSG_SIZE, IPC_NOWAIT);
    }
}

static void ui_append_line(const char *tag_name, const char *prefix, const char *body) {
    if (w_chat_buf == NULL || w_chat_view == NULL) {
        return;
    }

    char ts[16];
    get_timestamp(ts, sizeof(ts));

    GtkTextIter end;
    gtk_text_buffer_get_end_iter(w_chat_buf, &end);

    char ts_str[24];
    snprintf(ts_str, sizeof(ts_str), "[%s] ", ts);
    gtk_text_buffer_insert_with_tags_by_name(w_chat_buf, &end, ts_str, -1, "time", NULL);

    if (prefix != NULL && prefix[0] != '\0') {
        gtk_text_buffer_insert_with_tags_by_name(w_chat_buf, &end, prefix, -1, tag_name, NULL);
    }

    if (body != NULL && body[0] != '\0') {
        gtk_text_buffer_insert(w_chat_buf, &end, body, -1);
    }

    gtk_text_buffer_insert(w_chat_buf, &end, "\n", -1);

    GtkTextMark *mark = gtk_text_buffer_get_insert(w_chat_buf);
    gtk_text_buffer_get_end_iter(w_chat_buf, &end);
    gtk_text_buffer_move_mark(w_chat_buf, mark, &end);
    gtk_text_view_scroll_to_mark(GTK_TEXT_VIEW(w_chat_view), mark, 0.0, TRUE, 0.0, 1.0);
}

static void ui_set_connected_state(int connected, const char *name, long id) {
    gtk_widget_set_sensitive(w_nick_entry, !connected);
    gtk_widget_set_sensitive(w_target_entry, connected);
    gtk_widget_set_sensitive(w_msg_entry, connected);
    gtk_widget_set_sensitive(w_send_btn, connected);

    if (connected) {
        char status[128];
        char title[128];
        snprintf(status, sizeof(status), "Онлайн: %s (id=%ld)", name, id);
        snprintf(title, sizeof(title), "Queue Messenger — %s", name);
        gtk_label_set_text(GTK_LABEL(w_status_lbl), status);
        gtk_button_set_label(GTK_BUTTON(w_conn_btn), "Выйти");
        gtk_window_set_title(GTK_WINDOW(w_window), title);
        gtk_widget_grab_focus(w_target_entry);
    } else {
        gtk_label_set_text(GTK_LABEL(w_status_lbl), "Не подключён");
        gtk_button_set_label(GTK_BUTTON(w_conn_btn), "Войти");
        gtk_window_set_title(GTK_WINDOW(w_window), "Queue Messenger");
        gtk_widget_grab_focus(w_nick_entry);
    }
}

static gboolean ui_handle_event(gpointer user_data) {
    struct ui_event *ev = (struct ui_event *)user_data;

    if (!g_running || w_chat_buf == NULL) {
        free(ev);
        return G_SOURCE_REMOVE;
    }

    if (ev->kind == EV_CHAT) {
        char prefix[MAX_NAME + 8];
        snprintf(prefix, sizeof(prefix), "%s: ", ev->sender);
        ui_append_line("incoming", prefix, ev->text);
    } else if (ev->kind == EV_ACK) {
        char line[MAX_NAME + 64];
        snprintf(line, sizeof(line), "✓ доставлено %s (#%ld)", ev->sender, ev->msg_id);
        ui_append_line("ack", line, NULL);
    } else if (ev->kind == EV_LOST) {
        pthread_mutex_lock(&g_mtx);
        g_my_id = 0;
        g_my_name[0] = '\0';
        pthread_mutex_unlock(&g_mtx);
        ui_set_connected_state(0, "", 0);
        gtk_widget_set_sensitive(w_conn_btn, FALSE);
        gtk_widget_set_sensitive(w_nick_entry, FALSE);
        ui_append_line("err", "Соединение потеряно (арбитр остановлен).", NULL);
    }

    free(ev);
    return G_SOURCE_REMOVE;
}

static void *recv_worker(void *arg) {
    (void)arg;
    struct qmsg msg;

    while (g_running) {
        char cur_name[MAX_NAME];
        pthread_mutex_lock(&g_mtx);
        long my_id = g_my_id;
        snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
        pthread_mutex_unlock(&g_mtx);

        if (my_id <= 0) {
            usleep(50000);
            continue;
        }

        ssize_t n = msgrcv(g_msqid, &msg, MSG_SIZE, my_id, IPC_NOWAIT);
        if (n >= 0) {
            if (msg.cmd == CMD_CHAT) {
                msg.sender_name[MAX_NAME - 1] = '\0';
                msg.text[MAX_TEXT - 1] = '\0';

                if (msg.msg_id > 0 && msg.sender_name[0] != '\0') {
                    struct qmsg ack;
                    memset(&ack, 0, sizeof(ack));
                    ack.mtype = ARBITER_TYPE;
                    ack.cmd = CMD_ACK;
                    ack.sender_id = my_id;
                    ack.msg_id = msg.msg_id;
                    snprintf(ack.name, sizeof(ack.name), "%.*s", MAX_NAME - 1, msg.sender_name);
                    snprintf(ack.sender_name, sizeof(ack.sender_name), "%.*s", MAX_NAME - 1, cur_name);
                    msgsnd(g_msqid, &ack, MSG_SIZE, IPC_NOWAIT);
                }

                struct ui_event *ev = calloc(1, sizeof(*ev));
                if (ev != NULL) {
                    ev->kind = EV_CHAT;
                    ev->msg_id = msg.msg_id;
                    snprintf(ev->sender, sizeof(ev->sender), "%.*s", MAX_NAME - 1, msg.sender_name);
                    snprintf(ev->text, sizeof(ev->text), "%.*s", MAX_TEXT - 1, msg.text);
                    g_idle_add(ui_handle_event, ev);
                }
            } else if (msg.cmd == CMD_ACK) {
                msg.sender_name[MAX_NAME - 1] = '\0';

                struct ui_event *ev = calloc(1, sizeof(*ev));
                if (ev != NULL) {
                    ev->kind = EV_ACK;
                    ev->msg_id = msg.msg_id;
                    snprintf(ev->sender, sizeof(ev->sender), "%.*s", MAX_NAME - 1, msg.sender_name);
                    g_idle_add(ui_handle_event, ev);
                }
            }
        } else {
            if (errno == ENOMSG) {
                usleep(50000);
            } else if (errno == EINTR) {
                continue;
            } else {
                struct ui_event *ev = calloc(1, sizeof(*ev));
                if (ev != NULL) {
                    ev->kind = EV_LOST;
                    g_idle_add(ui_handle_event, ev);
                }
                g_running = 0;
                break;
            }
        }
    }

    return NULL;
}

static void *heartbeat_worker(void *arg) {
    (void)arg;
    struct qmsg hb;
    int ticks = 0;

    while (g_running) {
        usleep(100000);
        ticks++;
        if (ticks < HEARTBEAT_INTERVAL * 10) {
            continue;
        }
        ticks = 0;

        pthread_mutex_lock(&g_mtx);
        long my_id = g_my_id;
        pthread_mutex_unlock(&g_mtx);

        if (my_id > 0) {
            memset(&hb, 0, sizeof(hb));
            hb.mtype = ARBITER_TYPE;
            hb.cmd = CMD_HEARTBEAT;
            hb.sender_id = my_id;
            if (msgsnd(g_msqid, &hb, MSG_SIZE, IPC_NOWAIT) < 0 && errno != EINTR && errno != EAGAIN) {
                g_running = 0;
                break;
            }
        }
    }

    return NULL;
}

static void on_connect_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;

    pthread_mutex_lock(&g_mtx);
    long cur_id = g_my_id;
    pthread_mutex_unlock(&g_mtx);

    if (cur_id > 0) {
        send_logout();
        ui_set_connected_state(0, "", 0);
        ui_append_line("warn", "Вы вышли из чата.", NULL);
        return;
    }

    const char *name = gtk_entry_get_text(GTK_ENTRY(w_nick_entry));
    if (name == NULL || name[0] == '\0' || strchr(name, '#') != NULL || strlen(name) >= MAX_NAME) {
        ui_append_line("err", "Некорректный ник (1..31 символ, без '#').", NULL);
        return;
    }

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = CMD_REGISTER;
    req.reply_type = g_reply_type;
    snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, name);

    if (msgsnd(g_msqid, &req, MSG_SIZE, 0) < 0) {
        ui_append_line("err", "Не удалось отправить запрос арбитру.", NULL);
        return;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep) < 0) {
        ui_append_line("err", "Нет ответа от арбитра.", NULL);
        return;
    }

    if (rep.target_id > 0) {
        pthread_mutex_lock(&g_mtx);
        g_my_id = rep.target_id;
        g_next_msg_id = 1;
        snprintf(g_my_name, sizeof(g_my_name), "%.*s", MAX_NAME - 1, name);
        pthread_mutex_unlock(&g_mtx);

        ui_set_connected_state(1, name, rep.target_id);
        char info[128];
        snprintf(info, sizeof(info), "Подключено как %s (id=%ld).", name, rep.target_id);
        ui_append_line("sys", info, NULL);
    } else {
        char info[MAX_TEXT + 64];
        rep.text[MAX_TEXT - 1] = '\0';
        snprintf(info, sizeof(info), "Ошибка входа: %s", rep.text);
        ui_append_line("err", info, NULL);
    }
}

static void on_send_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;

    pthread_mutex_lock(&g_mtx);
    long cur_id = g_my_id;
    char cur_name[MAX_NAME];
    snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
    pthread_mutex_unlock(&g_mtx);

    if (cur_id <= 0) {
        ui_append_line("err", "Сначала войдите под своим ником.", NULL);
        return;
    }

    const char *target = gtk_entry_get_text(GTK_ENTRY(w_target_entry));
    const char *text = gtk_entry_get_text(GTK_ENTRY(w_msg_entry));

    if (target == NULL || target[0] == '\0' || strlen(target) >= MAX_NAME) {
        ui_append_line("err", "Укажите корректного получателя.", NULL);
        gtk_widget_grab_focus(w_target_entry);
        return;
    }
    if (text == NULL || text[0] == '\0') {
        return;
    }

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = CMD_LOOKUP;
    req.sender_id = cur_id;
    req.reply_type = g_reply_type;
    snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, target);

    if (msgsnd(g_msqid, &req, MSG_SIZE, 0) < 0) {
        ui_append_line("err", "Ошибка отправки запроса арбитру.", NULL);
        return;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep) < 0) {
        ui_append_line("err", "Нет ответа от арбитра.", NULL);
        return;
    }

    long msg_id = g_next_msg_id++;

    if (rep.target_id > 0) {
        struct qmsg chat;
        memset(&chat, 0, sizeof(chat));
        chat.mtype = rep.target_id;
        chat.cmd = CMD_CHAT;
        chat.sender_id = cur_id;
        chat.msg_id = msg_id;
        snprintf(chat.sender_name, sizeof(chat.sender_name), "%.*s", MAX_NAME - 1, cur_name);
        snprintf(chat.text, sizeof(chat.text), "%.*s", MAX_TEXT - 1, text);

        if (msgsnd(g_msqid, &chat, MSG_SIZE, 0) < 0) {
            ui_append_line("err", "Не удалось отправить сообщение.", NULL);
            return;
        }

        char prefix[MAX_NAME + 48];
        snprintf(prefix, sizeof(prefix), "Вы -> %s (#%ld): ", target, msg_id);
        ui_append_line("outgoing", prefix, text);
    } else {
        struct qmsg st;
        memset(&st, 0, sizeof(st));
        st.mtype = ARBITER_TYPE;
        st.cmd = CMD_STORE;
        st.sender_id = cur_id;
        st.msg_id = msg_id;
        snprintf(st.name, sizeof(st.name), "%.*s", MAX_NAME - 1, target);
        snprintf(st.sender_name, sizeof(st.sender_name), "%.*s", MAX_NAME - 1, cur_name);
        snprintf(st.text, sizeof(st.text), "%.*s", MAX_TEXT - 1, text);

        if (msgsnd(g_msqid, &st, MSG_SIZE, 0) < 0) {
            ui_append_line("err", "Не удалось сохранить офлайн-сообщение.", NULL);
            return;
        }

        char prefix[MAX_NAME + 80];
        snprintf(prefix, sizeof(prefix), "Вы -> %s (#%ld, офлайн, TTL %ds): ", target, msg_id, OFFLINE_TTL);
        ui_append_line("warn", prefix, text);
    }

    gtk_entry_set_text(GTK_ENTRY(w_msg_entry), "");
    gtk_widget_grab_focus(w_msg_entry);
}

static void on_window_destroy(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    w_chat_buf = NULL;
    w_chat_view = NULL;
    send_logout();
    g_running = 0;
    gtk_main_quit();
}

static void apply_styles(void) {
    const char *css_data =
        "window { background-color: #1e1e2e; color: #cdd6f4; }"
        "label { color: #cdd6f4; }"
        "entry { background-color: #313244; color: #cdd6f4; border: 1px solid #45475a; border-radius: 6px; padding: 6px 10px; }"
        "entry:focus { border-color: #89b4fa; }"
        "button { background-color: #89b4fa; color: #11111b; font-weight: bold; border-radius: 6px; padding: 6px 14px; border: none; }"
        "button:hover { background-color: #b4befe; }"
        "button:disabled { background-color: #45475a; color: #7f849c; }"
        "textview text { background-color: #181825; color: #cdd6f4; font-family: monospace; font-size: 11pt; }"
        "scrolledwindow { border: 1px solid #45475a; border-radius: 6px; }";

    GtkCssProvider *provider = gtk_css_provider_new();
    gtk_css_provider_load_from_data(provider, css_data, -1, NULL);
    gtk_style_context_add_provider_for_screen(
        gdk_screen_get_default(),
        GTK_STYLE_PROVIDER(provider),
        GTK_STYLE_PROVIDER_PRIORITY_APPLICATION
    );
    g_object_unref(provider);
}

static void build_ui(void) {
    apply_styles();

    w_window = gtk_window_new(GTK_WINDOW_TOPLEVEL);
    gtk_window_set_title(GTK_WINDOW(w_window), "Queue Messenger");
    gtk_window_set_default_size(GTK_WINDOW(w_window), 620, 480);
    g_signal_connect(w_window, "destroy", G_CALLBACK(on_window_destroy), NULL);

    GtkWidget *root = gtk_box_new(GTK_ORIENTATION_VERTICAL, 10);
    gtk_widget_set_margin_top(root, 12);
    gtk_widget_set_margin_bottom(root, 12);
    gtk_widget_set_margin_start(root, 12);
    gtk_widget_set_margin_end(root, 12);
    gtk_container_add(GTK_CONTAINER(w_window), root);

    GtkWidget *top_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    w_nick_entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(w_nick_entry), "Ваш никнейм");
    gtk_entry_set_max_length(GTK_ENTRY(w_nick_entry), MAX_NAME - 1);
    g_signal_connect(w_nick_entry, "activate", G_CALLBACK(on_connect_clicked), NULL);

    w_conn_btn = gtk_button_new_with_label("Войти");
    g_signal_connect(w_conn_btn, "clicked", G_CALLBACK(on_connect_clicked), NULL);

    w_status_lbl = gtk_label_new("Не подключён");
    gtk_widget_set_halign(w_status_lbl, GTK_ALIGN_END);

    gtk_box_pack_start(GTK_BOX(top_bar), w_nick_entry, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(top_bar), w_conn_btn, FALSE, FALSE, 0);
    gtk_box_pack_end(GTK_BOX(top_bar), w_status_lbl, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(root), top_bar, FALSE, FALSE, 0);

    GtkWidget *scroll = gtk_scrolled_window_new(NULL, NULL);
    gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(scroll), GTK_POLICY_AUTOMATIC, GTK_POLICY_AUTOMATIC);

    w_chat_view = gtk_text_view_new();
    gtk_text_view_set_editable(GTK_TEXT_VIEW(w_chat_view), FALSE);
    gtk_text_view_set_cursor_visible(GTK_TEXT_VIEW(w_chat_view), FALSE);
    gtk_text_view_set_wrap_mode(GTK_TEXT_VIEW(w_chat_view), GTK_WRAP_WORD_CHAR);
    gtk_text_view_set_left_margin(GTK_TEXT_VIEW(w_chat_view), 10);
    gtk_text_view_set_right_margin(GTK_TEXT_VIEW(w_chat_view), 10);
    gtk_text_view_set_top_margin(GTK_TEXT_VIEW(w_chat_view), 8);
    gtk_text_view_set_bottom_margin(GTK_TEXT_VIEW(w_chat_view), 8);

    w_chat_buf = gtk_text_view_get_buffer(GTK_TEXT_VIEW(w_chat_view));
    gtk_text_buffer_create_tag(w_chat_buf, "time", "foreground", "#7f849c", NULL);
    gtk_text_buffer_create_tag(w_chat_buf, "incoming", "foreground", "#89dceb", "weight", PANGO_WEIGHT_BOLD, NULL);
    gtk_text_buffer_create_tag(w_chat_buf, "outgoing", "foreground", "#a6e3a1", "weight", PANGO_WEIGHT_BOLD, NULL);
    gtk_text_buffer_create_tag(w_chat_buf, "ack", "foreground", "#89b4fa", NULL);
    gtk_text_buffer_create_tag(w_chat_buf, "warn", "foreground", "#f9e2af", NULL);
    gtk_text_buffer_create_tag(w_chat_buf, "err", "foreground", "#f38ba8", "weight", PANGO_WEIGHT_BOLD, NULL);
    gtk_text_buffer_create_tag(w_chat_buf, "sys", "foreground", "#94e2d5", NULL);

    gtk_container_add(GTK_CONTAINER(scroll), w_chat_view);
    gtk_box_pack_start(GTK_BOX(root), scroll, TRUE, TRUE, 0);

    GtkWidget *bottom_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    w_target_entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(w_target_entry), "Кому");
    gtk_entry_set_max_length(GTK_ENTRY(w_target_entry), MAX_NAME - 1);
    gtk_entry_set_width_chars(GTK_ENTRY(w_target_entry), 14);

    w_msg_entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(w_msg_entry), "Сообщение...");
    gtk_entry_set_max_length(GTK_ENTRY(w_msg_entry), MAX_TEXT - 1);
    g_signal_connect(w_msg_entry, "activate", G_CALLBACK(on_send_clicked), NULL);

    w_send_btn = gtk_button_new_with_label("Отправить");
    g_signal_connect(w_send_btn, "clicked", G_CALLBACK(on_send_clicked), NULL);

    gtk_box_pack_start(GTK_BOX(bottom_bar), w_target_entry, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(bottom_bar), w_msg_entry, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(bottom_bar), w_send_btn, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(root), bottom_bar, FALSE, FALSE, 0);

    ui_set_connected_state(0, "", 0);
    gtk_widget_show_all(w_window);
}

int main(int argc, char *argv[]) {
    gtk_init(&argc, &argv);

    key_t key = ftok(QUEUE_KEY_PATH, QUEUE_KEY_PROJ);
    if (key == (key_t)-1) {
        perror("ftok");
        return 1;
    }

    g_msqid = msgget(key, 0666);
    if (g_msqid < 0) {
        GtkWidget *dlg = gtk_message_dialog_new(
            NULL,
            GTK_DIALOG_MODAL,
            GTK_MESSAGE_ERROR,
            GTK_BUTTONS_OK,
            "Арбитр не запущен.\nСначала запустите './extraq arbiter' в терминале."
        );
        gtk_dialog_run(GTK_DIALOG(dlg));
        gtk_widget_destroy(dlg);
        return 1;
    }

    g_reply_type = REPLY_BASE + (long)getpid();

    build_ui();

    pthread_t t_recv;
    pthread_t t_hb;
    if (pthread_create(&t_recv, NULL, recv_worker, NULL) != 0) {
        perror("pthread_create");
        return 1;
    }
    if (pthread_create(&t_hb, NULL, heartbeat_worker, NULL) != 0) {
        g_running = 0;
        pthread_join(t_recv, NULL);
        perror("pthread_create");
        return 1;
    }

    gtk_main();

    send_logout();
    g_running = 0;
    pthread_join(t_recv, NULL);
    pthread_join(t_hb, NULL);
    return 0;
}