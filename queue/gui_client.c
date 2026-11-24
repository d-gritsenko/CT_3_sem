#define _DEFAULT_SOURCE

#include <errno.h>
#include <gtk/gtk.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "protocol.h"

#define MAX_DIALOGS MAX_CLIENTS
#define MAX_PENDING_ACKS 128

enum ui_event_kind {
    EV_CHAT = 1,
    EV_ACK = 2,
    EV_LOST = 3,
    EV_WHO = 4
};

struct ui_event {
    int kind;
    long msg_id;
    char sender[MAX_NAME];
    char text[MAX_TEXT];
};

struct dialog_entry {
    int used;
    int online;
    int unread;
    char peer[MAX_NAME];
    GtkTextBuffer *buf;
    long pending_acks[MAX_PENDING_ACKS];
    int pending_ack_count;
};

static volatile sig_atomic_t g_running = 1;
static volatile sig_atomic_t g_who_force = 0;
static int g_msqid = -1;
static long g_my_id = 0;
static long g_reply_type = 0;
static long g_who_reply_type = 0;
static long g_next_msg_id = 1;
static char g_my_name[MAX_NAME];
static char g_last_user[MAX_NAME];
static int g_was_connected = 0;
static pthread_mutex_t g_mtx = PTHREAD_MUTEX_INITIALIZER;

static struct dialog_entry g_dialogs[MAX_DIALOGS];
static char g_active_peer[MAX_NAME];
static long g_online_count = 0;
static int g_updating_sidebar = 0;
static int g_sidebar_dirty = 0;

static GtkWidget *w_window = NULL;
static GtkWidget *w_nick_entry = NULL;
static GtkWidget *w_reg_btn = NULL;
static GtkWidget *w_reconn_btn = NULL;
static GtkWidget *w_logout_btn = NULL;
static GtkWidget *w_status_lbl = NULL;
static GtkWidget *w_chat_header = NULL;
static GtkWidget *w_chat_view = NULL;
static GtkTextBuffer *w_sys_buf = NULL;
static GtkWidget *w_users_lbl = NULL;
static GtkWidget *w_users_list = NULL;
static GtkWidget *w_target_entry = NULL;
static GtkWidget *w_msg_entry = NULL;
static GtkWidget *w_send_btn = NULL;

static void get_timestamp(char *buf, size_t size) {
    time_t now = time(NULL);
    struct tm tm_now;
    localtime_r(&now, &tm_now);
    strftime(buf, size, "%H:%M:%S", &tm_now);
}

static void drain_type(int msqid, long mtype) {
    struct qmsg tmp;
    while (msgrcv(msqid, &tmp, MSG_SIZE, mtype, IPC_NOWAIT) >= 0) {
    }
}

static int wait_reply(int msqid, long reply_type, struct qmsg *rep, int max_tries) {
    for (int i = 0; i < max_tries; i++) {
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

static GtkTextBuffer *create_chat_buffer(void) {
    GtkTextBuffer *buf = gtk_text_buffer_new(NULL);
    gtk_text_buffer_create_tag(buf, "time", "foreground", "#7f849c", NULL);
    gtk_text_buffer_create_tag(buf, "incoming", "foreground", "#89dceb", "weight", PANGO_WEIGHT_BOLD, NULL);
    gtk_text_buffer_create_tag(buf, "outgoing", "foreground", "#a6e3a1", "weight", PANGO_WEIGHT_BOLD, NULL);
    gtk_text_buffer_create_tag(buf, "ack", "foreground", "#89b4fa", NULL);
    gtk_text_buffer_create_tag(buf, "warn", "foreground", "#f9e2af", NULL);
    gtk_text_buffer_create_tag(buf, "err", "foreground", "#f38ba8", "weight", PANGO_WEIGHT_BOLD, NULL);
    gtk_text_buffer_create_tag(buf, "sys", "foreground", "#94e2d5", NULL);
    return buf;
}

static struct dialog_entry *find_dialog(const char *peer) {
    if (peer == NULL || peer[0] == '\0') {
        return NULL;
    }
    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (g_dialogs[i].used && strcmp(g_dialogs[i].peer, peer) == 0) {
            return &g_dialogs[i];
        }
    }
    return NULL;
}

static struct dialog_entry *get_or_create_dialog(const char *peer) {
    struct dialog_entry *d = find_dialog(peer);
    if (d != NULL) {
        return d;
    }
    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (!g_dialogs[i].used) {
            g_dialogs[i].used = 1;
            g_dialogs[i].online = 0;
            g_dialogs[i].unread = 0;
            g_dialogs[i].pending_ack_count = 0;
            snprintf(g_dialogs[i].peer, sizeof(g_dialogs[i].peer), "%.*s", MAX_NAME - 1, peer);
            g_dialogs[i].buf = create_chat_buffer();
            return &g_dialogs[i];
        }
    }
    return NULL;
}

static void clear_all_dialogs(void) {
    if (w_chat_view != NULL && w_sys_buf != NULL) {
        gtk_text_view_set_buffer(GTK_TEXT_VIEW(w_chat_view), w_sys_buf);
    }
    g_active_peer[0] = '\0';
    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (g_dialogs[i].used) {
            if (g_dialogs[i].buf != NULL) {
                g_object_unref(g_dialogs[i].buf);
                g_dialogs[i].buf = NULL;
            }
            g_dialogs[i].used = 0;
            g_dialogs[i].online = 0;
            g_dialogs[i].unread = 0;
            g_dialogs[i].pending_ack_count = 0;
            g_dialogs[i].peer[0] = '\0';
        }
    }
}

static GtkTextBuffer *get_active_buffer(void) {
    if (g_active_peer[0] != '\0') {
        struct dialog_entry *d = find_dialog(g_active_peer);
        if (d != NULL && d->buf != NULL) {
            return d->buf;
        }
    }
    return w_sys_buf;
}

static void scroll_buffer_if_active(GtkTextBuffer *buf) {
    if (w_chat_view == NULL || buf == NULL) {
        return;
    }
    if (gtk_text_view_get_buffer(GTK_TEXT_VIEW(w_chat_view)) != buf) {
        return;
    }
    GtkTextIter end;
    GtkTextMark *mark = gtk_text_buffer_get_insert(buf);
    gtk_text_buffer_get_end_iter(buf, &end);
    gtk_text_buffer_move_mark(buf, mark, &end);
    gtk_text_view_scroll_to_mark(GTK_TEXT_VIEW(w_chat_view), mark, 0.0, TRUE, 0.0, 1.0);
}

static void ui_update_chat_header(void) {
    if (w_chat_header == NULL) {
        return;
    }
    if (g_active_peer[0] == '\0') {
        gtk_label_set_text(GTK_LABEL(w_chat_header), "System Log");
        return;
    }
    struct dialog_entry *d = find_dialog(g_active_peer);
    char header[MAX_NAME + 32];
    if (d != NULL && d->online) {
        snprintf(header, sizeof(header), "Chat with %s (online)", g_active_peer);
    } else {
        snprintf(header, sizeof(header), "Chat with %s (offline)", g_active_peer);
    }
    gtk_label_set_text(GTK_LABEL(w_chat_header), header);
}

static void destroy_row_cb(GtkWidget *widget, gpointer data) {
    (void)data;
    gtk_widget_destroy(widget);
}

static void ui_rebuild_sidebar(void) {
    if (w_users_list == NULL || w_users_lbl == NULL) {
        return;
    }

    char cur_name[MAX_NAME];
    pthread_mutex_lock(&g_mtx);
    long cur_id = g_my_id;
    snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
    pthread_mutex_unlock(&g_mtx);

    char header[64];
    snprintf(header, sizeof(header), "Online (%ld)", g_online_count);
    gtk_label_set_text(GTK_LABEL(w_users_lbl), header);

    g_updating_sidebar = 1;
    gtk_container_foreach(GTK_CONTAINER(w_users_list), destroy_row_cb, NULL);

    if (cur_id <= 0) {
        g_updating_sidebar = 0;
        return;
    }

    GtkListBoxRow *row_to_select = NULL;

    GtkWidget *sys_row = gtk_list_box_row_new();
    GtkWidget *sys_lbl = gtk_label_new(NULL);
    gtk_label_set_markup(GTK_LABEL(sys_lbl), "<span foreground='#94e2d5' weight='bold'>★ System</span>");
    gtk_widget_set_halign(sys_lbl, GTK_ALIGN_START);
    gtk_widget_set_margin_top(sys_lbl, 4);
    gtk_widget_set_margin_bottom(sys_lbl, 4);
    gtk_widget_set_margin_start(sys_lbl, 8);
    gtk_widget_set_margin_end(sys_lbl, 8);
    gtk_container_add(GTK_CONTAINER(sys_row), sys_lbl);
    g_object_set_data_full(G_OBJECT(sys_row), "peer", g_strdup(""), g_free);
    gtk_list_box_insert(GTK_LIST_BOX(w_users_list), sys_row, -1);
    if (g_active_peer[0] == '\0') {
        row_to_select = GTK_LIST_BOX_ROW(sys_row);
    }

    for (int pass = 0; pass < 2; pass++) {
        for (int i = 0; i < MAX_DIALOGS; i++) {
            if (!g_dialogs[i].used) {
                continue;
            }
            if ((pass == 0 && !g_dialogs[i].online) || (pass == 1 && g_dialogs[i].online)) {
                continue;
            }
            if (strcmp(g_dialogs[i].peer, cur_name) == 0) {
                continue;
            }

            GtkWidget *row = gtk_list_box_row_new();
            GtkWidget *lbl = gtk_label_new(NULL);
            char *escaped = g_markup_escape_text(g_dialogs[i].peer, -1);
            char markup[MAX_NAME * 4 + 128];

            const char *dot = g_dialogs[i].online
                ? "<span foreground='#a6e3a1'>●</span>"
                : "<span foreground='#7f849c'>○</span>";

            if (g_dialogs[i].unread > 0) {
                snprintf(markup, sizeof(markup),
                         "%s <span weight='bold'>%s</span> <span foreground='#f9e2af' weight='bold'>(%d)</span>",
                         dot, escaped, g_dialogs[i].unread);
            } else {
                snprintf(markup, sizeof(markup), "%s %s", dot, escaped);
            }

            g_free(escaped);
            gtk_label_set_markup(GTK_LABEL(lbl), markup);
            gtk_widget_set_halign(lbl, GTK_ALIGN_START);
            gtk_widget_set_margin_top(lbl, 4);
            gtk_widget_set_margin_bottom(lbl, 4);
            gtk_widget_set_margin_start(lbl, 8);
            gtk_widget_set_margin_end(lbl, 8);

            gtk_container_add(GTK_CONTAINER(row), lbl);
            g_object_set_data_full(G_OBJECT(row), "peer", g_strdup(g_dialogs[i].peer), g_free);
            gtk_list_box_insert(GTK_LIST_BOX(w_users_list), row, -1);

            if (g_active_peer[0] != '\0' && strcmp(g_dialogs[i].peer, g_active_peer) == 0) {
                row_to_select = GTK_LIST_BOX_ROW(row);
            }
        }
    }

    gtk_widget_show_all(w_users_list);
    if (row_to_select != NULL) {
        gtk_list_box_select_row(GTK_LIST_BOX(w_users_list), row_to_select);
    }
    g_updating_sidebar = 0;
}

static gboolean ui_rebuild_sidebar_idle(gpointer data) {
    (void)data;
    g_sidebar_dirty = 0;
    if (g_running) {
        ui_rebuild_sidebar();
    }
    return G_SOURCE_REMOVE;
}

static void ui_schedule_sidebar_rebuild(void) {
    if (!g_sidebar_dirty) {
        g_sidebar_dirty = 1;
        g_idle_add(ui_rebuild_sidebar_idle, NULL);
    }
}

static void send_read_ack(const char *peer, long msg_id) {
    if (peer == NULL || peer[0] == '\0' || msg_id <= 0 || g_msqid < 0) {
        return;
    }

    pthread_mutex_lock(&g_mtx);
    long my_id = g_my_id;
    char cur_name[MAX_NAME];
    snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
    pthread_mutex_unlock(&g_mtx);

    if (my_id <= 0) {
        return;
    }

    struct qmsg ack;
    memset(&ack, 0, sizeof(ack));
    ack.mtype = ARBITER_TYPE;
    ack.cmd = CMD_ACK;
    ack.sender_id = my_id;
    ack.msg_id = msg_id;
    snprintf(ack.name, sizeof(ack.name), "%.*s", MAX_NAME - 1, peer);
    snprintf(ack.sender_name, sizeof(ack.sender_name), "%.*s", MAX_NAME - 1, cur_name);
    msgsnd(g_msqid, &ack, MSG_SIZE, IPC_NOWAIT);
}

static void flush_dialog_acks(struct dialog_entry *d) {
    if (d == NULL) {
        return;
    }
    for (int i = 0; i < d->pending_ack_count; i++) {
        send_read_ack(d->peer, d->pending_acks[i]);
    }
    d->pending_ack_count = 0;
}

static void ui_switch_dialog(const char *peer, int focus_msg) {
    if (w_chat_view == NULL || w_sys_buf == NULL) {
        return;
    }

    if (peer == NULL || peer[0] == '\0') {
        g_active_peer[0] = '\0';
        gtk_text_view_set_buffer(GTK_TEXT_VIEW(w_chat_view), w_sys_buf);
        ui_update_chat_header();
        scroll_buffer_if_active(w_sys_buf);
        ui_schedule_sidebar_rebuild();
        return;
    }

    struct dialog_entry *d = get_or_create_dialog(peer);
    if (d == NULL) {
        return;
    }

    d->unread = 0;
    flush_dialog_acks(d);
    snprintf(g_active_peer, sizeof(g_active_peer), "%.*s", MAX_NAME - 1, d->peer);
    gtk_text_view_set_buffer(GTK_TEXT_VIEW(w_chat_view), d->buf);
    gtk_entry_set_text(GTK_ENTRY(w_target_entry), d->peer);
    ui_update_chat_header();
    scroll_buffer_if_active(d->buf);
    ui_schedule_sidebar_rebuild();

    if (focus_msg && w_msg_entry != NULL) {
        gtk_widget_grab_focus(w_msg_entry);
    }
}

static void ui_update_online_users(long count, const char *list_text) {
    g_online_count = count;

    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (g_dialogs[i].used) {
            g_dialogs[i].online = 0;
        }
    }

    char cur_name[MAX_NAME];
    pthread_mutex_lock(&g_mtx);
    snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
    pthread_mutex_unlock(&g_mtx);

    if (list_text != NULL && list_text[0] != '\0') {
        char copy[MAX_TEXT];
        snprintf(copy, sizeof(copy), "%.*s", MAX_TEXT - 1, list_text);

        char *saveptr = NULL;
        char *tok = strtok_r(copy, ",", &saveptr);
        while (tok != NULL) {
            while (*tok == ' ') {
                tok++;
            }
            if (*tok != '\0' && strcmp(tok, cur_name) != 0) {
                struct dialog_entry *d = get_or_create_dialog(tok);
                if (d != NULL) {
                    d->online = 1;
                }
            }
            tok = strtok_r(NULL, ",", &saveptr);
        }
    }

    ui_update_chat_header();
    ui_schedule_sidebar_rebuild();
}

static void on_user_row_selected(GtkListBox *box, GtkListBoxRow *row, gpointer data) {
    (void)box;
    (void)data;
    if (g_updating_sidebar || row == NULL) {
        return;
    }

    const char *peer = g_object_get_data(G_OBJECT(row), "peer");
    if (peer == NULL) {
        return;
    }

    ui_switch_dialog(peer, peer[0] != '\0');
}

static void ui_append_line_to(GtkTextBuffer *buf, const char *tag_name, const char *prefix, const char *body) {
    if (buf == NULL) {
        return;
    }

    char ts[16];
    get_timestamp(ts, sizeof(ts));

    GtkTextIter end;
    gtk_text_buffer_get_end_iter(buf, &end);

    char ts_str[24];
    snprintf(ts_str, sizeof(ts_str), "[%s] ", ts);
    gtk_text_buffer_insert_with_tags_by_name(buf, &end, ts_str, -1, "time", NULL);

    if (prefix != NULL && prefix[0] != '\0') {
        gtk_text_buffer_insert_with_tags_by_name(buf, &end, prefix, -1, tag_name, NULL);
    }

    if (body != NULL && body[0] != '\0') {
        gtk_text_buffer_insert(buf, &end, body, -1);
    }

    gtk_text_buffer_insert(buf, &end, "\n", -1);
    scroll_buffer_if_active(buf);
}

static void ui_append_sys(const char *tag_name, const char *text) {
    ui_append_line_to(w_sys_buf, tag_name, text, NULL);
}

static void ui_append_error(const char *text) {
    ui_append_line_to(get_active_buffer(), "err", text, NULL);
}

static void ui_append_outgoing_to(GtkTextBuffer *buf, long msg_id, const char *body) {
    if (buf == NULL) {
        return;
    }

    char ts[16];
    get_timestamp(ts, sizeof(ts));

    GtkTextIter end;
    gtk_text_buffer_get_end_iter(buf, &end);

    char ts_str[24];
    snprintf(ts_str, sizeof(ts_str), "[%s] ", ts);
    gtk_text_buffer_insert_with_tags_by_name(buf, &end, ts_str, -1, "time", NULL);
    gtk_text_buffer_insert_with_tags_by_name(buf, &end, "you: ", -1, "outgoing", NULL);

    if (body != NULL && body[0] != '\0') {
        gtk_text_buffer_insert(buf, &end, body, -1);
    }

    char m_start[32];
    char m_end[32];
    snprintf(m_start, sizeof(m_start), "ms_%ld", msg_id);
    snprintf(m_end, sizeof(m_end), "me_%ld", msg_id);

    gtk_text_buffer_get_end_iter(buf, &end);
    gtk_text_buffer_create_mark(buf, m_start, &end, TRUE);
    gtk_text_buffer_insert_with_tags_by_name(buf, &end, " ✓", -1, "time", NULL);
    gtk_text_buffer_get_end_iter(buf, &end);
    gtk_text_buffer_create_mark(buf, m_end, &end, TRUE);
    gtk_text_buffer_insert(buf, &end, "\n", -1);
    scroll_buffer_if_active(buf);
}

static int mark_delivered_in_buf(GtkTextBuffer *buf, long msg_id) {
    if (buf == NULL) {
        return 0;
    }

    char m_start[32];
    char m_end[32];
    snprintf(m_start, sizeof(m_start), "ms_%ld", msg_id);
    snprintf(m_end, sizeof(m_end), "me_%ld", msg_id);

    GtkTextMark *ms = gtk_text_buffer_get_mark(buf, m_start);
    GtkTextMark *me = gtk_text_buffer_get_mark(buf, m_end);
    if (ms == NULL || me == NULL) {
        return 0;
    }

    GtkTextIter s;
    GtkTextIter e;
    gtk_text_buffer_get_iter_at_mark(buf, &s, ms);
    gtk_text_buffer_get_iter_at_mark(buf, &e, me);
    gtk_text_buffer_delete(buf, &s, &e);
    gtk_text_buffer_insert_with_tags_by_name(buf, &s, " ✓✓", -1, "ack", NULL);
    gtk_text_buffer_delete_mark(buf, ms);
    gtk_text_buffer_delete_mark(buf, me);
    return 1;
}

static void ui_mark_delivered(const char *peer, long msg_id) {
    struct dialog_entry *d = find_dialog(peer);
    if (d != NULL && mark_delivered_in_buf(d->buf, msg_id)) {
        return;
    }
    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (g_dialogs[i].used && mark_delivered_in_buf(g_dialogs[i].buf, msg_id)) {
            return;
        }
    }
}

static void ui_set_connected_state(int connected, const char *name, long id) {
    gtk_widget_set_sensitive(w_nick_entry, !connected);
    gtk_widget_set_visible(w_reg_btn, !connected);
    gtk_widget_set_visible(w_reconn_btn, !connected);
    gtk_widget_set_visible(w_logout_btn, connected);
    gtk_widget_set_sensitive(w_target_entry, connected);
    gtk_widget_set_sensitive(w_msg_entry, connected);
    gtk_widget_set_sensitive(w_send_btn, connected);
    gtk_widget_set_sensitive(w_users_list, connected);

    if (connected) {
        char status[128];
        char title[128];
        snprintf(status, sizeof(status), "Online: %s (id=%ld)", name, id);
        snprintf(title, sizeof(title), "Queue Messenger — %s", name);
        gtk_label_set_text(GTK_LABEL(w_status_lbl), status);
        gtk_window_set_title(GTK_WINDOW(w_window), title);
        ui_update_chat_header();
        ui_schedule_sidebar_rebuild();
        gtk_widget_grab_focus(w_target_entry);
    } else {
        g_online_count = 0;
        g_active_peer[0] = '\0';
        if (w_chat_view != NULL && w_sys_buf != NULL) {
            gtk_text_view_set_buffer(GTK_TEXT_VIEW(w_chat_view), w_sys_buf);
        }
        gtk_label_set_text(GTK_LABEL(w_status_lbl), "Disconnected");
        gtk_window_set_title(GTK_WINDOW(w_window), "Queue Messenger");
        ui_update_chat_header();
        ui_schedule_sidebar_rebuild();
        gtk_widget_grab_focus(w_nick_entry);
    }
}

static gboolean ui_handle_event(gpointer user_data) {
    struct ui_event *ev = (struct ui_event *)user_data;

    if (!g_running || w_chat_view == NULL || w_sys_buf == NULL) {
        free(ev);
        return G_SOURCE_REMOVE;
    }

    if (ev->kind == EV_CHAT) {
        struct dialog_entry *d = get_or_create_dialog(ev->sender);
        if (d != NULL) {
            char prefix[MAX_NAME + 8];
            snprintf(prefix, sizeof(prefix), "%s: ", ev->sender);
            ui_append_line_to(d->buf, "incoming", prefix, ev->text);
            if (strcmp(g_active_peer, d->peer) == 0) {
                send_read_ack(d->peer, ev->msg_id);
            } else {
                d->unread++;
                if (ev->msg_id > 0 && d->pending_ack_count < MAX_PENDING_ACKS) {
                    d->pending_acks[d->pending_ack_count++] = ev->msg_id;
                }
                ui_schedule_sidebar_rebuild();
            }
        }
    } else if (ev->kind == EV_ACK) {
        ui_mark_delivered(ev->sender, ev->msg_id);
    } else if (ev->kind == EV_WHO) {
        ui_update_online_users(ev->msg_id, ev->text);
    } else if (ev->kind == EV_LOST) {
        pthread_mutex_lock(&g_mtx);
        g_my_id = 0;
        g_my_name[0] = '\0';
        pthread_mutex_unlock(&g_mtx);
        ui_set_connected_state(0, "", 0);
        gtk_widget_set_sensitive(w_reg_btn, FALSE);
        gtk_widget_set_sensitive(w_reconn_btn, FALSE);
        gtk_widget_set_sensitive(w_logout_btn, FALSE);
        gtk_widget_set_sensitive(w_nick_entry, FALSE);
        ui_append_sys("err", "Connection lost (arbiter stopped).");
    }

    free(ev);
    return G_SOURCE_REMOVE;
}

static void *recv_worker(void *arg) {
    (void)arg;
    struct qmsg msg;

    while (g_running) {
        pthread_mutex_lock(&g_mtx);
        long my_id = g_my_id;
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

                struct ui_event *ev = calloc(1, sizeof(*ev));
                if (ev != NULL) {
                    ev->kind = EV_CHAT;
                    ev->msg_id = msg.msg_id;
                    copy_utf8_safe(ev->sender, sizeof(ev->sender), msg.sender_name);
                    copy_utf8_safe(ev->text, sizeof(ev->text), msg.text);
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
    int hb_ticks = 0;
    int who_ticks = 0;
    long last_count = -1;
    char last_list[MAX_TEXT];
    last_list[0] = '\0';

    while (g_running) {
        usleep(100000);
        hb_ticks++;
        who_ticks++;

        pthread_mutex_lock(&g_mtx);
        long my_id = g_my_id;
        pthread_mutex_unlock(&g_mtx);

        if (my_id <= 0) {
            last_count = -1;
            last_list[0] = '\0';
            continue;
        }

        if (hb_ticks >= HEARTBEAT_INTERVAL * 10) {
            hb_ticks = 0;
            memset(&hb, 0, sizeof(hb));
            hb.mtype = ARBITER_TYPE;
            hb.cmd = CMD_HEARTBEAT;
            hb.sender_id = my_id;
            if (msgsnd(g_msqid, &hb, MSG_SIZE, IPC_NOWAIT) < 0 && errno != EINTR && errno != EAGAIN) {
                g_running = 0;
                break;
            }
        }

        if (g_who_force || who_ticks >= 20) {
            int forced = g_who_force;
            g_who_force = 0;
            who_ticks = 0;

            drain_type(g_msqid, g_who_reply_type);

            struct qmsg req;
            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = CMD_WHO;
            req.sender_id = my_id;
            req.reply_type = g_who_reply_type;

            if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) == 0) {
                struct qmsg rep;
                if (wait_reply(g_msqid, g_who_reply_type, &rep, 50) == 0) {
                    rep.text[MAX_TEXT - 1] = '\0';
                    if (forced || rep.target_id != last_count || strcmp(rep.text, last_list) != 0) {
                        last_count = rep.target_id;
                        snprintf(last_list, sizeof(last_list), "%.*s", MAX_TEXT - 1, rep.text);

                        struct ui_event *ev = calloc(1, sizeof(*ev));
                        if (ev != NULL) {
                            ev->kind = EV_WHO;
                            ev->msg_id = rep.target_id;
                            snprintf(ev->text, sizeof(ev->text), "%.*s", MAX_TEXT - 1, rep.text);
                            g_idle_add(ui_handle_event, ev);
                        }
                    }
                }
            }
        }
    }

    drain_type(g_msqid, g_who_reply_type);
    return NULL;
}

static void do_auth(int is_reconnect) {
    pthread_mutex_lock(&g_mtx);
    long cur_id = g_my_id;
    pthread_mutex_unlock(&g_mtx);

    if (cur_id > 0) {
        return;
    }

    const char *name = gtk_entry_get_text(GTK_ENTRY(w_nick_entry));
    if (name == NULL || name[0] == '\0' || strchr(name, '#') != NULL || strchr(name, ',') != NULL || strlen(name) >= MAX_NAME) {
        ui_append_sys("err", "Invalid name (1..31 chars, no '#' or ',').");
        return;
    }

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = is_reconnect ? CMD_RECONNECT : CMD_REGISTER;
    req.reply_type = g_reply_type;
    snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, name);

    if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) < 0) {
        ui_append_sys("err", errno == EAGAIN
            ? "Message queue is full, try again."
            : "Failed to send request to arbiter.");
        return;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
        ui_append_sys("err", "No response from arbiter.");
        return;
    }

    if (rep.target_id > 0) {
        int user_changed = (g_last_user[0] != '\0' && strcmp(g_last_user, name) != 0);
        if (user_changed) {
            clear_all_dialogs();
            g_next_msg_id = 1;
        }
        snprintf(g_last_user, sizeof(g_last_user), "%.*s", MAX_NAME - 1, name);

        pthread_mutex_lock(&g_mtx);
        g_my_id = rep.target_id;
        snprintf(g_my_name, sizeof(g_my_name), "%.*s", MAX_NAME - 1, name);
        pthread_mutex_unlock(&g_mtx);

        g_was_connected = 1;
        ui_set_connected_state(1, name, rep.target_id);
        g_who_force = 1;

        char info[128];
        snprintf(info, sizeof(info), "%s as %s (id=%ld).",
                 is_reconnect ? "Reconnected" : "Registered", name, rep.target_id);
        ui_append_sys("sys", info);
    } else {
        char info[MAX_TEXT + 64];
        rep.text[MAX_TEXT - 1] = '\0';
        snprintf(info, sizeof(info), "%s failed: %s",
                 is_reconnect ? "Reconnect" : "Registration", rep.text);
        ui_append_sys("err", info);
    }
}

static void on_register_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    do_auth(0);
}

static void on_reconnect_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    do_auth(1);
}

static void on_logout_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    send_logout();
    ui_set_connected_state(0, "", 0);
    ui_append_sys("warn", "Logged out.");
}

static void on_nick_activate(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    do_auth(g_was_connected);
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
        ui_append_sys("err", "Register first.");
        return;
    }

    const char *target = gtk_entry_get_text(GTK_ENTRY(w_target_entry));
    const char *text = gtk_entry_get_text(GTK_ENTRY(w_msg_entry));

    if (target == NULL || target[0] == '\0' || strlen(target) >= MAX_NAME) {
        ui_append_error("Enter a valid recipient.");
        gtk_widget_grab_focus(w_target_entry);
        return;
    }
    if (text == NULL || text[0] == '\0') {
        return;
    }
    if (strcmp(target, cur_name) == 0) {
        ui_append_error("Cannot send message to yourself.");
        return;
    }
    if (strcasecmp(target, "arbiter") == 0) {
        ui_append_error("Cannot send message to arbiter.");
        return;
    }

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = CMD_LOOKUP;
    req.sender_id = cur_id;
    req.reply_type = g_reply_type;
    snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, target);

    if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) < 0) {
        ui_append_error(errno == EAGAIN
            ? "Message queue is full, try again."
            : "Failed to send request to arbiter.");
        return;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
        ui_append_error("No response from arbiter.");
        return;
    }

    struct dialog_entry *d = get_or_create_dialog(target);
    if (d == NULL) {
        ui_append_error("Too many open dialogs.");
        return;
    }
    d->online = (rep.target_id > 0);

    if (strcmp(g_active_peer, d->peer) != 0) {
        ui_switch_dialog(d->peer, 0);
    } else {
        ui_update_chat_header();
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
        copy_utf8_safe(chat.text, sizeof(chat.text), text);

        if (msgsnd(g_msqid, &chat, MSG_SIZE, IPC_NOWAIT) < 0) {
            ui_append_error(errno == EAGAIN
                ? "Message queue is full, try again."
                : "Failed to send message.");
            return;
        }

        ui_append_outgoing_to(d->buf, msg_id, text);
    } else {
        struct qmsg st;
        memset(&st, 0, sizeof(st));
        st.mtype = ARBITER_TYPE;
        st.cmd = CMD_STORE;
        st.sender_id = cur_id;
        st.msg_id = msg_id;
        snprintf(st.name, sizeof(st.name), "%.*s", MAX_NAME - 1, target);
        snprintf(st.sender_name, sizeof(st.sender_name), "%.*s", MAX_NAME - 1, cur_name);
        copy_utf8_safe(st.text, sizeof(st.text), text);

        if (msgsnd(g_msqid, &st, MSG_SIZE, IPC_NOWAIT) < 0) {
            ui_append_error(errno == EAGAIN
                ? "Message queue is full, try again."
                : "Failed to store offline message.");
            return;
        }

        ui_append_outgoing_to(d->buf, msg_id, text);
    }

    gtk_entry_set_text(GTK_ENTRY(w_msg_entry), "");
    gtk_widget_grab_focus(w_msg_entry);
}

static void on_window_destroy(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    w_chat_view = NULL;
    w_chat_header = NULL;
    w_users_list = NULL;
    w_users_lbl = NULL;
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
        "scrolledwindow { border: 1px solid #45475a; border-radius: 6px; }"
        "list { background-color: #181825; color: #cdd6f4; }"
        "list row { padding: 2px 4px; }"
        "list row:hover { background-color: #313244; }"
        "list row:selected { background-color: #45475a; color: #ffffff; }";

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
    gtk_window_set_default_size(GTK_WINDOW(w_window), 780, 500);
    g_signal_connect(w_window, "destroy", G_CALLBACK(on_window_destroy), NULL);

    GtkWidget *root = gtk_box_new(GTK_ORIENTATION_VERTICAL, 10);
    gtk_widget_set_margin_top(root, 12);
    gtk_widget_set_margin_bottom(root, 12);
    gtk_widget_set_margin_start(root, 12);
    gtk_widget_set_margin_end(root, 12);
    gtk_container_add(GTK_CONTAINER(w_window), root);

    GtkWidget *top_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    w_nick_entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(w_nick_entry), "Username");
    gtk_entry_set_max_length(GTK_ENTRY(w_nick_entry), MAX_NAME - 1);
    g_signal_connect(w_nick_entry, "activate", G_CALLBACK(on_nick_activate), NULL);

    w_reg_btn = gtk_button_new_with_label("Register");
    g_signal_connect(w_reg_btn, "clicked", G_CALLBACK(on_register_clicked), NULL);

    w_reconn_btn = gtk_button_new_with_label("Reconnect");
    g_signal_connect(w_reconn_btn, "clicked", G_CALLBACK(on_reconnect_clicked), NULL);

    w_logout_btn = gtk_button_new_with_label("Logout");
    g_signal_connect(w_logout_btn, "clicked", G_CALLBACK(on_logout_clicked), NULL);

    w_status_lbl = gtk_label_new("Disconnected");
    gtk_widget_set_halign(w_status_lbl, GTK_ALIGN_END);

    gtk_box_pack_start(GTK_BOX(top_bar), w_nick_entry, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(top_bar), w_reg_btn, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(top_bar), w_reconn_btn, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(top_bar), w_logout_btn, FALSE, FALSE, 0);
    gtk_box_pack_end(GTK_BOX(top_bar), w_status_lbl, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(root), top_bar, FALSE, FALSE, 0);

    GtkWidget *mid_box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 10);

    GtkWidget *chat_col = gtk_box_new(GTK_ORIENTATION_VERTICAL, 6);
    w_chat_header = gtk_label_new("System Log");
    gtk_widget_set_halign(w_chat_header, GTK_ALIGN_START);
    gtk_box_pack_start(GTK_BOX(chat_col), w_chat_header, FALSE, FALSE, 0);

    GtkWidget *scroll = gtk_scrolled_window_new(NULL, NULL);
    gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(scroll), GTK_POLICY_AUTOMATIC, GTK_POLICY_AUTOMATIC);

    w_sys_buf = create_chat_buffer();
    w_chat_view = gtk_text_view_new_with_buffer(w_sys_buf);
    gtk_text_view_set_editable(GTK_TEXT_VIEW(w_chat_view), FALSE);
    gtk_text_view_set_cursor_visible(GTK_TEXT_VIEW(w_chat_view), FALSE);
    gtk_text_view_set_wrap_mode(GTK_TEXT_VIEW(w_chat_view), GTK_WRAP_WORD_CHAR);
    gtk_text_view_set_left_margin(GTK_TEXT_VIEW(w_chat_view), 10);
    gtk_text_view_set_right_margin(GTK_TEXT_VIEW(w_chat_view), 10);
    gtk_text_view_set_top_margin(GTK_TEXT_VIEW(w_chat_view), 8);
    gtk_text_view_set_bottom_margin(GTK_TEXT_VIEW(w_chat_view), 8);

    gtk_container_add(GTK_CONTAINER(scroll), w_chat_view);
    gtk_box_pack_start(GTK_BOX(chat_col), scroll, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(mid_box), chat_col, TRUE, TRUE, 0);

    GtkWidget *sidebar = gtk_box_new(GTK_ORIENTATION_VERTICAL, 6);
    gtk_widget_set_size_request(sidebar, 190, -1);

    w_users_lbl = gtk_label_new("Online (0)");
    gtk_widget_set_halign(w_users_lbl, GTK_ALIGN_START);
    gtk_box_pack_start(GTK_BOX(sidebar), w_users_lbl, FALSE, FALSE, 0);

    GtkWidget *users_scroll = gtk_scrolled_window_new(NULL, NULL);
    gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(users_scroll), GTK_POLICY_AUTOMATIC, GTK_POLICY_AUTOMATIC);

    w_users_list = gtk_list_box_new();
    gtk_list_box_set_selection_mode(GTK_LIST_BOX(w_users_list), GTK_SELECTION_SINGLE);
    g_signal_connect(w_users_list, "row-selected", G_CALLBACK(on_user_row_selected), NULL);

    gtk_container_add(GTK_CONTAINER(users_scroll), w_users_list);
    gtk_box_pack_start(GTK_BOX(sidebar), users_scroll, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(mid_box), sidebar, FALSE, FALSE, 0);

    gtk_box_pack_start(GTK_BOX(root), mid_box, TRUE, TRUE, 0);

    GtkWidget *bottom_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    w_target_entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(w_target_entry), "To");
    gtk_entry_set_max_length(GTK_ENTRY(w_target_entry), MAX_NAME - 1);
    gtk_entry_set_width_chars(GTK_ENTRY(w_target_entry), 14);

    w_msg_entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(w_msg_entry), "Message...");
    gtk_entry_set_max_length(GTK_ENTRY(w_msg_entry), MAX_TEXT - 1);
    g_signal_connect(w_msg_entry, "activate", G_CALLBACK(on_send_clicked), NULL);

    w_send_btn = gtk_button_new_with_label("Send");
    g_signal_connect(w_send_btn, "clicked", G_CALLBACK(on_send_clicked), NULL);

    gtk_box_pack_start(GTK_BOX(bottom_bar), w_target_entry, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(bottom_bar), w_msg_entry, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(bottom_bar), w_send_btn, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(root), bottom_bar, FALSE, FALSE, 0);

    gtk_widget_show_all(w_window);
    ui_set_connected_state(0, "", 0);
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
            "Arbiter is not running.\nStart './extraq arbiter' in a terminal first."
        );
        gtk_dialog_run(GTK_DIALOG(dlg));
        gtk_widget_destroy(dlg);
        return 1;
    }

    g_reply_type = REPLY_BASE + (long)getpid() * 2L;
    g_who_reply_type = g_reply_type + 1L;

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
    clear_all_dialogs();
    if (w_sys_buf != NULL) {
        g_object_unref(w_sys_buf);
        w_sys_buf = NULL;
    }
    return 0;
}