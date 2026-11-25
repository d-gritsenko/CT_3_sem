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

#define MAX_DIALOGS (MAX_CLIENTS + MAX_ROOMS + 8)
#define MAX_PENDING_ACKS 128

enum ui_event_kind {
    EV_CHAT = 1,
    EV_ACK = 2,
    EV_LOST = 3,
    EV_WHO = 4,
    EV_ROOM_MSG = 5,
    EV_ROOMS = 6,
    EV_HISTORY_ITEM = 7
};

struct ui_event {
    int kind;
    int is_room;
    int is_outgoing;
    long msg_id;
    long timestamp;
    char room[MAX_NAME];
    char sender[MAX_NAME];
    char text[MAX_TEXT];
};

struct pending_ack {
    long msg_id;
    char sender[MAX_NAME];
};

struct dialog_entry {
    int used;
    int is_room;
    int online;
    int joined;
    int member_count;
    int unread;
    char peer[MAX_NAME];
    GtkTextBuffer *buf;
    struct pending_ack pending_acks[MAX_PENDING_ACKS];

    int pending_ack_count;
    long unacked_ids[MAX_PENDING_ACKS];
    int unacked_count;
};

struct room_list_item {
    char name[MAX_NAME];
    int joined;
    int members;
};

struct ui_theme {
    const char *name;
    const char *win_bg;
    const char *fg;
    const char *entry_bg;
    const char *border;
    const char *accent;
    const char *accent_hover;
    const char *btn_fg;
    const char *disabled_bg;
    const char *muted;
    const char *chat_bg;
    const char *row_hover;
    const char *row_sel_bg;
    const char *row_sel_fg;
    const char *leave_bg;
    const char *leave_hover;
    const char *incoming;
    const char *outgoing;
    const char *ack;
    const char *warn;
    const char *err;
    const char *sys;
    const char *online;
    const char *unread;
};

static const struct ui_theme g_themes[] = {
    {
        "Catppuccin Mocha",
        "#1e1e2e", "#cdd6f4", "#313244", "#45475a",
        "#89b4fa", "#b4befe", "#11111b", "#313244",
        "#7f849c", "#181825", "#313244", "#45475a", "#ffffff",
        "#f38ba8", "#eba0ac",
        "#89dceb", "#a6e3a1", "#89b4fa", "#f9e2af", "#f38ba8", "#94e2d5",
        "#a6e3a1", "#f9e2af"
    },
    {
        "Nord Dark",
        "#2e3440", "#eceff4", "#3b4252", "#4c566a",
        "#88c0d0", "#8fbcbb", "#2e3440", "#3b4252",
        "#7b88a1", "#242933", "#3b4252", "#4c566a", "#ffffff",
        "#bf616a", "#d08770",
        "#88c0d0", "#a3be8c", "#81a1c1", "#ebcb8b", "#bf616a", "#8fbcbb",
        "#a3be8c", "#ebcb8b"
    },
    {
        "Gruvbox Dark",
        "#282828", "#ebdbb2", "#3c3836", "#504945",
        "#fabd2f", "#fe8019", "#1d2021", "#3c3836",
        "#928374", "#1d2021", "#3c3836", "#504945", "#fbf1c7",
        "#fb4934", "#cc241d",
        "#83a598", "#b8bb26", "#8ec07c", "#fabd2f", "#fb4934", "#fe8019",
        "#b8bb26", "#fabd2f"
    },
    {
        "Tokyo Night",
        "#1a1b26", "#c0caf5", "#24283b", "#414868",
        "#7aa2f7", "#bb9af7", "#15161e", "#24283b",
        "#787c99", "#16161e", "#24283b", "#414868", "#ffffff",
        "#f7768e", "#ff9e64",
        "#7dcfff", "#9ece6a", "#7aa2f7", "#e0af68", "#f7768e", "#2ac3de",
        "#9ece6a", "#e0af68"
    },
    {
        "Cyber Dark",
        "#222426", "#f8f8f2", "#2e3236", "#494d52",
        "#a6e22e", "#66d9ef", "#1b1d1e", "#2e3236",
        "#75715e", "#1a1c1d", "#2e3236", "#494d52", "#ffffff",
        "#f92672", "#fd971f",
        "#66d9ef", "#a6e22e", "#ae81ff", "#e6db74", "#f92672", "#a6e22e",
        "#a6e22e", "#e6db74"
    }
};

#define THEME_COUNT ((int)(sizeof(g_themes) / sizeof(g_themes[0])))

static volatile sig_atomic_t g_running = 1;
static volatile sig_atomic_t g_who_force = 0;
static int g_msqid = -1;
static long g_my_id = 0;
static int g_is_debug = 0;
static long g_reply_type = 0;
static long g_who_reply_type = 0;
static long g_next_msg_id = 1;
static char g_my_name[MAX_NAME];
static pthread_mutex_t g_mtx = PTHREAD_MUTEX_INITIALIZER;

static int g_suppress_select = 0;
static int g_suppress_theme_save = 0;
static int g_theme_idx = 0;
static GtkCssProvider *g_css_provider = NULL;

static int g_font_size = 11;

static GtkTextBuffer *g_sys_buf = NULL;
static struct dialog_entry g_dialogs[MAX_DIALOGS];
static char g_active_peer[MAX_NAME] = "";
static int g_active_is_room = 0;

static char g_online_users[MAX_CLIENTS][MAX_NAME];
static int g_online_count = 0;

static struct room_list_item g_rooms_cache[MAX_ROOMS];
static int g_rooms_cache_count = 0;

static GtkWidget *w_window = NULL;
static GtkWidget *w_main_stack = NULL;

static GtkWidget *w_auth_user = NULL;
static GtkWidget *w_auth_pass = NULL;
static GtkWidget *w_login_btn = NULL;
static GtkWidget *w_reg_btn = NULL;
static GtkWidget *w_auth_err = NULL;

static GtkWidget *w_logout_btn = NULL;
static GtkWidget *w_status_lbl = NULL;
static GtkWidget *w_theme_combo = NULL;
static GtkWidget *w_chat_header = NULL;
static GtkWidget *w_leave_btn = NULL;
static GtkWidget *w_delete_btn = NULL;
static GtkWidget *w_chat_view = NULL;

static GtkWidget *w_tab_users = NULL;
static GtkWidget *w_tab_rooms = NULL;
static GtkWidget *w_sidebar_stack = NULL;

static GtkWidget *w_sidebar = NULL;

static GtkWidget *w_users_lbl = NULL;
static GtkWidget *w_users_list = NULL;

static GtkWidget *w_room_entry = NULL;
static GtkWidget *w_join_btn = NULL;
static GtkWidget *w_rooms_lbl = NULL;
static GtkWidget *w_rooms_list = NULL;

static GtkWidget *w_target_entry = NULL;
static GtkWidget *w_msg_entry = NULL;
static GtkWidget *w_send_btn = NULL;

static void ui_rebuild_users_list(void);
static void ui_rebuild_rooms_list(void);
static void ui_update_tab_labels(void);
static void ui_update_chat_header(void);
static void ui_switch_dialog(const char *peer, int is_room, int set_target);

static const struct ui_theme *current_theme(void) {
    if (g_theme_idx < 0 || g_theme_idx >= THEME_COUNT) {
        return &g_themes[0];
    }
    return &g_themes[g_theme_idx];
}

static void get_timestamp(char *buf, size_t size) {
    time_t now = time(NULL);
    struct tm tm_now;
    localtime_r(&now, &tm_now);
    strftime(buf, size, "%H:%M:%S", &tm_now);
}

static void format_timestamp_from(time_t ts, char *buf, size_t size) {
    time_t now = time(NULL);
    if (ts <= 0) {
        ts = now;
    }
    struct tm tm_ts;
    struct tm tm_now;
    localtime_r(&ts, &tm_ts);
    localtime_r(&now, &tm_now);
    if (tm_ts.tm_year == tm_now.tm_year && tm_ts.tm_yday == tm_now.tm_yday) {
        strftime(buf, size, "%H:%M:%S", &tm_ts);
    } else {
        strftime(buf, size, "%d.%m %H:%M", &tm_ts);
    }
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
    g_is_debug = 0;
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

static void send_read_ack(long msg_id, const char *sender_name) {
    if (msg_id <= 0 || sender_name == NULL || sender_name[0] == '\0' || g_msqid < 0) {
        return;
    }

    char cur_name[MAX_NAME];
    pthread_mutex_lock(&g_mtx);
    long my_id = g_my_id;
    snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
    pthread_mutex_unlock(&g_mtx);

    if (my_id <= 0 || cur_name[0] == '\0') {
        return;
    }

    struct qmsg ack;
    memset(&ack, 0, sizeof(ack));
    ack.mtype = ARBITER_TYPE;
    ack.cmd = CMD_ACK;
    ack.sender_id = my_id;
    ack.msg_id = msg_id;
    snprintf(ack.name, sizeof(ack.name), "%.*s", MAX_NAME - 1, sender_name);
    snprintf(ack.sender_name, sizeof(ack.sender_name), "%.*s", MAX_NAME - 1, cur_name);
    msgsnd(g_msqid, &ack, MSG_SIZE, IPC_NOWAIT);
}

static void send_mark_read(const char *peer, int is_room) {
    if (peer == NULL || peer[0] == '\0' || g_msqid < 0) {
        return;
    }

    pthread_mutex_lock(&g_mtx);
    long my_id = g_my_id;
    int is_dbg = g_is_debug;
    pthread_mutex_unlock(&g_mtx);

    if (my_id <= 0 || is_dbg) {
        return;
    }

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = CMD_MARK_READ;
    req.sender_id = my_id;
    req.target_id = is_room ? 1 : 0;
    snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, peer);
    msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT);
}


static void apply_buffer_theme(GtkTextBuffer *buf) {
    if (buf == NULL) {
        return;
    }
    const struct ui_theme *t = current_theme();
    GtkTextTagTable *table = gtk_text_buffer_get_tag_table(buf);
    GtkTextTag *tag;

    if ((tag = gtk_text_tag_table_lookup(table, "time")) != NULL) {
        g_object_set(tag, "foreground", t->muted, NULL);
    }
    if ((tag = gtk_text_tag_table_lookup(table, "incoming")) != NULL) {
        g_object_set(tag, "foreground", t->incoming, NULL);
    }
    if ((tag = gtk_text_tag_table_lookup(table, "outgoing")) != NULL) {
        g_object_set(tag, "foreground", t->outgoing, NULL);
    }
    if ((tag = gtk_text_tag_table_lookup(table, "ack")) != NULL) {
        g_object_set(tag, "foreground", t->ack, NULL);
    }
    if ((tag = gtk_text_tag_table_lookup(table, "warn")) != NULL) {
        g_object_set(tag, "foreground", t->warn, NULL);
    }
    if ((tag = gtk_text_tag_table_lookup(table, "err")) != NULL) {
        g_object_set(tag, "foreground", t->err, NULL);
    }
    if ((tag = gtk_text_tag_table_lookup(table, "sys")) != NULL) {
        g_object_set(tag, "foreground", t->sys, NULL);
    }
}

static GtkTextBuffer *create_chat_buffer(void) {
    const struct ui_theme *t = current_theme();
    GtkTextBuffer *buf = gtk_text_buffer_new(NULL);
    gtk_text_buffer_create_tag(buf, "time", "foreground", t->muted, NULL);
    gtk_text_buffer_create_tag(buf, "incoming", "foreground", t->incoming, "weight", PANGO_WEIGHT_BOLD, NULL);
    gtk_text_buffer_create_tag(buf, "outgoing", "foreground", t->outgoing, "weight", PANGO_WEIGHT_BOLD, NULL);
    gtk_text_buffer_create_tag(buf, "ack", "foreground", t->ack, NULL);
    gtk_text_buffer_create_tag(buf, "warn", "foreground", t->warn, NULL);
    gtk_text_buffer_create_tag(buf, "err", "foreground", t->err, "weight", PANGO_WEIGHT_BOLD, NULL);
    gtk_text_buffer_create_tag(buf, "sys", "foreground", t->sys, NULL);
    return buf;
}


static void apply_css(void) {
    const struct ui_theme *t = current_theme();
    int chat_pt = g_font_size;
    int ui_pt = (chat_pt > 10) ? (chat_pt - 1) : 9;
    int title_pt = chat_pt + 7;
    int hint_pt = (chat_pt > 10) ? (chat_pt - 2) : 8;

    if (g_css_provider == NULL) {
        g_css_provider = gtk_css_provider_new();
        gtk_style_context_add_provider_for_screen(
            gdk_screen_get_default(),
            GTK_STYLE_PROVIDER(g_css_provider),
            GTK_STYLE_PROVIDER_PRIORITY_APPLICATION
        );
    }

    char css_data[4096];
    snprintf(css_data, sizeof(css_data),
        "window { background-color: %s; color: %s; font-size: %dpt; }\n"
        "label { color: %s; font-size: %dpt; }\n"
        ".auth-card { background-color: %s; border: 1px solid %s; border-radius: 10px; padding: 24px 28px; }\n"
        ".auth-title { font-size: %dpt; font-weight: bold; color: %s; }\n"
        ".auth-hint { font-size: %dpt; color: %s; }\n"
        ".auth-err { font-weight: bold; color: %s; }\n"
        "entry { background-color: %s; color: %s; border: 1px solid %s; border-radius: 6px; padding: 6px 10px; font-size: %dpt; }\n"
        "entry:focus { border-color: %s; }\n"
        "button { background-image: none; background-color: %s; border-radius: 6px; padding: 6px 14px; border: none; font-size: %dpt; }\n"
        "button, button label { color: %s; font-weight: bold; }\n"
        "button:hover { background-image: none; background-color: %s; }\n"
        "button:disabled { background-image: none; background-color: %s; }\n"
        "button:disabled, button:disabled label { color: %s; }\n"
        "button.tab-btn { background-image: none; background-color: %s; border-radius: 6px; padding: 5px 8px; border: 1px solid %s; }\n"
        "button.tab-btn, button.tab-btn label { color: %s; font-weight: bold; }\n"
        "button.tab-btn:hover { background-image: none; background-color: %s; }\n"
        "button.tab-active { background-image: none; background-color: %s; border: 1px solid %s; }\n"
        "button.tab-active, button.tab-active label { color: %s; }\n"
        "button.tab-active:hover { background-image: none; background-color: %s; }\n"
        "button.leave-btn { background-image: none; background-color: %s; padding: 3px 10px; }\n"
        "button.leave-btn, button.leave-btn label { color: %s; }\n"
        "button.leave-btn:hover { background-image: none; background-color: %s; }\n"
        "combobox button { background-image: none; background-color: %s; border: 1px solid %s; border-radius: 6px; padding: 4px 10px; }\n"
        "combobox button, combobox button label, combobox button cellview { color: %s; font-weight: normal; }\n"
        "combobox button:hover { background-image: none; background-color: %s; }\n"
        "textview text { background-color: %s; color: %s; font-family: monospace; font-size: %dpt; }\n"
        "scrolledwindow { border: 1px solid %s; border-radius: 6px; }\n"
        "list { background-color: %s; color: %s; font-size: %dpt; }\n"
        "list row { padding: 2px 4px; }\n"
        "list row:hover { background-color: %s; }\n"
        "list row:selected { background-color: %s; color: %s; }\n",
        t->win_bg, t->fg, ui_pt,
        t->fg, ui_pt,
        t->chat_bg, t->border,
        title_pt, t->accent,
        hint_pt, t->muted,
        t->err,
        t->entry_bg, t->fg, t->border, ui_pt,
        t->accent,
        t->accent, ui_pt,
        t->btn_fg,
        t->accent_hover,
        t->disabled_bg,
        t->muted,
        t->entry_bg, t->border,
        t->fg,
        t->row_hover,
        t->accent, t->accent,
        t->btn_fg,
        t->accent_hover,
        t->leave_bg,
        t->btn_fg,
        t->leave_hover,
        t->entry_bg, t->border,
        t->fg,
        t->row_hover,
        t->chat_bg, t->fg, chat_pt,
        t->border,
        t->chat_bg, t->fg, ui_pt,
        t->row_hover,
        t->row_sel_bg, t->row_sel_fg
    );

    gtk_css_provider_load_from_data(g_css_provider, css_data, -1, NULL);
}

static void apply_theme(int idx) {
    if (idx < 0 || idx >= THEME_COUNT) {
        idx = 0;
    }
    g_theme_idx = idx;

    apply_css();

    apply_buffer_theme(g_sys_buf);
    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (g_dialogs[i].used && g_dialogs[i].buf != NULL) {
            apply_buffer_theme(g_dialogs[i].buf);
        }
    }

    ui_rebuild_users_list();
    ui_rebuild_rooms_list();
}

static void on_theme_changed(GtkComboBox *combo, gpointer data) {
    (void)data;
    int idx = gtk_combo_box_get_active(combo);
    if (idx < 0 || idx >= THEME_COUNT) {
        return;
    }
    apply_theme(idx);

    if (g_suppress_theme_save) {
        return;
    }

    pthread_mutex_lock(&g_mtx);
    long my_id = g_my_id;
    int is_dbg = g_is_debug;
    pthread_mutex_unlock(&g_mtx);

    if (my_id > 0 && !is_dbg && g_msqid >= 0) {
        struct qmsg req;
        memset(&req, 0, sizeof(req));
        req.mtype = ARBITER_TYPE;
        req.cmd = CMD_SET_THEME;
        req.sender_id = my_id;
        req.msg_id = idx;
        msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT);
    }
}

static int is_user_online(const char *name) {
    for (int i = 0; i < g_online_count; i++) {
        if (strcmp(g_online_users[i], name) == 0) {
            return 1;
        }
    }
    return 0;
}

static struct dialog_entry *find_dialog(const char *peer, int is_room) {
    if (peer == NULL || peer[0] == '\0') {
        return NULL;
    }
    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (g_dialogs[i].used && g_dialogs[i].is_room == is_room && strcmp(g_dialogs[i].peer, peer) == 0) {
            return &g_dialogs[i];
        }
    }
    return NULL;
}

static struct dialog_entry *get_or_create_dialog(const char *peer, int is_room) {
    struct dialog_entry *d = find_dialog(peer, is_room);
    if (d != NULL) {
        return d;
    }
    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (!g_dialogs[i].used) {
            memset(&g_dialogs[i], 0, sizeof(g_dialogs[i]));
            g_dialogs[i].used = 1;
            g_dialogs[i].is_room = is_room;
            g_dialogs[i].online = is_room ? 0 : is_user_online(peer);
            snprintf(g_dialogs[i].peer, sizeof(g_dialogs[i].peer), "%.*s", MAX_NAME - 1, peer);
            g_dialogs[i].buf = create_chat_buffer();
            return &g_dialogs[i];
        }
    }
    return NULL;
}

static void remove_dialog(const char *peer, int is_room) {
    if (peer == NULL || peer[0] == '\0') {
        return;
    }
    if (g_active_is_room == is_room && strcmp(g_active_peer, peer) == 0) {
        ui_switch_dialog("", 0, 0);
    }
    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (g_dialogs[i].used && g_dialogs[i].is_room == is_room && strcmp(g_dialogs[i].peer, peer) == 0) {
            if (g_dialogs[i].buf != NULL) {
                g_object_unref(g_dialogs[i].buf);
            }
            memset(&g_dialogs[i], 0, sizeof(g_dialogs[i]));
            break;
        }
    }
}

static void flush_dialog_acks(struct dialog_entry *d) {
    if (d == NULL) {
        return;
    }
    for (int i = 0; i < d->pending_ack_count; i++) {
        send_read_ack(d->pending_acks[i].msg_id, d->pending_acks[i].sender);
    }
    d->pending_ack_count = 0;
}

static void queue_dialog_ack(struct dialog_entry *d, long msg_id, const char *sender) {
    if (d == NULL || msg_id <= 0 || sender == NULL || sender[0] == '\0') {
        return;
    }
    if (d->pending_ack_count < MAX_PENDING_ACKS) {
        d->pending_acks[d->pending_ack_count].msg_id = msg_id;
        snprintf(d->pending_acks[d->pending_ack_count].sender, MAX_NAME, "%.*s", MAX_NAME - 1, sender);
        d->pending_ack_count++;
    }
}

static void ui_scroll_if_active(GtkTextBuffer *buf) {
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

static void ui_append_line_ts(GtkTextBuffer *buf, time_t ts_val, const char *tag_name, const char *prefix, const char *body) {
    if (buf == NULL) {
        return;
    }

    char ts[24];
    if (ts_val > 0) {
        format_timestamp_from(ts_val, ts, sizeof(ts));
    } else {
        get_timestamp(ts, sizeof(ts));
    }

    GtkTextIter end;
    gtk_text_buffer_get_end_iter(buf, &end);

    char ts_str[32];
    snprintf(ts_str, sizeof(ts_str), "[%s] ", ts);
    gtk_text_buffer_insert_with_tags_by_name(buf, &end, ts_str, -1, "time", NULL);

    if (prefix != NULL && prefix[0] != '\0') {
        gtk_text_buffer_insert_with_tags_by_name(buf, &end, prefix, -1, tag_name, NULL);
    }

    if (body != NULL && body[0] != '\0') {
        gtk_text_buffer_insert(buf, &end, body, -1);
    }

    gtk_text_buffer_insert(buf, &end, "\n", -1);
    ui_scroll_if_active(buf);
}

static void ui_append_line_to(GtkTextBuffer *buf, const char *tag_name, const char *prefix, const char *body) {
    ui_append_line_ts(buf, 0, tag_name, prefix, body);
}

static void ui_append_sys(const char *tag_name, const char *text) {
    ui_append_line_to(g_sys_buf, tag_name, text, NULL);
}

static void ui_append_error(const char *text) {
    GtkTextBuffer *cur = (w_chat_view != NULL) ? gtk_text_view_get_buffer(GTK_TEXT_VIEW(w_chat_view)) : g_sys_buf;
    ui_append_line_to(cur != NULL ? cur : g_sys_buf, "err", text, NULL);
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

    ui_scroll_if_active(buf);
}


static void record_unacked_id(struct dialog_entry *d, long msg_id) {
    if (d == NULL || msg_id <= 0) {
        return;
    }
    if (d->unacked_count >= MAX_PENDING_ACKS) {
        memmove(&d->unacked_ids[0], &d->unacked_ids[1], sizeof(long) * (MAX_PENDING_ACKS - 1));
        d->unacked_count = MAX_PENDING_ACKS - 1;
    }
    d->unacked_ids[d->unacked_count++] = msg_id;
}

static void ui_mark_delivered(long msg_id) {
    char m_start[32];
    char m_end[32];
    snprintf(m_start, sizeof(m_start), "ms_%ld", msg_id);
    snprintf(m_end, sizeof(m_end), "me_%ld", msg_id);

    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (!g_dialogs[i].used || g_dialogs[i].buf == NULL) {
            continue;
        }
        GtkTextMark *ms = gtk_text_buffer_get_mark(g_dialogs[i].buf, m_start);
        GtkTextMark *me = gtk_text_buffer_get_mark(g_dialogs[i].buf, m_end);
        if (ms != NULL && me != NULL) {
            GtkTextIter s;
            GtkTextIter e;
            gtk_text_buffer_get_iter_at_mark(g_dialogs[i].buf, &s, ms);
            gtk_text_buffer_get_iter_at_mark(g_dialogs[i].buf, &e, me);
            gtk_text_buffer_delete(g_dialogs[i].buf, &s, &e);
            gtk_text_buffer_insert_with_tags_by_name(g_dialogs[i].buf, &s, " ✓✓", -1, "ack", NULL);
            gtk_text_buffer_delete_mark(g_dialogs[i].buf, ms);
            gtk_text_buffer_delete_mark(g_dialogs[i].buf, me);
            for (int j = 0; j < g_dialogs[i].unacked_count; j++) {
                if (g_dialogs[i].unacked_ids[j] == msg_id) {
                    for (int k = j; k < g_dialogs[i].unacked_count - 1; k++) {
                        g_dialogs[i].unacked_ids[k] = g_dialogs[i].unacked_ids[k + 1];
                    }
                    g_dialogs[i].unacked_count--;
                    break;
                }
            }
            return;
        }
    }
}

static void ui_mark_all_delivered(const char *peer) {
    struct dialog_entry *d = find_dialog(peer, 0);
    if (d == NULL) {
        return;
    }
    while (d->unacked_count > 0) {
        long mid = d->unacked_ids[0];
        int prev_count = d->unacked_count;
        ui_mark_delivered(mid);
        if (d->unacked_count == prev_count) {
            for (int k = 0; k < d->unacked_count - 1; k++) {
                d->unacked_ids[k] = d->unacked_ids[k + 1];
            }
            d->unacked_count--;
        }
    }
}
static void ui_update_chat_header(void) {
    if (w_chat_header == NULL) {
        return;
    }

    if (g_active_peer[0] == '\0') {
        gtk_label_set_text(GTK_LABEL(w_chat_header), "★ System");
        if (w_leave_btn != NULL) {
            gtk_widget_set_visible(w_leave_btn, FALSE);
        }
        if (w_delete_btn != NULL) {
            gtk_widget_set_visible(w_delete_btn, FALSE);
        }
        return;
    }

    char header[MAX_NAME + 64];
    if (g_active_is_room) {
        struct dialog_entry *d = find_dialog(g_active_peer, 1);
        int members = (d != NULL && d->member_count > 0) ? d->member_count : 1;
        snprintf(header, sizeof(header), "#%s (%d %s)", g_active_peer, members, members == 1 ? "member" : "members");
        gtk_label_set_text(GTK_LABEL(w_chat_header), header);
        if (w_leave_btn != NULL) {
            gtk_widget_set_visible(w_leave_btn, (d != NULL && d->joined) ? TRUE : FALSE);
        }
        if (w_delete_btn != NULL) {
            gtk_button_set_label(GTK_BUTTON(w_delete_btn), "Delete Room");
            gtk_widget_set_visible(w_delete_btn, TRUE);
        }
    } else {
        int online = is_user_online(g_active_peer);
        snprintf(header, sizeof(header), "Chat with %s (%s)", g_active_peer, online ? "online" : "offline");
        gtk_label_set_text(GTK_LABEL(w_chat_header), header);
        if (w_leave_btn != NULL) {
            gtk_widget_set_visible(w_leave_btn, FALSE);
        }
        if (w_delete_btn != NULL) {
            gtk_button_set_label(GTK_BUTTON(w_delete_btn), "Delete Chat");
            gtk_widget_set_visible(w_delete_btn, TRUE);
        }
    }
}

static void ui_update_tab_labels(void) {
    if (w_tab_users == NULL || w_tab_rooms == NULL) {
        return;
    }

    int unread_users = 0;
    int unread_rooms = 0;
    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (!g_dialogs[i].used || g_dialogs[i].unread <= 0) {
            continue;
        }
        if (g_dialogs[i].is_room) {
            unread_rooms += g_dialogs[i].unread;
        } else {
            unread_users += g_dialogs[i].unread;
        }
    }

    char u_lbl[32];
    char r_lbl[32];
    if (unread_users > 0) {
        snprintf(u_lbl, sizeof(u_lbl), "Users (%d)", unread_users);
    } else {
        snprintf(u_lbl, sizeof(u_lbl), "Users");
    }
    if (unread_rooms > 0) {
        snprintf(r_lbl, sizeof(r_lbl), "Rooms (%d)", unread_rooms);
    } else {
        snprintf(r_lbl, sizeof(r_lbl), "Rooms");
    }

    gtk_button_set_label(GTK_BUTTON(w_tab_users), u_lbl);
    gtk_button_set_label(GTK_BUTTON(w_tab_rooms), r_lbl);
}

static void ui_select_sidebar_tab(const char *tab_name) {
    if (w_sidebar_stack == NULL || w_tab_users == NULL || w_tab_rooms == NULL) {
        return;
    }
    gtk_stack_set_visible_child_name(GTK_STACK(w_sidebar_stack), tab_name);

    GtkStyleContext *ctx_u = gtk_widget_get_style_context(w_tab_users);
    GtkStyleContext *ctx_r = gtk_widget_get_style_context(w_tab_rooms);
    if (strcmp(tab_name, "rooms") == 0) {
        gtk_style_context_remove_class(ctx_u, "tab-active");
        gtk_style_context_add_class(ctx_r, "tab-active");
    } else {
        gtk_style_context_add_class(ctx_u, "tab-active");
        gtk_style_context_remove_class(ctx_r, "tab-active");
    }
}

static int g_sidebar_rebuild_pending = 0;

static gboolean ui_deferred_sidebar_rebuild(gpointer data) {
    (void)data;
    g_sidebar_rebuild_pending = 0;
    if (!g_running) {
        return G_SOURCE_REMOVE;
    }
    ui_rebuild_users_list();
    ui_rebuild_rooms_list();
    ui_update_tab_labels();
    return G_SOURCE_REMOVE;
}

static void ui_schedule_sidebar_rebuild(void) {
    if (!g_sidebar_rebuild_pending) {
        g_sidebar_rebuild_pending = 1;
        g_idle_add(ui_deferred_sidebar_rebuild, NULL);
    }
}

static void ui_switch_dialog(const char *peer, int is_room, int set_target) {
    if (w_chat_view == NULL) {
        return;
    }

    if (peer == NULL || peer[0] == '\0') {
        g_active_peer[0] = '\0';
        g_active_is_room = 0;
        gtk_text_view_set_buffer(GTK_TEXT_VIEW(w_chat_view), g_sys_buf);
        ui_scroll_if_active(g_sys_buf);
        ui_update_chat_header();
        return;
    }

    struct dialog_entry *d = get_or_create_dialog(peer, is_room);
    if (d == NULL) {
        return;
    }

    snprintf(g_active_peer, sizeof(g_active_peer), "%.*s", MAX_NAME - 1, d->peer);
    g_active_is_room = is_room;
    d->unread = 0;
    flush_dialog_acks(d);

    send_mark_read(d->peer, is_room);

    gtk_text_view_set_buffer(GTK_TEXT_VIEW(w_chat_view), d->buf);
    ui_scroll_if_active(d->buf);
    ui_update_chat_header();

    if (set_target && w_target_entry != NULL) {
        if (is_room) {
            char room_target[MAX_NAME + 2];
            snprintf(room_target, sizeof(room_target), "#%s", d->peer);
            gtk_entry_set_text(GTK_ENTRY(w_target_entry), room_target);
        } else {
            gtk_entry_set_text(GTK_ENTRY(w_target_entry), d->peer);
        }
    }

    ui_schedule_sidebar_rebuild();
}

static void destroy_row_cb(GtkWidget *widget, gpointer data) {
    (void)data;
    gtk_widget_destroy(widget);
}

static GtkWidget *make_sidebar_row(const char *markup_text, const char *key, const char *value) {
    GtkWidget *row = gtk_list_box_row_new();
    GtkWidget *lbl = gtk_label_new(NULL);
    gtk_label_set_markup(GTK_LABEL(lbl), markup_text);
    gtk_widget_set_halign(lbl, GTK_ALIGN_START);
    gtk_widget_set_margin_top(lbl, 4);
    gtk_widget_set_margin_bottom(lbl, 4);
    gtk_widget_set_margin_start(lbl, 8);
    gtk_widget_set_margin_end(lbl, 8);
    gtk_container_add(GTK_CONTAINER(row), lbl);
    g_object_set_data_full(G_OBJECT(row), key, g_strdup(value), g_free);
    return row;
}

static void ui_rebuild_users_list(void) {
    if (w_users_list == NULL || w_users_lbl == NULL) {
        return;
    }

    const struct ui_theme *t = current_theme();

    char cur_name[MAX_NAME];
    pthread_mutex_lock(&g_mtx);
    snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
    pthread_mutex_unlock(&g_mtx);

    int other_online = 0;
    for (int i = 0; i < g_online_count; i++) {
        if (cur_name[0] != '\0' && strcmp(g_online_users[i], cur_name) == 0) {
            continue;
        }
        other_online++;
    }

    char header[64];
    snprintf(header, sizeof(header), "Online (%d)", other_online);
    gtk_label_set_text(GTK_LABEL(w_users_lbl), header);

    g_suppress_select = 1;
    gtk_container_foreach(GTK_CONTAINER(w_users_list), destroy_row_cb, NULL);

    char sys_markup[128];
    snprintf(sys_markup, sizeof(sys_markup), "<span foreground='%s'>★</span> System", t->sys);
    GtkWidget *sys_row = make_sidebar_row(sys_markup, "username", "");
    gtk_list_box_insert(GTK_LIST_BOX(w_users_list), sys_row, -1);
    GtkListBoxRow *row_to_select = (!g_active_is_room && g_active_peer[0] == '\0') ? GTK_LIST_BOX_ROW(sys_row) : NULL;

    for (int i = 0; i < g_online_count; i++) {
        const char *uname = g_online_users[i];
        if (cur_name[0] != '\0' && strcmp(uname, cur_name) == 0) {
            continue;
        }
        struct dialog_entry *d = find_dialog(uname, 0);
        char *esc = g_markup_escape_text(uname, -1);
        char label_text[MAX_NAME * 6 + 160];

        if (d != NULL && d->unread > 0) {
            snprintf(label_text, sizeof(label_text),
                     "<span foreground='%s'>●</span> <span foreground='%s' weight='bold'>%s (%d)</span>",
                     t->online, t->unread, esc, d->unread);
        } else {
            snprintf(label_text, sizeof(label_text),
                     "<span foreground='%s'>●</span> %s", t->online, esc);
        }
        g_free(esc);

        GtkWidget *row = make_sidebar_row(label_text, "username", uname);
        gtk_list_box_insert(GTK_LIST_BOX(w_users_list), row, -1);
        if (!g_active_is_room && strcmp(g_active_peer, uname) == 0) {
            row_to_select = GTK_LIST_BOX_ROW(row);
        }
    }

    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (!g_dialogs[i].used || g_dialogs[i].is_room) {
            continue;
        }
        if (is_user_online(g_dialogs[i].peer)) {
            continue;
        }
        char *esc = g_markup_escape_text(g_dialogs[i].peer, -1);
        char label_text[MAX_NAME * 6 + 160];
        if (g_dialogs[i].unread > 0) {
            snprintf(label_text, sizeof(label_text),
                     "<span foreground='%s'>○</span> <span foreground='%s' weight='bold'>%s (%d)</span>",
                     t->muted, t->unread, esc, g_dialogs[i].unread);
        } else {
            snprintf(label_text, sizeof(label_text),
                     "<span foreground='%s'>○ %s</span>", t->muted, esc);
        }
        g_free(esc);

        GtkWidget *row = make_sidebar_row(label_text, "username", g_dialogs[i].peer);
        gtk_list_box_insert(GTK_LIST_BOX(w_users_list), row, -1);
        if (!g_active_is_room && strcmp(g_active_peer, g_dialogs[i].peer) == 0) {
            row_to_select = GTK_LIST_BOX_ROW(row);
        }
    }

    gtk_widget_show_all(w_users_list);
    if (row_to_select != NULL) {
        gtk_list_box_select_row(GTK_LIST_BOX(w_users_list), row_to_select);
    } else {
        gtk_list_box_unselect_all(GTK_LIST_BOX(w_users_list));
    }
    g_suppress_select = 0;
}

static void ui_rebuild_rooms_list(void) {
    if (w_rooms_list == NULL || w_rooms_lbl == NULL) {
        return;
    }

    const struct ui_theme *t = current_theme();

    char header[64];
    snprintf(header, sizeof(header), "Active rooms (%d)", g_rooms_cache_count);
    gtk_label_set_text(GTK_LABEL(w_rooms_lbl), header);

    g_suppress_select = 1;
    gtk_container_foreach(GTK_CONTAINER(w_rooms_list), destroy_row_cb, NULL);

    GtkListBoxRow *row_to_select = NULL;

    for (int i = 0; i < g_rooms_cache_count; i++) {
        const char *rname = g_rooms_cache[i].name;
        int joined = g_rooms_cache[i].joined;
        int members = g_rooms_cache[i].members;
        struct dialog_entry *d = find_dialog(rname, 1);
        char *esc = g_markup_escape_text(rname, -1);
        char label_text[MAX_NAME * 6 + 160];

        if (joined) {
            if (d != NULL && d->unread > 0) {
                snprintf(label_text, sizeof(label_text),
                         "<span foreground='%s' weight='bold'>#%s (%d) [%d]</span>",
                         t->unread, esc, members, d->unread);
            } else {
                snprintf(label_text, sizeof(label_text),
                         "<span foreground='%s'>#</span>%s <span foreground='%s'>(%d)</span>",
                         t->online, esc, t->muted, members);
            }
        } else {
            snprintf(label_text, sizeof(label_text),
                     "<span foreground='%s'>+ #%s (%d)</span>", t->muted, esc, members);
        }
        g_free(esc);

        GtkWidget *row = make_sidebar_row(label_text, "roomname", rname);
        g_object_set_data(G_OBJECT(row), "joined", GINT_TO_POINTER(joined));
        gtk_list_box_insert(GTK_LIST_BOX(w_rooms_list), row, -1);

        if (g_active_is_room && strcmp(g_active_peer, rname) == 0) {
            row_to_select = GTK_LIST_BOX_ROW(row);
        }
    }

    gtk_widget_show_all(w_rooms_list);
    if (row_to_select != NULL) {
        gtk_list_box_select_row(GTK_LIST_BOX(w_rooms_list), row_to_select);
    } else {
        gtk_list_box_unselect_all(GTK_LIST_BOX(w_rooms_list));
    }
    g_suppress_select = 0;
}

static void ui_update_users_list(long count, const char *list_text) {
    (void)count;
    g_online_count = 0;

    if (list_text != NULL && list_text[0] != '\0') {
        char copy[MAX_TEXT];
        snprintf(copy, sizeof(copy), "%.*s", MAX_TEXT - 1, list_text);
        char *saveptr = NULL;
        char *tok = strtok_r(copy, ",", &saveptr);
        while (tok != NULL && g_online_count < MAX_CLIENTS) {
            while (*tok == ' ') {
                tok++;
            }
            if (*tok != '\0') {
                snprintf(g_online_users[g_online_count], MAX_NAME, "%.*s", MAX_NAME - 1, tok);
                g_online_count++;
            }
            tok = strtok_r(NULL, ",", &saveptr);
        }
    }

    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (g_dialogs[i].used && !g_dialogs[i].is_room) {
            g_dialogs[i].online = is_user_online(g_dialogs[i].peer);
        }
    }

    ui_rebuild_users_list();
    ui_update_chat_header();
    ui_update_tab_labels();
}

static void ui_update_rooms_list(long count, const char *list_text) {
    (void)count;
    g_rooms_cache_count = 0;

    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (g_dialogs[i].used && g_dialogs[i].is_room) {
            g_dialogs[i].joined = 0;
            g_dialogs[i].member_count = 0;
        }
    }

    if (list_text != NULL && list_text[0] != '\0') {
        char copy[MAX_TEXT];
        snprintf(copy, sizeof(copy), "%.*s", MAX_TEXT - 1, list_text);
        char *saveptr = NULL;
        char *tok = strtok_r(copy, ",", &saveptr);
        while (tok != NULL && g_rooms_cache_count < MAX_ROOMS) {
            while (*tok == ' ') {
                tok++;
            }
            if (*tok != '\0') {
                int joined = 0;
                if (*tok == '*') {
                    joined = 1;
                    tok++;
                }
                int members = 0;
                char *lp = strrchr(tok, '(');
                if (lp != NULL) {
                    members = atoi(lp + 1);
                    *lp = '\0';
                }
                if (*tok != '\0') {
                    struct room_list_item *item = &g_rooms_cache[g_rooms_cache_count++];
                    snprintf(item->name, sizeof(item->name), "%.*s", MAX_NAME - 1, tok);
                    item->joined = joined;
                    item->members = members;

                    struct dialog_entry *d = find_dialog(item->name, 1);
                    if (d != NULL) {
                        d->joined = joined;
                        d->member_count = members;
                    }
                }
            }
            tok = strtok_r(NULL, ",", &saveptr);
        }
    }

    ui_rebuild_rooms_list();
    ui_update_chat_header();
    ui_update_tab_labels();
}

static void clear_all_dialogs(void) {
    ui_switch_dialog("", 0, 0);
    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (g_dialogs[i].used) {
            if (g_dialogs[i].buf != NULL) {
                g_object_unref(g_dialogs[i].buf);
            }
            memset(&g_dialogs[i], 0, sizeof(g_dialogs[i]));
        }
    }
    if (g_sys_buf != NULL) {
        gtk_text_buffer_set_text(g_sys_buf, "", -1);
    }
}

static int do_join_room(const char *raw_name) {
    pthread_mutex_lock(&g_mtx);
    long cur_id = g_my_id;
    pthread_mutex_unlock(&g_mtx);

    if (cur_id <= 0) {
        ui_append_error("Login first.");
        return 0;
    }

    if (raw_name == NULL) {
        return 0;
    }
    while (*raw_name == ' ' || *raw_name == '#') {
        raw_name++;
    }
    if (raw_name[0] == '\0' || strchr(raw_name, '#') != NULL ||
        strchr(raw_name, ',') != NULL || strchr(raw_name, ' ') != NULL ||
        strlen(raw_name) >= MAX_NAME - 1) {
        ui_append_error("Invalid room name (no spaces, '#' or ',').");
        return 0;
    }

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = CMD_ROOM_JOIN;
    req.sender_id = cur_id;
    req.reply_type = g_reply_type;
    snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 2, raw_name);

    if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) < 0) {
        ui_append_error(errno == EAGAIN
            ? "Message queue is full, try again."
            : "Failed to send room join request.");
        return 0;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
        ui_append_error("No response from arbiter.");
        return 0;
    }
    rep.text[MAX_TEXT - 1] = '\0';

    if (rep.target_id > 0) {
        struct dialog_entry *d = get_or_create_dialog(req.name, 1);
        if (d != NULL) {
            d->joined = 1;
            d->member_count = (int)rep.target_id;
            char note[MAX_TEXT + MAX_NAME + 16];
            snprintf(note, sizeof(note), "Room #%s: %s.", req.name, rep.text);
            ui_append_sys("sys", note);
        }
        g_who_force = 1;
        ui_switch_dialog(req.name, 1, 1);
        ui_select_sidebar_tab("rooms");
        if (w_msg_entry != NULL) {
            gtk_widget_grab_focus(w_msg_entry);
        }
        return 1;
    }

    char err[MAX_TEXT + 32];
    snprintf(err, sizeof(err), "Room error: %s", rep.text);
    ui_append_error(err);
    return 0;
}

static void on_join_room_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    if (w_room_entry == NULL) {
        return;
    }
    const char *rname = gtk_entry_get_text(GTK_ENTRY(w_room_entry));
    if (do_join_room(rname)) {
        gtk_entry_set_text(GTK_ENTRY(w_room_entry), "");
    }
}

static void on_leave_room_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    if (!g_active_is_room || g_active_peer[0] == '\0') {
        return;
    }

    pthread_mutex_lock(&g_mtx);
    long cur_id = g_my_id;
    pthread_mutex_unlock(&g_mtx);

    if (cur_id <= 0) {
        return;
    }

    char rname[MAX_NAME];
    snprintf(rname, sizeof(rname), "%.*s", MAX_NAME - 1, g_active_peer);

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = CMD_ROOM_LEAVE;
    req.sender_id = cur_id;
    req.reply_type = g_reply_type;
    snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 2, rname);

    if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) < 0) {
        ui_append_error(errno == EAGAIN
            ? "Message queue is full, try again."
            : "Failed to leave room.");
        return;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
        ui_append_error("No response from arbiter.");
        return;
    }
    rep.text[MAX_TEXT - 1] = '\0';

    if (rep.target_id > 0) {
        struct dialog_entry *d = find_dialog(rname, 1);
        if (d != NULL) {
            d->joined = 0;
            d->unread = 0;
            d->pending_ack_count = 0;
        }
        char note[MAX_NAME + 32];
        snprintf(note, sizeof(note), "Left #%s.", rname);
        ui_append_sys("warn", note);
        g_who_force = 1;
        ui_switch_dialog("", 0, 0);
        if (w_target_entry != NULL) {
            gtk_entry_set_text(GTK_ENTRY(w_target_entry), "");
        }
    } else {
        char err[MAX_TEXT + 32];
        snprintf(err, sizeof(err), "Leave failed: %s", rep.text);
        ui_append_error(err);
    }
}

static void on_delete_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    if (g_active_peer[0] == '\0') {
        return;
    }

    pthread_mutex_lock(&g_mtx);
    long cur_id = g_my_id;
    pthread_mutex_unlock(&g_mtx);

    if (cur_id <= 0) {
        return;
    }

    char peer[MAX_NAME];
    int is_room = g_active_is_room;
    snprintf(peer, sizeof(peer), "%.*s", MAX_NAME - 1, g_active_peer);

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = is_room ? CMD_DELETE_ROOM : CMD_DELETE_CHAT;
    req.sender_id = cur_id;
    req.reply_type = g_reply_type;
    snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, peer);

    if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) < 0) {
        ui_append_error(errno == EAGAIN
            ? "Message queue is full, try again."
            : "Failed to delete.");
        return;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
        ui_append_error("No response from arbiter.");
        return;
    }
    rep.text[MAX_TEXT - 1] = '\0';

    if (rep.target_id > 0) {
        remove_dialog(peer, is_room);
        if (w_target_entry != NULL) {
            gtk_entry_set_text(GTK_ENTRY(w_target_entry), "");
        }
        char note[MAX_NAME + 48];
        if (is_room) {
            snprintf(note, sizeof(note), "Deleted room #%s.", peer);
        } else {
            snprintf(note, sizeof(note), "Deleted chat with %s.", peer);
        }
        ui_append_sys("warn", note);
        g_who_force = 1;
        ui_rebuild_users_list();
        ui_rebuild_rooms_list();
        ui_update_tab_labels();
    } else {
        char err[MAX_TEXT + 32];
        snprintf(err, sizeof(err), "Delete failed: %s", rep.text);
        ui_append_error(err);
    }
}

static void on_user_row_selected(GtkListBox *box, GtkListBoxRow *row, gpointer data) {
    (void)data;
    if (g_suppress_select || row == NULL) {
        return;
    }

    const char *uname = g_object_get_data(G_OBJECT(row), "username");
    if (uname == NULL) {
        return;
    }

    if (uname[0] == '\0') {
        ui_switch_dialog("", 0, 0);
        return;
    }

    char cur_name[MAX_NAME];
    pthread_mutex_lock(&g_mtx);
    snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
    pthread_mutex_unlock(&g_mtx);

    if (strcmp(uname, cur_name) == 0) {
        gtk_list_box_unselect_row(box, row);
        return;
    }

    ui_switch_dialog(uname, 0, 1);
    if (w_msg_entry != NULL) {
        gtk_widget_grab_focus(w_msg_entry);
    }
}

static void on_room_row_selected(GtkListBox *box, GtkListBoxRow *row, gpointer data) {
    (void)box;
    (void)data;
    if (g_suppress_select || row == NULL) {
        return;
    }

    const char *rname = g_object_get_data(G_OBJECT(row), "roomname");
    if (rname == NULL || rname[0] == '\0') {
        return;
    }

    int joined = GPOINTER_TO_INT(g_object_get_data(G_OBJECT(row), "joined"));
    if (!joined) {
        do_join_room(rname);
        return;
    }

    ui_switch_dialog(rname, 1, 1);
    if (w_msg_entry != NULL) {
        gtk_widget_grab_focus(w_msg_entry);
    }
}

static void on_tab_users_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    ui_select_sidebar_tab("users");
}

static void on_tab_rooms_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    ui_select_sidebar_tab("rooms");
}

static void ui_set_connected_state(int connected, const char *name, long id, int is_debug) {
    if (connected) {
        char status[128];
        char title[128];
        snprintf(status, sizeof(status), "Online: %s%s (id=%ld)",
                 name, is_debug ? " [DEBUG]" : "", id);
        snprintf(title, sizeof(title), "DeepSpeak — %s%s",
                 name, is_debug ? " [DEBUG]" : "");
        gtk_label_set_text(GTK_LABEL(w_status_lbl), status);
        gtk_window_set_title(GTK_WINDOW(w_window), title);
        gtk_label_set_text(GTK_LABEL(w_auth_err), "");
        gtk_entry_set_text(GTK_ENTRY(w_auth_pass), "");
        gtk_stack_set_visible_child_name(GTK_STACK(w_main_stack), "chat");
        gtk_widget_grab_focus(w_target_entry);
    } else {
        gtk_window_set_title(GTK_WINDOW(w_window), "DeepSpeak");
        clear_all_dialogs();
        if (w_target_entry != NULL) {
            gtk_entry_set_text(GTK_ENTRY(w_target_entry), "");
        }
        ui_update_users_list(0, "");
        ui_update_rooms_list(0, "");
        gtk_stack_set_visible_child_name(GTK_STACK(w_main_stack), "auth");
        gtk_widget_grab_focus(w_auth_user);
    }
}

static gboolean ui_handle_event(gpointer user_data) {
    struct ui_event *ev = (struct ui_event *)user_data;

    if (!g_running || w_chat_view == NULL) {
        free(ev);
        return G_SOURCE_REMOVE;
    }

    if (ev->kind == EV_CHAT) {
        struct dialog_entry *d = get_or_create_dialog(ev->sender, 0);
        if (d != NULL) {
            char prefix[MAX_NAME + 8];
            snprintf(prefix, sizeof(prefix), "%s: ", ev->sender);
            ui_append_line_ts(d->buf, (time_t)ev->timestamp, "incoming", prefix, ev->text);

            if (!g_active_is_room && strcmp(g_active_peer, d->peer) == 0) {
                send_read_ack(ev->msg_id, ev->sender);

                send_mark_read(d->peer, 0);
            } else {
                d->unread++;
                queue_dialog_ack(d, ev->msg_id, ev->sender);
                ui_rebuild_users_list();
                ui_update_tab_labels();
            }
        }
    } else if (ev->kind == EV_HISTORY_ITEM) {
        struct dialog_entry *d = get_or_create_dialog(ev->room, ev->is_room);
        if (d != NULL) {
            if (ev->is_room) {
                d->joined = 1;
            }
            if (ev->is_outgoing) {
                ui_append_line_ts(d->buf, (time_t)ev->timestamp, "outgoing", "you: ", ev->text);
            } else {
                char prefix[MAX_NAME + 8];
                snprintf(prefix, sizeof(prefix), "%s: ", ev->sender);
                ui_append_line_ts(d->buf, (time_t)ev->timestamp, "incoming", prefix, ev->text);

            if (!ev->is_outgoing && ev->msg_id != 0) {
                if (g_active_is_room == ev->is_room && strcmp(g_active_peer, d->peer) == 0) {
                    send_mark_read(d->peer, ev->is_room);
                } else {
                    d->unread++;
                }
            }
            }
            ui_schedule_sidebar_rebuild();
        }
    } else if (ev->kind == EV_ROOM_MSG) {
        struct dialog_entry *d = get_or_create_dialog(ev->room, 1);
        if (d != NULL) {
            d->joined = 1;
            if (strcmp(ev->sender, "arbiter") == 0) {
                ui_append_line_to(d->buf, "sys", "* ", ev->text);
                g_who_force = 1;
            } else {
                char prefix[MAX_NAME + 8];
                snprintf(prefix, sizeof(prefix), "%s: ", ev->sender);
                ui_append_line_ts(d->buf, (time_t)ev->timestamp, "incoming", prefix, ev->text);

                if (g_active_is_room && strcmp(g_active_peer, d->peer) == 0) {
                    send_read_ack(ev->msg_id, ev->sender);

                    send_mark_read(d->peer, 1);
                } else {
                    d->unread++;
                    queue_dialog_ack(d, ev->msg_id, ev->sender);
                    ui_rebuild_rooms_list();
                    ui_update_tab_labels();
                }
            }
        }
    } else if (ev->kind == EV_ACK) {

        if (ev->msg_id == 0) {
            ui_mark_all_delivered(ev->sender);
        } else {
            ui_mark_delivered(ev->msg_id);
        }
    } else if (ev->kind == EV_WHO) {
        ui_update_users_list(ev->msg_id, ev->text);
    } else if (ev->kind == EV_ROOMS) {
        ui_update_rooms_list(ev->msg_id, ev->text);
    } else if (ev->kind == EV_LOST) {
        pthread_mutex_lock(&g_mtx);
        g_my_id = 0;
        g_is_debug = 0;
        g_my_name[0] = '\0';
        pthread_mutex_unlock(&g_mtx);
        ui_set_connected_state(0, "", 0, 0);
        gtk_widget_set_sensitive(w_auth_user, FALSE);
        gtk_widget_set_sensitive(w_auth_pass, FALSE);
        gtk_widget_set_sensitive(w_login_btn, FALSE);
        gtk_widget_set_sensitive(w_reg_btn, FALSE);
        gtk_label_set_text(GTK_LABEL(w_auth_err), "Connection lost (arbiter stopped).");
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
                    ev->timestamp = msg.timestamp;
                    copy_utf8_safe(ev->sender, sizeof(ev->sender), msg.sender_name);
                    copy_utf8_safe(ev->text, sizeof(ev->text), msg.text);
                    g_idle_add(ui_handle_event, ev);
                }
            } else if (msg.cmd == CMD_HISTORY_ITEM) {
                msg.name[MAX_NAME - 1] = '\0';
                msg.sender_name[MAX_NAME - 1] = '\0';
                msg.text[MAX_TEXT - 1] = '\0';

                struct ui_event *ev = calloc(1, sizeof(*ev));
                if (ev != NULL) {
                    ev->kind = EV_HISTORY_ITEM;
                    ev->is_room = (msg.target_id != 0);
                    ev->is_outgoing = (msg.sender_id != 0);

                    ev->msg_id = msg.msg_id;
                    ev->timestamp = msg.timestamp;
                    copy_utf8_safe(ev->room, sizeof(ev->room), msg.name);
                    copy_utf8_safe(ev->sender, sizeof(ev->sender), msg.sender_name);
                    copy_utf8_safe(ev->text, sizeof(ev->text), msg.text);
                    g_idle_add(ui_handle_event, ev);
                }
            } else if (msg.cmd == CMD_ROOM_MSG) {
                msg.name[MAX_NAME - 1] = '\0';
                msg.sender_name[MAX_NAME - 1] = '\0';
                msg.text[MAX_TEXT - 1] = '\0';

                struct ui_event *ev = calloc(1, sizeof(*ev));
                if (ev != NULL) {
                    ev->kind = EV_ROOM_MSG;
                    ev->msg_id = msg.msg_id;
                    ev->timestamp = msg.timestamp;
                    copy_utf8_safe(ev->room, sizeof(ev->room), msg.name);
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
                    copy_utf8_safe(ev->sender, sizeof(ev->sender), msg.sender_name);
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
    int poll_ticks = 0;

    long last_who_count = -1;
    char last_who_list[MAX_TEXT];
    last_who_list[0] = '\0';

    long last_room_count = -1;
    char last_room_list[MAX_TEXT];
    last_room_list[0] = '\0';

    while (g_running) {
        usleep(100000);
        hb_ticks++;
        poll_ticks++;

        pthread_mutex_lock(&g_mtx);
        long my_id = g_my_id;
        pthread_mutex_unlock(&g_mtx);

        if (my_id <= 0) {
            last_who_count = -1;
            last_who_list[0] = '\0';
            last_room_count = -1;
            last_room_list[0] = '\0';
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

        if (g_who_force || poll_ticks >= 20) {
            int forced = g_who_force;
            g_who_force = 0;
            poll_ticks = 0;

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
                    if (forced || rep.target_id != last_who_count || strcmp(rep.text, last_who_list) != 0) {
                        last_who_count = rep.target_id;
                        snprintf(last_who_list, sizeof(last_who_list), "%.*s", MAX_TEXT - 1, rep.text);

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

            drain_type(g_msqid, g_who_reply_type);

            memset(&req, 0, sizeof(req));
            req.mtype = ARBITER_TYPE;
            req.cmd = CMD_ROOM_LIST;
            req.sender_id = my_id;
            req.reply_type = g_who_reply_type;

            if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) == 0) {
                struct qmsg rep;
                if (wait_reply(g_msqid, g_who_reply_type, &rep, 50) == 0) {
                    rep.text[MAX_TEXT - 1] = '\0';
                    if (forced || rep.target_id != last_room_count || strcmp(rep.text, last_room_list) != 0) {
                        last_room_count = rep.target_id;
                        snprintf(last_room_list, sizeof(last_room_list), "%.*s", MAX_TEXT - 1, rep.text);

                        struct ui_event *ev = calloc(1, sizeof(*ev));
                        if (ev != NULL) {
                            ev->kind = EV_ROOMS;
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

static void do_auth(int is_login) {
    pthread_mutex_lock(&g_mtx);
    long cur_id = g_my_id;
    pthread_mutex_unlock(&g_mtx);

    if (cur_id > 0) {
        return;
    }

    const char *name = gtk_entry_get_text(GTK_ENTRY(w_auth_user));
    const char *pass = gtk_entry_get_text(GTK_ENTRY(w_auth_pass));

    if (name == NULL || name[0] == '\0' || strchr(name, '#') != NULL ||
        strchr(name, ',') != NULL || strchr(name, ' ') != NULL || strlen(name) >= MAX_NAME) {
        gtk_label_set_text(GTK_LABEL(w_auth_err), "Invalid username (1..31 chars, no spaces, '#' or ',').");
        gtk_widget_grab_focus(w_auth_user);
        return;
    }
    if (pass == NULL || pass[0] == '\0' || strlen(pass) >= MAX_PASS) {
        gtk_label_set_text(GTK_LABEL(w_auth_err), "Enter password.");
        gtk_widget_grab_focus(w_auth_pass);
        return;
    }

    drain_type(g_msqid, g_reply_type);

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = is_login ? CMD_LOGIN : CMD_REGISTER;
    req.reply_type = g_reply_type;
    snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, name);
    snprintf(req.text, sizeof(req.text), "%.*s", MAX_PASS - 1, pass);

    if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) < 0) {
        gtk_label_set_text(GTK_LABEL(w_auth_err), errno == EAGAIN
            ? "Message queue is full, try again."
            : "Failed to send request to arbiter.");
        return;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
        gtk_label_set_text(GTK_LABEL(w_auth_err), "No response from arbiter.");
        return;
    }
    rep.text[MAX_TEXT - 1] = '\0';

    if (rep.target_id > 0) {
        int is_dbg = (rep.sender_id != 0);
        int saved_theme = (int)rep.msg_id;
        if (saved_theme < 0 || saved_theme >= THEME_COUNT) {
            saved_theme = 0;
        }

        pthread_mutex_lock(&g_mtx);
        g_my_id = rep.target_id;
        g_is_debug = is_dbg;
        g_next_msg_id = 1;
        snprintf(g_my_name, sizeof(g_my_name), "%.*s", MAX_NAME - 1, name);
        pthread_mutex_unlock(&g_mtx);

        g_suppress_theme_save = 1;
        gtk_combo_box_set_active(GTK_COMBO_BOX(w_theme_combo), saved_theme);
        apply_theme(saved_theme);
        g_suppress_theme_save = 0;

        ui_set_connected_state(1, name, rep.target_id, is_dbg);
        g_who_force = 1;

        char info[128];
        snprintf(info, sizeof(info), "%s as %s%s (id=%ld).",
                 is_login ? "Logged in" : "Registered", name,
                 is_dbg ? " [DEBUG]" : "", rep.target_id);
        ui_append_sys("sys", info);

        if (!is_dbg) {
            struct qmsg hreq;
            memset(&hreq, 0, sizeof(hreq));
            hreq.mtype = ARBITER_TYPE;
            hreq.cmd = CMD_HISTORY;
            hreq.sender_id = rep.target_id;
            hreq.reply_type = 0;
            msgsnd(g_msqid, &hreq, MSG_SIZE, IPC_NOWAIT);
        }
    } else {
        char info[MAX_TEXT + 32];
        snprintf(info, sizeof(info), "%s", rep.text[0] != '\0' ? rep.text : "Auth failed");
        gtk_label_set_text(GTK_LABEL(w_auth_err), info);
    }
}

static void on_login_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    do_auth(1);
}

static void on_register_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    do_auth(0);
}

static void on_auth_user_activate(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    gtk_widget_grab_focus(w_auth_pass);
}

static void on_auth_pass_activate(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    do_auth(1);
}

static void on_logout_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    send_logout();
    ui_set_connected_state(0, "", 0, 0);
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
        ui_append_error("Login first.");
        return;
    }

    const char *target = gtk_entry_get_text(GTK_ENTRY(w_target_entry));
    const char *text = gtk_entry_get_text(GTK_ENTRY(w_msg_entry));

    if (target == NULL || target[0] == '\0' || strlen(target) >= MAX_NAME + 1) {
        ui_append_error("Enter a valid recipient or #room.");
        gtk_widget_grab_focus(w_target_entry);
        return;
    }
    if (text == NULL || text[0] == '\0') {
        return;
    }

    if (target[0] == '#') {
        const char *rname = target + 1;
        if (rname[0] == '\0' || strlen(rname) >= MAX_NAME - 1) {
            ui_append_error("Enter a valid #room name.");
            return;
        }

        long msg_id = g_next_msg_id++;

        struct qmsg req;
        memset(&req, 0, sizeof(req));
        req.mtype = ARBITER_TYPE;
        req.cmd = CMD_ROOM_MSG;
        req.sender_id = cur_id;
        req.msg_id = msg_id;
        req.reply_type = g_reply_type;
        snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 2, rname);
        copy_utf8_safe(req.text, sizeof(req.text), text);

        if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) < 0) {
            ui_append_error(errno == EAGAIN
                ? "Message queue is full, try again."
                : "Failed to send room message.");
            return;
        }

        struct qmsg rep;
        if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
            ui_append_error("No response from arbiter.");
            return;
        }
        rep.text[MAX_TEXT - 1] = '\0';

        if (rep.target_id < 0) {
            char err[MAX_TEXT + 32];
            snprintf(err, sizeof(err), "Room send failed: %s", rep.text);
            ui_append_error(err);
            return;
        }

        struct dialog_entry *d = get_or_create_dialog(rname, 1);
        if (d == NULL) {
            return;
        }
        d->joined = 1;
        if (!g_active_is_room || strcmp(g_active_peer, d->peer) != 0) {
            ui_switch_dialog(d->peer, 1, 0);
        }
        ui_append_outgoing_to(d->buf, msg_id, text);
        gtk_entry_set_text(GTK_ENTRY(w_msg_entry), "");
        gtk_widget_grab_focus(w_msg_entry);
        return;
    }

    if (strlen(target) >= MAX_NAME) {
        ui_append_error("Enter a valid recipient.");
        gtk_widget_grab_focus(w_target_entry);
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

    long msg_id = g_next_msg_id++;

    struct qmsg chat;
    memset(&chat, 0, sizeof(chat));
    chat.mtype = ARBITER_TYPE;
    chat.cmd = CMD_CHAT;
    chat.sender_id = cur_id;
    chat.msg_id = msg_id;
    chat.reply_type = g_reply_type;
    snprintf(chat.name, sizeof(chat.name), "%.*s", MAX_NAME - 1, target);
    snprintf(chat.sender_name, sizeof(chat.sender_name), "%.*s", MAX_NAME - 1, cur_name);
    copy_utf8_safe(chat.text, sizeof(chat.text), text);

    if (msgsnd(g_msqid, &chat, MSG_SIZE, IPC_NOWAIT) < 0) {
        ui_append_error(errno == EAGAIN
            ? "Message queue is full, try again."
            : "Failed to send message.");
        return;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
        ui_append_error("No response from arbiter.");
        return;
    }
    rep.text[MAX_TEXT - 1] = '\0';

    if (rep.target_id < 0) {
        char err[MAX_TEXT + 32];
        snprintf(err, sizeof(err), "Send failed: %s", rep.text);
        ui_append_error(err);
        return;
    }

    struct dialog_entry *d = get_or_create_dialog(target, 0);
    if (d == NULL) {
        return;
    }
    d->online = (rep.target_id > 0);

    if (g_active_is_room || strcmp(g_active_peer, d->peer) != 0) {
        ui_switch_dialog(d->peer, 0, 0);
    } else {
        ui_update_chat_header();
    }

    ui_append_outgoing_to(d->buf, msg_id, text);

    record_unacked_id(d, msg_id);
    gtk_entry_set_text(GTK_ENTRY(w_msg_entry), "");
    gtk_widget_grab_focus(w_msg_entry);
}


static gboolean on_window_configure(GtkWidget *widget, GdkEventConfigure *event, gpointer data) {
    (void)widget;
    (void)data;
    if (event == NULL || event->width <= 0 || event->height <= 0) {
        return FALSE;
    }

    double scale_w = (double)event->width / 820.0;
    double scale_h = (double)event->height / 500.0;
    double scale = (scale_w < scale_h) ? scale_w : scale_h;

    double delta = (scale - 1.0) * 4.5;
    int new_size = 11 + (int)(delta >= 0.0 ? (delta + 0.5) : (delta - 0.5));
    if (new_size < 10) {
        new_size = 10;
    } else if (new_size > 18) {
        new_size = 18;
    }

    if (new_size != g_font_size) {
        g_font_size = new_size;
        apply_css();
        if (w_sidebar != NULL) {
            int sb_w = 210 + (g_font_size > 11 ? (g_font_size - 11) * 16 : 0);
            gtk_widget_set_size_request(w_sidebar, sb_w, -1);
        }
    }
    return FALSE;
}

static void on_window_destroy(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    w_chat_view = NULL;
    w_chat_header = NULL;
    w_leave_btn = NULL;
    w_delete_btn = NULL;
    w_theme_combo = NULL;
    w_users_list = NULL;
    w_users_lbl = NULL;
    w_rooms_list = NULL;
    w_rooms_lbl = NULL;
    w_room_entry = NULL;
    w_join_btn = NULL;
    w_tab_users = NULL;
    w_tab_rooms = NULL;
    w_sidebar_stack = NULL;


    w_sidebar = NULL;
    send_logout();
    g_running = 0;
    gtk_main_quit();
}

static void build_ui(void) {
    g_sys_buf = create_chat_buffer();

    w_window = gtk_window_new(GTK_WINDOW_TOPLEVEL);
    gtk_window_set_title(GTK_WINDOW(w_window), "DeepSpeak");
    gtk_window_set_default_size(GTK_WINDOW(w_window), 820, 500);
    g_signal_connect(w_window, "destroy", G_CALLBACK(on_window_destroy), NULL);

    g_signal_connect(w_window, "configure-event", G_CALLBACK(on_window_configure), NULL);

    w_main_stack = gtk_stack_new();
    gtk_stack_set_transition_type(GTK_STACK(w_main_stack), GTK_STACK_TRANSITION_TYPE_CROSSFADE);
    gtk_stack_set_transition_duration(GTK_STACK(w_main_stack), 150);
    gtk_container_add(GTK_CONTAINER(w_window), w_main_stack);

    /* Auth screen */
    GtkWidget *auth_outer = gtk_box_new(GTK_ORIENTATION_VERTICAL, 0);
    gtk_widget_set_halign(auth_outer, GTK_ALIGN_CENTER);
    gtk_widget_set_valign(auth_outer, GTK_ALIGN_CENTER);

    GtkWidget *auth_card = gtk_box_new(GTK_ORIENTATION_VERTICAL, 12);
    gtk_widget_set_size_request(auth_card, 340, -1);
    gtk_style_context_add_class(gtk_widget_get_style_context(auth_card), "auth-card");

    GtkWidget *auth_title = gtk_label_new("DeepSpeak");
    gtk_style_context_add_class(gtk_widget_get_style_context(auth_title), "auth-title");
    gtk_box_pack_start(GTK_BOX(auth_card), auth_title, FALSE, FALSE, 0);

    GtkWidget *auth_sub = gtk_label_new("Sign in or create a new account");
    gtk_style_context_add_class(gtk_widget_get_style_context(auth_sub), "auth-hint");
    gtk_box_pack_start(GTK_BOX(auth_card), auth_sub, FALSE, FALSE, 4);

    w_auth_user = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(w_auth_user), "Username");
    gtk_entry_set_max_length(GTK_ENTRY(w_auth_user), MAX_NAME - 1);
    g_signal_connect(w_auth_user, "activate", G_CALLBACK(on_auth_user_activate), NULL);
    gtk_box_pack_start(GTK_BOX(auth_card), w_auth_user, FALSE, FALSE, 0);

    w_auth_pass = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(w_auth_pass), "Password");
    gtk_entry_set_max_length(GTK_ENTRY(w_auth_pass), MAX_PASS - 1);
    gtk_entry_set_visibility(GTK_ENTRY(w_auth_pass), FALSE);
    gtk_entry_set_invisible_char(GTK_ENTRY(w_auth_pass), 0x2022);
    g_signal_connect(w_auth_pass, "activate", G_CALLBACK(on_auth_pass_activate), NULL);
    gtk_box_pack_start(GTK_BOX(auth_card), w_auth_pass, FALSE, FALSE, 0);

    GtkWidget *auth_btns = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    gtk_box_set_homogeneous(GTK_BOX(auth_btns), TRUE);

    w_login_btn = gtk_button_new_with_label("Log In");
    g_signal_connect(w_login_btn, "clicked", G_CALLBACK(on_login_clicked), NULL);

    w_reg_btn = gtk_button_new_with_label("Register");
    gtk_style_context_add_class(gtk_widget_get_style_context(w_reg_btn), "tab-btn");
    g_signal_connect(w_reg_btn, "clicked", G_CALLBACK(on_register_clicked), NULL);

    gtk_box_pack_start(GTK_BOX(auth_btns), w_login_btn, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(auth_btns), w_reg_btn, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(auth_card), auth_btns, FALSE, FALSE, 4);

    w_auth_err = gtk_label_new("");
    gtk_label_set_line_wrap(GTK_LABEL(w_auth_err), TRUE);
    gtk_style_context_add_class(gtk_widget_get_style_context(w_auth_err), "auth-err");
    gtk_box_pack_start(GTK_BOX(auth_card), w_auth_err, FALSE, FALSE, 0);

    GtkWidget *dbg_hint = gtk_label_new("Debug accounts: user* with debug password");
    gtk_style_context_add_class(gtk_widget_get_style_context(dbg_hint), "auth-hint");
    gtk_box_pack_start(GTK_BOX(auth_card), dbg_hint, FALSE, FALSE, 0);

    gtk_box_pack_start(GTK_BOX(auth_outer), auth_card, FALSE, FALSE, 0);
    gtk_stack_add_named(GTK_STACK(w_main_stack), auth_outer, "auth");

    /* Chat screen */
    GtkWidget *root = gtk_box_new(GTK_ORIENTATION_VERTICAL, 10);
    gtk_widget_set_margin_top(root, 12);
    gtk_widget_set_margin_bottom(root, 12);
    gtk_widget_set_margin_start(root, 12);
    gtk_widget_set_margin_end(root, 12);

    GtkWidget *top_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);

    w_status_lbl = gtk_label_new("Disconnected");
    gtk_widget_set_halign(w_status_lbl, GTK_ALIGN_START);

    w_logout_btn = gtk_button_new_with_label("Logout");
    g_signal_connect(w_logout_btn, "clicked", G_CALLBACK(on_logout_clicked), NULL);

    w_theme_combo = gtk_combo_box_text_new();
    for (int i = 0; i < THEME_COUNT; i++) {
        gtk_combo_box_text_append_text(GTK_COMBO_BOX_TEXT(w_theme_combo), g_themes[i].name);
    }
    gtk_combo_box_set_active(GTK_COMBO_BOX(w_theme_combo), g_theme_idx);
    g_signal_connect(w_theme_combo, "changed", G_CALLBACK(on_theme_changed), NULL);

    gtk_box_pack_start(GTK_BOX(top_bar), w_status_lbl, TRUE, TRUE, 0);
    gtk_box_pack_end(GTK_BOX(top_bar), w_logout_btn, FALSE, FALSE, 0);
    gtk_box_pack_end(GTK_BOX(top_bar), w_theme_combo, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(root), top_bar, FALSE, FALSE, 0);

    GtkWidget *mid_box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 10);

    GtkWidget *chat_col = gtk_box_new(GTK_ORIENTATION_VERTICAL, 6);

    GtkWidget *header_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    w_chat_header = gtk_label_new("★ System");
    gtk_widget_set_halign(w_chat_header, GTK_ALIGN_START);

    w_leave_btn = gtk_button_new_with_label("Leave");
    gtk_style_context_add_class(gtk_widget_get_style_context(w_leave_btn), "tab-btn");
    g_signal_connect(w_leave_btn, "clicked", G_CALLBACK(on_leave_room_clicked), NULL);

    w_delete_btn = gtk_button_new_with_label("Delete Chat");
    gtk_style_context_add_class(gtk_widget_get_style_context(w_delete_btn), "leave-btn");
    g_signal_connect(w_delete_btn, "clicked", G_CALLBACK(on_delete_clicked), NULL);

    gtk_box_pack_start(GTK_BOX(header_bar), w_chat_header, TRUE, TRUE, 0);
    gtk_box_pack_end(GTK_BOX(header_bar), w_delete_btn, FALSE, FALSE, 0);
    gtk_box_pack_end(GTK_BOX(header_bar), w_leave_btn, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(chat_col), header_bar, FALSE, FALSE, 0);

    GtkWidget *scroll = gtk_scrolled_window_new(NULL, NULL);
    gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(scroll), GTK_POLICY_AUTOMATIC, GTK_POLICY_AUTOMATIC);

    w_chat_view = gtk_text_view_new_with_buffer(g_sys_buf);
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

    w_sidebar = sidebar;
    gtk_widget_set_size_request(sidebar, 210, -1);

    GtkWidget *tab_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_box_set_homogeneous(GTK_BOX(tab_bar), TRUE);

    w_tab_users = gtk_button_new_with_label("Users");
    gtk_style_context_add_class(gtk_widget_get_style_context(w_tab_users), "tab-btn");
    g_signal_connect(w_tab_users, "clicked", G_CALLBACK(on_tab_users_clicked), NULL);

    w_tab_rooms = gtk_button_new_with_label("Rooms");
    gtk_style_context_add_class(gtk_widget_get_style_context(w_tab_rooms), "tab-btn");
    g_signal_connect(w_tab_rooms, "clicked", G_CALLBACK(on_tab_rooms_clicked), NULL);

    gtk_box_pack_start(GTK_BOX(tab_bar), w_tab_users, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(tab_bar), w_tab_rooms, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(sidebar), tab_bar, FALSE, FALSE, 0);

    w_sidebar_stack = gtk_stack_new();
    gtk_stack_set_transition_type(GTK_STACK(w_sidebar_stack), GTK_STACK_TRANSITION_TYPE_NONE);

    GtkWidget *users_page = gtk_box_new(GTK_ORIENTATION_VERTICAL, 6);
    w_users_lbl = gtk_label_new("Online (0)");
    gtk_widget_set_halign(w_users_lbl, GTK_ALIGN_START);
    gtk_box_pack_start(GTK_BOX(users_page), w_users_lbl, FALSE, FALSE, 0);

    GtkWidget *users_scroll = gtk_scrolled_window_new(NULL, NULL);
    gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(users_scroll), GTK_POLICY_AUTOMATIC, GTK_POLICY_AUTOMATIC);

    w_users_list = gtk_list_box_new();
    gtk_list_box_set_selection_mode(GTK_LIST_BOX(w_users_list), GTK_SELECTION_SINGLE);
    g_signal_connect(w_users_list, "row-selected", G_CALLBACK(on_user_row_selected), NULL);

    gtk_container_add(GTK_CONTAINER(users_scroll), w_users_list);
    gtk_box_pack_start(GTK_BOX(users_page), users_scroll, TRUE, TRUE, 0);
    gtk_stack_add_named(GTK_STACK(w_sidebar_stack), users_page, "users");

    GtkWidget *rooms_page = gtk_box_new(GTK_ORIENTATION_VERTICAL, 6);
    GtkWidget *join_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 4);

    w_room_entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(w_room_entry), "room");
    gtk_entry_set_max_length(GTK_ENTRY(w_room_entry), MAX_NAME - 2);
    gtk_entry_set_width_chars(GTK_ENTRY(w_room_entry), 10);
    g_signal_connect(w_room_entry, "activate", G_CALLBACK(on_join_room_clicked), NULL);

    w_join_btn = gtk_button_new_with_label("Join");
    g_signal_connect(w_join_btn, "clicked", G_CALLBACK(on_join_room_clicked), NULL);

    gtk_box_pack_start(GTK_BOX(join_bar), w_room_entry, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(join_bar), w_join_btn, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(rooms_page), join_bar, FALSE, FALSE, 0);

    w_rooms_lbl = gtk_label_new("Active rooms (0)");
    gtk_widget_set_halign(w_rooms_lbl, GTK_ALIGN_START);
    gtk_box_pack_start(GTK_BOX(rooms_page), w_rooms_lbl, FALSE, FALSE, 0);

    GtkWidget *rooms_scroll = gtk_scrolled_window_new(NULL, NULL);
    gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(rooms_scroll), GTK_POLICY_AUTOMATIC, GTK_POLICY_AUTOMATIC);

    w_rooms_list = gtk_list_box_new();
    gtk_list_box_set_selection_mode(GTK_LIST_BOX(w_rooms_list), GTK_SELECTION_SINGLE);
    g_signal_connect(w_rooms_list, "row-selected", G_CALLBACK(on_room_row_selected), NULL);

    gtk_container_add(GTK_CONTAINER(rooms_scroll), w_rooms_list);
    gtk_box_pack_start(GTK_BOX(rooms_page), rooms_scroll, TRUE, TRUE, 0);
    gtk_stack_add_named(GTK_STACK(w_sidebar_stack), rooms_page, "rooms");

    gtk_box_pack_start(GTK_BOX(sidebar), w_sidebar_stack, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(mid_box), sidebar, FALSE, FALSE, 0);

    gtk_box_pack_start(GTK_BOX(root), mid_box, TRUE, TRUE, 0);

    GtkWidget *bottom_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    w_target_entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(w_target_entry), "To / #room");
    gtk_entry_set_max_length(GTK_ENTRY(w_target_entry), MAX_NAME);
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

    gtk_stack_add_named(GTK_STACK(w_main_stack), root, "chat");

    apply_theme(g_theme_idx);
    gtk_widget_show_all(w_window);
    ui_select_sidebar_tab("users");
    ui_set_connected_state(0, "", 0, 0);
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
    return 0;
}