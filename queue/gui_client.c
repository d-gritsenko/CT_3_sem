#define _DEFAULT_SOURCE

#include <errno.h>
#include <gtk/gtk.h>

#include <gdk/gdkkeysyms.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/ipc.h>
#include <sys/msg.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "protocol.h"

#define MAX_DIALOGS (MAX_CLIENTS + MAX_ROOMS + 8)
#define MAX_PENDING_ACKS 128

#define MAX_FILE_ANCHORS 64


#define MAX_TRACKED_MSGS 256
enum ui_event_kind {
    EV_CHAT = 1,
    EV_ACK = 2,
    EV_LOST = 3,
    EV_WHO = 4,
    EV_ROOM_MSG = 5,
    EV_ROOMS = 6,

    EV_HISTORY_ITEM = 7,

    EV_TYPING = 8,
    EV_MSG_EDIT = 9,
    EV_MSG_DELETE = 10
};

struct ui_event {
    int kind;
    int is_room;
    int is_outgoing;

    int is_unread;
    int is_edited;
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


struct msg_slot {
    long msg_id;
    int is_outgoing;
    int is_file;
    char sender[MAX_NAME];
    char raw_text[MAX_TEXT];
    GtkTextMark *line_start;
    GtkTextMark *body_start;
    GtkTextMark *body_end;
    GtkTextMark *line_end;
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

    GtkTextChildAnchor *file_anchors[MAX_FILE_ANCHORS];
    int file_anchor_count;

    char typing_user[MAX_NAME];
    time_t typing_until;

    struct msg_slot msgs[MAX_TRACKED_MSGS];
    int msg_count;
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

static int g_manual_font_size = 0;

static time_t g_last_typing_sent = 0;
static char g_last_typing_peer[MAX_NAME] = "";
static int g_last_typing_is_room = 0;
static guint g_typing_timer_id = 0;


static long g_edit_msg_id = 0;
static char g_edit_reply_prefix[128] = "";
static char g_reply_author[MAX_NAME] = "";
static char g_reply_snippet[64] = "";

static long g_ctx_msg_id = 0;
static char g_ctx_msg_sender[MAX_NAME] = "";
static char g_ctx_msg_raw[MAX_TEXT] = "";

static int g_search_cur = -1;
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

static GtkWidget *w_menu_btn = NULL;
static GtkWidget *w_menu_popover = NULL;
static GtkWidget *w_acct_info_lbl = NULL;
static GtkWidget *w_scale_combo = NULL;
static GtkWidget *w_logout_btn = NULL;
static GtkWidget *w_status_lbl = NULL;
static GtkWidget *w_theme_combo = NULL;
static GtkWidget *w_chat_header = NULL;
static GtkWidget *w_chat_menu_btn = NULL;
static GtkWidget *w_chat_menu = NULL;

static GtkWidget *w_mi_search = NULL;
static GtkWidget *w_mi_leave = NULL;
static GtkWidget *w_mi_delete = NULL;
static GtkWidget *w_chat_view = NULL;

static GtkWidget *w_search_bar = NULL;
static GtkWidget *w_search_entry = NULL;
static GtkWidget *w_search_count_lbl = NULL;

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

static GtkWidget *w_new_chat_btn = NULL;
static GtkWidget *w_msg_entry = NULL;
static GtkWidget *w_send_btn = NULL;
static GtkWidget *w_file_btn = NULL;
static GtkWidget *w_attach_menu = NULL;

static GtkWidget *w_compose_bar = NULL;
static GtkWidget *w_compose_lbl = NULL;
static GtkWidget *w_msg_menu = NULL;
static GtkWidget *w_mi_msg_reply = NULL;
static GtkWidget *w_mi_msg_edit = NULL;
static GtkWidget *w_mi_msg_delete = NULL;
static void ui_rebuild_users_list(void);
static void ui_rebuild_rooms_list(void);
static void ui_update_tab_labels(void);
static void ui_update_chat_header(void);

static void ui_cancel_compose(void);
static void ui_switch_dialog(const char *peer, int is_room, int set_target);

static void ui_append_sys(const char *tag_name, const char *text);
static void ui_append_error(const char *text);


static const struct ui_theme *current_theme(void) {
    if (g_theme_idx < 0 || g_theme_idx >= THEME_COUNT) {
        return &g_themes[0];
    }
    return &g_themes[g_theme_idx];
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

static void send_typing(void) {
    if (g_active_peer[0] == '\0' || g_msqid < 0) {
        return;
    }

    pthread_mutex_lock(&g_mtx);
    long my_id = g_my_id;
    pthread_mutex_unlock(&g_mtx);

    if (my_id <= 0) {
        return;
    }

    time_t now = time(NULL);
    if ((now - g_last_typing_sent) < 2 &&
        g_last_typing_is_room == g_active_is_room &&
        strcmp(g_last_typing_peer, g_active_peer) == 0) {
        return;
    }

    g_last_typing_sent = now;
    g_last_typing_is_room = g_active_is_room;
    snprintf(g_last_typing_peer, sizeof(g_last_typing_peer), "%.*s", MAX_NAME - 1, g_active_peer);

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = CMD_TYPING;
    req.sender_id = my_id;
    req.target_id = g_active_is_room ? 1 : 0;
    snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, g_active_peer);
    msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT);
}

static void on_msg_entry_changed(GtkEditable *editable, gpointer data) {
    (void)data;
    const char *txt = gtk_entry_get_text(GTK_ENTRY(editable));
    if (txt != NULL && txt[0] != '\0') {
        send_typing();
    }
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
    if ((tag = gtk_text_tag_table_lookup(table, "quote")) != NULL) {
        g_object_set(tag, "foreground", t->muted, NULL);
    }
    if ((tag = gtk_text_tag_table_lookup(table, "edited")) != NULL) {
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

    if ((tag = gtk_text_tag_table_lookup(table, "search_hit")) != NULL) {
        g_object_set(tag, "background", t->row_sel_bg, "foreground", t->row_sel_fg, NULL);
    }
    if ((tag = gtk_text_tag_table_lookup(table, "search_cur")) != NULL) {
        g_object_set(tag, "background", t->accent, "foreground", t->btn_fg, NULL);
    }
}

static GtkTextBuffer *create_chat_buffer(void) {
    const struct ui_theme *t = current_theme();
    GtkTextBuffer *buf = gtk_text_buffer_new(NULL);
    gtk_text_buffer_create_tag(buf, "time", "foreground", t->muted, NULL);

    gtk_text_buffer_create_tag(buf, "quote", "foreground", t->muted, "style", PANGO_STYLE_ITALIC, NULL);
    gtk_text_buffer_create_tag(buf, "edited", "foreground", t->muted, "style", PANGO_STYLE_ITALIC, NULL);
    gtk_text_buffer_create_tag(buf, "incoming", "foreground", t->incoming, "weight", PANGO_WEIGHT_BOLD, NULL);
    gtk_text_buffer_create_tag(buf, "outgoing", "foreground", t->outgoing, "weight", PANGO_WEIGHT_BOLD, NULL);
    gtk_text_buffer_create_tag(buf, "ack", "foreground", t->ack, NULL);
    gtk_text_buffer_create_tag(buf, "warn", "foreground", t->warn, NULL);
    gtk_text_buffer_create_tag(buf, "err", "foreground", t->err, "weight", PANGO_WEIGHT_BOLD, NULL);
    gtk_text_buffer_create_tag(buf, "sys", "foreground", t->sys, NULL);

    gtk_text_buffer_create_tag(buf, "search_hit", "background", t->row_sel_bg, "foreground", t->row_sel_fg, NULL);
    gtk_text_buffer_create_tag(buf, "search_cur", "background", t->accent, "foreground", t->btn_fg, "weight", PANGO_WEIGHT_BOLD, NULL);
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



    char css_data[8192];
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

        "list row:selected { background-color: %s; color: %s; }\n"
        "button.file-btn { background-image: none; background-color: %s; border: 1px solid %s; border-radius: 6px; padding: 3px 10px; }\n"
        "button.file-btn, button.file-btn label { color: %s; font-weight: bold; }\n"

        "button.file-btn:hover { background-image: none; background-color: %s; }\n"
        "button.photo-btn { background-image: none; background-color: %s; border: 1px solid %s; border-radius: 8px; padding: 6px; }\n"
        "button.photo-btn label { color: %s; font-weight: normal; font-size: %dpt; }\n"
        "button.photo-btn:hover { background-image: none; background-color: %s; border-color: %s; }\n"
        "menu { background-color: %s; color: %s; border: 1px solid %s; padding: 4px; }\n"
        "menuitem { padding: 6px 14px; color: %s; }\n"

        "menuitem:hover { background-color: %s; color: %s; }\n"
        "popover, popover.background { background-color: %s; color: %s; border: 1px solid %s; border-radius: 8px; padding: 12px; }\n"

        ".acct-info { font-weight: bold; color: %s; }\n"
        ".compose-bar { background-color: %s; border: 1px solid %s; border-radius: 6px; padding: 4px 8px; }\n",
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

        t->row_sel_bg, t->row_sel_fg,

        t->entry_bg, t->border, t->accent, t->row_hover,
        t->entry_bg, t->border,
        t->muted, hint_pt,
        t->row_hover, t->accent,
        t->entry_bg, t->fg, t->border,
        t->fg,

        t->row_sel_bg, t->row_sel_fg,
        t->chat_bg, t->fg, t->border,

        t->accent, t->entry_bg, t->accent
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


static void apply_font_size(int new_size) {
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
}

static int compute_auto_font_size(int width, int height) {
    if (width <= 0 || height <= 0) {
        return 11;
    }
    double scale_w = (double)width / 820.0;
    double scale_h = (double)height / 500.0;
    double scale = (scale_w < scale_h) ? scale_w : scale_h;
    double delta = (scale - 1.0) * 4.5;
    int sz = 11 + (int)(delta >= 0.0 ? (delta + 0.5) : (delta - 0.5));
    if (sz < 10) {
        sz = 10;
    } else if (sz > 18) {
        sz = 18;
    }
    return sz;
}

static void on_scale_changed(GtkComboBox *combo, gpointer data) {
    (void)data;
    int idx = gtk_combo_box_get_active(combo);
    static const int sizes[] = { 0, 10, 12, 14, 16, 18 };
    if (idx < 0 || idx >= (int)(sizeof(sizes) / sizeof(sizes[0]))) {
        return;
    }
    g_manual_font_size = sizes[idx];
    if (g_manual_font_size > 0) {
        apply_font_size(g_manual_font_size);
    } else if (w_window != NULL) {
        int w = 0;
        int h = 0;
        gtk_window_get_size(GTK_WINDOW(w_window), &w, &h);
        apply_font_size(compute_auto_font_size(w, h));
    }
}

static void on_menu_btn_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    if (w_menu_popover != NULL) {
        gtk_popover_popup(GTK_POPOVER(w_menu_popover));
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


static void on_file_download_clicked(GtkWidget *btn, gpointer data) {
    (void)data;
    const char *stored = g_object_get_data(G_OBJECT(btn), "stored");
    const char *orig = g_object_get_data(G_OBJECT(btn), "orig");
    if (stored == NULL || stored[0] == '\0') {
        return;
    }

    pthread_mutex_lock(&g_mtx);
    long cur_id = g_my_id;
    pthread_mutex_unlock(&g_mtx);
    if (cur_id <= 0) {
        ui_append_error("Login first.");
        return;
    }

    GtkWidget *dlg = gtk_file_chooser_dialog_new(
        "Save File",
        GTK_WINDOW(w_window),
        GTK_FILE_CHOOSER_ACTION_SAVE,
        "_Cancel", GTK_RESPONSE_CANCEL,
        "_Save", GTK_RESPONSE_ACCEPT,
        NULL
    );
    gtk_file_chooser_set_do_overwrite_confirmation(GTK_FILE_CHOOSER(dlg), TRUE);
    gtk_file_chooser_set_current_name(GTK_FILE_CHOOSER(dlg), (orig != NULL && orig[0] != '\0') ? orig : stored);

    char *save_path = NULL;
    if (gtk_dialog_run(GTK_DIALOG(dlg)) == GTK_RESPONSE_ACCEPT) {
        save_path = gtk_file_chooser_get_filename(GTK_FILE_CHOOSER(dlg));
    }
    gtk_widget_destroy(dlg);

    if (save_path == NULL) {
        return;
    }

    drain_type(g_msqid, g_reply_type);

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = CMD_FILE_GET;
    req.sender_id = cur_id;
    req.reply_type = g_reply_type;
    snprintf(req.text, sizeof(req.text), "%.*s", MAX_TEXT - 1, stored);

    if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) < 0) {
        g_free(save_path);
        ui_append_error("Failed to request file from arbiter.");
        return;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
        g_free(save_path);
        ui_append_error("No response from arbiter.");
        return;
    }
    rep.text[MAX_TEXT - 1] = '\0';

    if (rep.target_id < 0) {
        char err[MAX_TEXT + 32];
        snprintf(err, sizeof(err), "Download failed: %s", rep.text);
        ui_append_error(err);
    } else if (shm_save_to_file((int)rep.target_id, rep.timestamp, save_path) == 0) {
        char note[320];
        snprintf(note, sizeof(note), "Saved file to %s", save_path);
        ui_append_sys("sys", note);
    } else {
        ui_append_error("Failed to save file to disk.");
    }
    g_free(save_path);
}


static int is_image_filename(const char *name) {
    if (name == NULL) {
        return 0;
    }
    const char *dot = strrchr(name, '.');
    if (dot == NULL || dot[1] == '\0') {
        return 0;
    }
    return strcasecmp(dot, ".png") == 0 ||
           strcasecmp(dot, ".jpg") == 0 ||
           strcasecmp(dot, ".jpeg") == 0 ||
           strcasecmp(dot, ".gif") == 0 ||
           strcasecmp(dot, ".bmp") == 0 ||
           strcasecmp(dot, ".webp") == 0;
}

static GdkPixbuf *shm_fetch_pixbuf(const char *stored) {
    if (stored == NULL || stored[0] == '\0' || g_msqid < 0) {
        return NULL;
    }

    pthread_mutex_lock(&g_mtx);
    long cur_id = g_my_id;
    pthread_mutex_unlock(&g_mtx);
    if (cur_id <= 0) {
        return NULL;
    }

    drain_type(g_msqid, g_reply_type);

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = CMD_FILE_GET;
    req.sender_id = cur_id;
    req.reply_type = g_reply_type;
    snprintf(req.text, sizeof(req.text), "%.*s", MAX_TEXT - 1, stored);

    if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) < 0) {
        return NULL;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
        return NULL;
    }

    int shmid = (int)rep.target_id;
    long fsize = rep.timestamp;
    if (shmid < 0 || fsize <= 0 || fsize > MAX_FILE_SIZE) {
        if (shmid >= 0) {
            shmctl(shmid, IPC_RMID, NULL);
        }
        return NULL;
    }

    void *ptr = shmat(shmid, NULL, SHM_RDONLY);
    if (ptr == (void *)-1) {
        shmctl(shmid, IPC_RMID, NULL);
        return NULL;
    }

    GdkPixbufLoader *loader = gdk_pixbuf_loader_new();
    gboolean ok = gdk_pixbuf_loader_write(loader, (const guchar *)ptr, (gsize)fsize, NULL);
    if (!gdk_pixbuf_loader_close(loader, NULL)) {
        ok = FALSE;
    }

    shmdt(ptr);
    shmctl(shmid, IPC_RMID, NULL);

    GdkPixbuf *result = NULL;
    if (ok) {
        GdkPixbuf *raw = gdk_pixbuf_loader_get_pixbuf(loader);
        if (raw != NULL) {
            int w = gdk_pixbuf_get_width(raw);
            int h = gdk_pixbuf_get_height(raw);
            int max_w = 260;
            int max_h = 220;
            if (w > max_w || h > max_h) {
                double sx = (double)max_w / (double)w;
                double sy = (double)max_h / (double)h;
                double sc = (sx < sy) ? sx : sy;
                int nw = (int)(w * sc);
                int nh = (int)(h * sc);
                if (nw < 1) {
                    nw = 1;
                }
                if (nh < 1) {
                    nh = 1;
                }
                result = gdk_pixbuf_scale_simple(raw, nw, nh, GDK_INTERP_BILINEAR);
            } else {
                result = g_object_ref(raw);
            }
        }
    }

    g_object_unref(loader);
    return result;
}

static void ui_attach_file_widget(GtkTextChildAnchor *anchor) {
    if (w_chat_view == NULL || anchor == NULL) {
        return;
    }
    GList *widgets = gtk_text_child_anchor_get_widgets(anchor);
    if (widgets != NULL) {
        g_list_free(widgets);
        return;
    }
    const char *stored = g_object_get_data(G_OBJECT(anchor), "stored");
    const char *orig = g_object_get_data(G_OBJECT(anchor), "orig");
    long fsize = GPOINTER_TO_INT(g_object_get_data(G_OBJECT(anchor), "fsize"));
    if (stored == NULL || orig == NULL) {
        return;
    }

    char sz_str[32];
    format_file_size(fsize, sz_str, sizeof(sz_str));

    GtkWidget *btn = NULL;
    if (is_image_filename(orig)) {
        GdkPixbuf *pb = g_object_get_data(G_OBJECT(anchor), "pixbuf");
        if (pb == NULL && g_object_get_data(G_OBJECT(anchor), "pb_tried") == NULL) {
            g_object_set_data(G_OBJECT(anchor), "pb_tried", GINT_TO_POINTER(1));
            pb = shm_fetch_pixbuf(stored);
            if (pb != NULL) {
                g_object_set_data_full(G_OBJECT(anchor), "pixbuf", pb, g_object_unref);
            }
        }
        if (pb != NULL) {
            btn = gtk_button_new();
            gtk_style_context_add_class(gtk_widget_get_style_context(btn), "photo-btn");
            gtk_widget_set_tooltip_text(btn, "Click to save photo");

            GtkWidget *box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 4);
            GtkWidget *img = gtk_image_new_from_pixbuf(pb);
            char cap[160];
            snprintf(cap, sizeof(cap), "%s (%s)", orig, sz_str);
            GtkWidget *lbl = gtk_label_new(cap);
            gtk_box_pack_start(GTK_BOX(box), img, FALSE, FALSE, 0);
            gtk_box_pack_start(GTK_BOX(box), lbl, FALSE, FALSE, 0);
            gtk_container_add(GTK_CONTAINER(btn), box);
        }
    }

    if (btn == NULL) {
        char label[160];
        snprintf(label, sizeof(label), "File: %s (%s)", orig, sz_str);
        btn = gtk_button_new_with_label(label);
        gtk_style_context_add_class(gtk_widget_get_style_context(btn), "file-btn");
        gtk_widget_set_tooltip_text(btn, "Click to save file");
    }

    g_object_set_data_full(G_OBJECT(btn), "stored", g_strdup(stored), g_free);
    g_object_set_data_full(G_OBJECT(btn), "orig", g_strdup(orig), g_free);
    g_signal_connect(btn, "clicked", G_CALLBACK(on_file_download_clicked), NULL);

    gtk_text_view_add_child_at_anchor(GTK_TEXT_VIEW(w_chat_view), btn, anchor);
    gtk_widget_show_all(btn);
}

static void ui_attach_dialog_widgets(struct dialog_entry *d) {
    if (d == NULL || w_chat_view == NULL) {
        return;
    }
    if (gtk_text_view_get_buffer(GTK_TEXT_VIEW(w_chat_view)) != d->buf) {
        return;
    }
    for (int i = 0; i < d->file_anchor_count; i++) {
        ui_attach_file_widget(d->file_anchors[i]);
    }
}

static void ui_insert_file_or_text(struct dialog_entry *d, GtkTextBuffer *buf, GtkTextIter *end, const char *body) {
    char stored[96];
    char orig[96];
    long fsize = 0;
    if (d != NULL && parse_file_marker(body, stored, sizeof(stored), &fsize, orig, sizeof(orig))) {
        GtkTextChildAnchor *anchor = gtk_text_buffer_create_child_anchor(buf, end);
        g_object_set_data_full(G_OBJECT(anchor), "stored", g_strdup(stored), g_free);
        g_object_set_data_full(G_OBJECT(anchor), "orig", g_strdup(orig), g_free);
        g_object_set_data(G_OBJECT(anchor), "fsize", GINT_TO_POINTER((int)fsize));
        if (d->file_anchor_count < MAX_FILE_ANCHORS) {
            d->file_anchors[d->file_anchor_count++] = anchor;
        }
        if (w_chat_view != NULL && gtk_text_view_get_buffer(GTK_TEXT_VIEW(w_chat_view)) == buf) {
            ui_attach_file_widget(anchor);
        }
    } else if (body != NULL && body[0] != '\0') {
        gtk_text_buffer_insert(buf, end, body, -1);
    }
}



static void ui_record_msg_slot(struct dialog_entry *d, long msg_id, int is_outgoing, int is_file,
                               const char *sender, const char *raw_text,
                               GtkTextMark *ls, GtkTextMark *bs, GtkTextMark *be, GtkTextMark *le) {
    if (d == NULL || ls == NULL || bs == NULL || be == NULL || le == NULL) {
        return;
    }
    if (d->msg_count >= MAX_TRACKED_MSGS) {
        if (d->buf != NULL) {
            gtk_text_buffer_delete_mark(d->buf, d->msgs[0].line_start);
            gtk_text_buffer_delete_mark(d->buf, d->msgs[0].body_start);
            gtk_text_buffer_delete_mark(d->buf, d->msgs[0].body_end);
            gtk_text_buffer_delete_mark(d->buf, d->msgs[0].line_end);
        }
        memmove(&d->msgs[0], &d->msgs[1], sizeof(struct msg_slot) * (MAX_TRACKED_MSGS - 1));
        d->msg_count = MAX_TRACKED_MSGS - 1;
    }
    struct msg_slot *s = &d->msgs[d->msg_count++];
    memset(s, 0, sizeof(*s));
    s->msg_id = msg_id;
    s->is_outgoing = is_outgoing;
    s->is_file = is_file;
    snprintf(s->sender, sizeof(s->sender), "%.*s", MAX_NAME - 1, sender != NULL ? sender : "");
    copy_utf8_safe(s->raw_text, sizeof(s->raw_text), raw_text != NULL ? raw_text : "");
    s->line_start = ls;
    s->body_start = bs;
    s->body_end = be;
    s->line_end = le;
}

static void ui_apply_msg_edit(struct dialog_entry *d, long msg_id, const char *new_raw) {
    if (d == NULL || d->buf == NULL || msg_id <= 0 || new_raw == NULL) {
        return;
    }
    for (int i = 0; i < d->msg_count; i++) {
        if (d->msgs[i].msg_id == msg_id) {
            struct msg_slot *slot = &d->msgs[i];
            copy_utf8_safe(slot->raw_text, sizeof(slot->raw_text), new_raw);

            char rauthor[MAX_NAME];
            char rsnip[64];
            const char *display_body = new_raw;
            if (parse_reply_prefix(new_raw, rauthor, sizeof(rauthor), rsnip, sizeof(rsnip), &display_body)) {
                /* display_body points past reply prefix */
            }

            GtkTextIter s;
            GtkTextIter e;
            gtk_text_buffer_get_iter_at_mark(d->buf, &s, slot->body_start);
            gtk_text_buffer_get_iter_at_mark(d->buf, &e, slot->body_end);
            gtk_text_buffer_delete(d->buf, &s, &e);
            gtk_text_buffer_insert(d->buf, &s, display_body, -1);
            gtk_text_buffer_insert_with_tags_by_name(d->buf, &s, " (edited)", -1, "edited", NULL);
            gtk_text_buffer_move_mark(d->buf, slot->body_end, &s);
            return;
        }
    }
}

static void ui_apply_msg_delete(struct dialog_entry *d, long msg_id) {
    if (d == NULL || d->buf == NULL || msg_id <= 0) {
        return;
    }
    for (int i = 0; i < d->msg_count; i++) {
        if (d->msgs[i].msg_id == msg_id) {
            struct msg_slot *slot = &d->msgs[i];
            GtkTextIter s;
            GtkTextIter e;
            gtk_text_buffer_get_iter_at_mark(d->buf, &s, slot->line_start);
            gtk_text_buffer_get_iter_at_mark(d->buf, &e, slot->line_end);
            gtk_text_buffer_delete(d->buf, &s, &e);
            gtk_text_buffer_delete_mark(d->buf, slot->line_start);
            gtk_text_buffer_delete_mark(d->buf, slot->body_start);
            gtk_text_buffer_delete_mark(d->buf, slot->body_end);
            gtk_text_buffer_delete_mark(d->buf, slot->line_end);
            for (int k = i; k < d->msg_count - 1; k++) {
                d->msgs[k] = d->msgs[k + 1];
            }
            d->msg_count--;
            return;
        }
    }
}


static void ui_append_dialog_msg_ts(struct dialog_entry *d, long msg_id, time_t ts_val,
                                    int is_outgoing, int is_edited,
                                    const char *tag_name, const char *sender_name,
                                    const char *prefix, const char *body) {
    if (d == NULL || d->buf == NULL) {
        return;
    }

    char ts[24];
    if (ts_val > 0) {
        format_timestamp_from(ts_val, ts, sizeof(ts));
    } else {
        get_timestamp(ts, sizeof(ts));
    }

    GtkTextIter end;
    gtk_text_buffer_get_end_iter(d->buf, &end);
    GtkTextMark *ls = gtk_text_buffer_create_mark(d->buf, NULL, &end, TRUE);

    char rauthor[MAX_NAME];
    char rsnip[64];
    const char *display_body = (body != NULL) ? body : "";
    if (parse_reply_prefix(display_body, rauthor, sizeof(rauthor), rsnip, sizeof(rsnip), &display_body)) {
        char qline[MAX_NAME + 80];
        snprintf(qline, sizeof(qline), "  ↳ %s: %s\n", rauthor, rsnip);
        gtk_text_buffer_insert_with_tags_by_name(d->buf, &end, qline, -1, "quote", NULL);
    }

    char ts_str[32];
    snprintf(ts_str, sizeof(ts_str), "[%s] ", ts);
    gtk_text_buffer_insert_with_tags_by_name(d->buf, &end, ts_str, -1, "time", NULL);

    if (prefix != NULL && prefix[0] != '\0') {
        gtk_text_buffer_insert_with_tags_by_name(d->buf, &end, prefix, -1, tag_name, NULL);
    }

    gtk_text_buffer_get_end_iter(d->buf, &end);
    GtkTextMark *bs = gtk_text_buffer_create_mark(d->buf, NULL, &end, TRUE);

    char dummy_s[96];
    char dummy_o[96];
    int is_file = parse_file_marker(display_body, dummy_s, sizeof(dummy_s), NULL, dummy_o, sizeof(dummy_o));
    ui_insert_file_or_text(d, d->buf, &end, display_body);

    if (is_edited && !is_file) {
        gtk_text_buffer_get_end_iter(d->buf, &end);
        gtk_text_buffer_insert_with_tags_by_name(d->buf, &end, " (edited)", -1, "edited", NULL);
    }

    gtk_text_buffer_get_end_iter(d->buf, &end);
    GtkTextMark *be = gtk_text_buffer_create_mark(d->buf, NULL, &end, TRUE);

    gtk_text_buffer_insert(d->buf, &end, "\n", -1);
    gtk_text_buffer_get_end_iter(d->buf, &end);
    GtkTextMark *le = gtk_text_buffer_create_mark(d->buf, NULL, &end, TRUE);

    ui_record_msg_slot(d, msg_id, is_outgoing, is_file, sender_name, body, ls, bs, be, le);
    ui_scroll_if_active(d->buf);
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



static void ui_append_outgoing_to(struct dialog_entry *d, long msg_id, const char *body) {

    if (d == NULL || d->buf == NULL) {
        return;
    }

    char ts[16];
    get_timestamp(ts, sizeof(ts));

    GtkTextIter end;
    gtk_text_buffer_get_end_iter(d->buf, &end);
    GtkTextMark *ls = gtk_text_buffer_create_mark(d->buf, NULL, &end, TRUE);

    char rauthor[MAX_NAME];
    char rsnip[64];
    const char *display_body = (body != NULL) ? body : "";
    if (parse_reply_prefix(display_body, rauthor, sizeof(rauthor), rsnip, sizeof(rsnip), &display_body)) {
        char qline[MAX_NAME + 80];
        snprintf(qline, sizeof(qline), "  ↳ %s: %s\n", rauthor, rsnip);
        gtk_text_buffer_insert_with_tags_by_name(d->buf, &end, qline, -1, "quote", NULL);
    }

    char ts_str[24];
    snprintf(ts_str, sizeof(ts_str), "[%s] ", ts);
    gtk_text_buffer_insert_with_tags_by_name(d->buf, &end, ts_str, -1, "time", NULL);
    gtk_text_buffer_insert_with_tags_by_name(d->buf, &end, "you: ", -1, "outgoing", NULL);

    gtk_text_buffer_get_end_iter(d->buf, &end);
    GtkTextMark *bs = gtk_text_buffer_create_mark(d->buf, NULL, &end, TRUE);

    char dummy_s[96];
    char dummy_o[96];
    int is_file = parse_file_marker(display_body, dummy_s, sizeof(dummy_s), NULL, dummy_o, sizeof(dummy_o));
    ui_insert_file_or_text(d, d->buf, &end, display_body);

    gtk_text_buffer_get_end_iter(d->buf, &end);
    GtkTextMark *be = gtk_text_buffer_create_mark(d->buf, NULL, &end, TRUE);

    char m_start[32];
    char m_end[32];
    snprintf(m_start, sizeof(m_start), "ms_%ld", msg_id);
    snprintf(m_end, sizeof(m_end), "me_%ld", msg_id);

    gtk_text_buffer_create_mark(d->buf, m_start, &end, TRUE);
    gtk_text_buffer_insert_with_tags_by_name(d->buf, &end, " ✓", -1, "time", NULL);
    gtk_text_buffer_get_end_iter(d->buf, &end);
    gtk_text_buffer_create_mark(d->buf, m_end, &end, TRUE);
    gtk_text_buffer_insert(d->buf, &end, "\n", -1);

    gtk_text_buffer_get_end_iter(d->buf, &end);
    GtkTextMark *le = gtk_text_buffer_create_mark(d->buf, NULL, &end, TRUE);

    char cur_name[MAX_NAME];
    pthread_mutex_lock(&g_mtx);
    snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
    pthread_mutex_unlock(&g_mtx);

    ui_record_msg_slot(d, msg_id, 1, is_file, cur_name, body, ls, bs, be, le);
    ui_scroll_if_active(d->buf);
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
        if (w_chat_menu_btn != NULL) {
            gtk_widget_set_visible(w_chat_menu_btn, FALSE);
        }
        return;
    }

    time_t now = time(NULL);
    char header[MAX_NAME * 2 + 96];
    if (g_active_is_room) {
        struct dialog_entry *d = find_dialog(g_active_peer, 1);
        int members = (d != NULL && d->member_count > 0) ? d->member_count : 1;
        if (d != NULL && d->typing_until > now && d->typing_user[0] != '\0') {
            snprintf(header, sizeof(header), "#%s (%d %s) — %s is typing...",
                     g_active_peer, members, members == 1 ? "member" : "members", d->typing_user);
        } else {
            snprintf(header, sizeof(header), "#%s (%d %s)", g_active_peer, members, members == 1 ? "member" : "members");
        }
        gtk_label_set_text(GTK_LABEL(w_chat_header), header);
    } else {
        struct dialog_entry *d = find_dialog(g_active_peer, 0);
        int online = is_user_online(g_active_peer);
        if (d != NULL && d->typing_until > now) {
            snprintf(header, sizeof(header), "Chat with %s — typing...", g_active_peer);
        } else {
            snprintf(header, sizeof(header), "Chat with %s (%s)", g_active_peer, online ? "online" : "offline");
        }
        gtk_label_set_text(GTK_LABEL(w_chat_header), header);
    }

    if (w_chat_menu_btn != NULL) {
        gtk_widget_set_visible(w_chat_menu_btn, TRUE);
    }
}

static gboolean ui_typing_tick(gpointer data) {
    (void)data;
    if (!g_running) {
        return G_SOURCE_REMOVE;
    }
    time_t now = time(NULL);
    int need_header = 0;
    for (int i = 0; i < MAX_DIALOGS; i++) {
        if (g_dialogs[i].used && g_dialogs[i].typing_until > 0 && now >= g_dialogs[i].typing_until) {
            g_dialogs[i].typing_until = 0;
            g_dialogs[i].typing_user[0] = '\0';
            if (g_active_is_room == g_dialogs[i].is_room &&
                strcmp(g_active_peer, g_dialogs[i].peer) == 0) {
                need_header = 1;
            }
        }
    }
    if (need_header) {
        ui_update_chat_header();
    }
    return G_SOURCE_CONTINUE;
}

static void ui_clear_search_tags(GtkTextBuffer *buf) {
    if (buf == NULL) {
        return;
    }
    GtkTextIter start;
    GtkTextIter end;
    gtk_text_buffer_get_bounds(buf, &start, &end);
    gtk_text_buffer_remove_tag_by_name(buf, "search_hit", &start, &end);
    gtk_text_buffer_remove_tag_by_name(buf, "search_cur", &start, &end);
}

static void ui_search_run(int jump_dir) {
    if (w_chat_view == NULL || w_search_entry == NULL || w_search_count_lbl == NULL) {
        return;
    }
    GtkTextBuffer *buf = gtk_text_view_get_buffer(GTK_TEXT_VIEW(w_chat_view));
    if (buf == NULL) {
        return;
    }
    ui_clear_search_tags(buf);

    const char *query = gtk_entry_get_text(GTK_ENTRY(w_search_entry));
    if (query == NULL || query[0] == '\0') {

        g_search_cur = -1;
        gtk_label_set_text(GTK_LABEL(w_search_count_lbl), "");
        return;
    }

    GtkTextIter iter;
    GtkTextIter m_start;
    GtkTextIter m_end;
    gtk_text_buffer_get_start_iter(buf, &iter);

    int total = 0;
    while (gtk_text_iter_forward_search(&iter, query, GTK_TEXT_SEARCH_CASE_INSENSITIVE, &m_start, &m_end, NULL)) {
        gtk_text_buffer_apply_tag_by_name(buf, "search_hit", &m_start, &m_end);
        total++;
        iter = m_end;
    }

    if (total == 0) {
        g_search_cur = -1;
        gtk_label_set_text(GTK_LABEL(w_search_count_lbl), "0 / 0");
        return;
    }

    if (jump_dir == 0 || g_search_cur < 0 || g_search_cur >= total) {
        g_search_cur = total - 1;
    } else if (jump_dir < 0) {
        g_search_cur = (g_search_cur - 1 + total) % total;
    } else {
        g_search_cur = (g_search_cur + 1) % total;
    }

    gtk_text_buffer_get_start_iter(buf, &iter);
    int idx = 0;
    while (gtk_text_iter_forward_search(&iter, query, GTK_TEXT_SEARCH_CASE_INSENSITIVE, &m_start, &m_end, NULL)) {
        if (idx == g_search_cur) {
            gtk_text_buffer_apply_tag_by_name(buf, "search_cur", &m_start, &m_end);
            gtk_text_view_scroll_to_iter(GTK_TEXT_VIEW(w_chat_view), &m_start, 0.1, TRUE, 0.0, 0.5);
            break;
        }
        idx++;
        iter = m_end;
    }

    char cnt[32];
    snprintf(cnt, sizeof(cnt), "%d / %d", g_search_cur + 1, total);
    gtk_label_set_text(GTK_LABEL(w_search_count_lbl), cnt);
}

static void ui_close_search(void) {
    if (w_chat_view != NULL) {
        ui_clear_search_tags(gtk_text_view_get_buffer(GTK_TEXT_VIEW(w_chat_view)));
    }

    g_search_cur = -1;
    if (w_search_entry != NULL) {
        gtk_entry_set_text(GTK_ENTRY(w_search_entry), "");
    }
    if (w_search_count_lbl != NULL) {
        gtk_label_set_text(GTK_LABEL(w_search_count_lbl), "");
    }
    if (w_search_bar != NULL) {
        gtk_widget_set_visible(w_search_bar, FALSE);
    }
    if (g_active_peer[0] != '\0' && w_msg_entry != NULL) {
        gtk_widget_grab_focus(w_msg_entry);
    }
}

static void on_search_menu_activate(GtkMenuItem *item, gpointer data) {
    (void)item;
    (void)data;
    if (w_search_bar == NULL || w_search_entry == NULL || g_active_peer[0] == '\0') {
        return;
    }
    gtk_widget_set_visible(w_search_bar, TRUE);
    gtk_widget_grab_focus(w_search_entry);
    gtk_editable_select_region(GTK_EDITABLE(w_search_entry), 0, -1);
}

static void on_search_entry_changed(GtkEditable *editable, gpointer data) {
    (void)editable;
    (void)data;
    ui_search_run(0);
}

static void on_search_prev_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    ui_search_run(-1);
}

static void on_search_next_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    ui_search_run(1);
}

static void on_search_close_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    ui_close_search();
}


static gboolean on_search_entry_key_press(GtkWidget *widget, GdkEventKey *event, gpointer data) {
    (void)widget;
    (void)data;
    if (event != NULL && event->keyval == GDK_KEY_Escape) {
        ui_close_search();
        return TRUE;
    }
    return FALSE;
}

static void ui_cancel_compose(void) {
    g_edit_msg_id = 0;
    g_edit_reply_prefix[0] = '\0';
    g_reply_author[0] = '\0';
    g_reply_snippet[0] = '\0';
    if (w_compose_lbl != NULL) {
        gtk_label_set_text(GTK_LABEL(w_compose_lbl), "");
    }
    if (w_compose_bar != NULL) {
        gtk_widget_set_visible(w_compose_bar, FALSE);
    }
    if (w_send_btn != NULL) {
        gtk_button_set_label(GTK_BUTTON(w_send_btn), "Send");
    }
}

static void on_compose_cancel_clicked(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    int was_edit = (g_edit_msg_id > 0);
    ui_cancel_compose();
    if (was_edit && w_msg_entry != NULL) {
        gtk_entry_set_text(GTK_ENTRY(w_msg_entry), "");
    }
    if (w_msg_entry != NULL) {
        gtk_widget_grab_focus(w_msg_entry);
    }
}

static gboolean on_msg_entry_key_press(GtkWidget *widget, GdkEventKey *event, gpointer data) {
    (void)widget;
    (void)data;
    if (event != NULL && event->keyval == GDK_KEY_Escape &&
        (g_edit_msg_id > 0 || g_reply_author[0] != '\0')) {
        on_compose_cancel_clicked(NULL, NULL);
        return TRUE;
    }
    return FALSE;
}

static void on_msg_reply_activate(GtkMenuItem *item, gpointer data) {
    (void)item;
    (void)data;
    if (g_ctx_msg_sender[0] == '\0') {
        return;
    }
    ui_cancel_compose();

    snprintf(g_reply_author, sizeof(g_reply_author), "%.*s", MAX_NAME - 1, g_ctx_msg_sender);

    char rauthor[MAX_NAME];
    char rsnip[64];
    const char *src_body = g_ctx_msg_raw;
    if (parse_reply_prefix(src_body, rauthor, sizeof(rauthor), rsnip, sizeof(rsnip), &src_body)) {
        /* use body without nested quote */
    }
    char fstored[96];
    char forig[96];

    char clean_src[112];
    if (parse_file_marker(src_body, fstored, sizeof(fstored), NULL, forig, sizeof(forig))) {
        snprintf(clean_src, sizeof(clean_src), "[File: %s]", forig);
    } else {
        copy_utf8_safe(clean_src, sizeof(clean_src), src_body);
    }

    size_t pos = 0;
    for (size_t i = 0; clean_src[i] != '\0' && pos + 1 < sizeof(g_reply_snippet) && pos < 44; i++) {
        unsigned char c = (unsigned char)clean_src[i];
        if (c == '|' || c == '[' || c == ']' || c == '\n' || c == '\r') {
            g_reply_snippet[pos++] = ' ';
        } else {
            g_reply_snippet[pos++] = (char)c;
        }
    }
    g_reply_snippet[pos] = '\0';
    copy_utf8_safe(g_reply_snippet, sizeof(g_reply_snippet), g_reply_snippet);

    if (w_compose_lbl != NULL && w_compose_bar != NULL) {
        char lbl[MAX_NAME + 96];
        snprintf(lbl, sizeof(lbl), "↩ Replying to %s: %s", g_reply_author, g_reply_snippet);
        gtk_label_set_text(GTK_LABEL(w_compose_lbl), lbl);
        gtk_widget_set_visible(w_compose_bar, TRUE);
    }
    if (w_msg_entry != NULL) {
        gtk_widget_grab_focus(w_msg_entry);
    }
}

static void on_msg_edit_activate(GtkMenuItem *item, gpointer data) {
    (void)item;
    (void)data;
    if (g_ctx_msg_id <= 0) {
        return;
    }
    ui_cancel_compose();
    g_edit_msg_id = g_ctx_msg_id;

    char rauthor[MAX_NAME];
    char rsnip[64];
    const char *body_only = g_ctx_msg_raw;
    if (parse_reply_prefix(g_ctx_msg_raw, rauthor, sizeof(rauthor), rsnip, sizeof(rsnip), &body_only)) {
        size_t plen = (size_t)(body_only - g_ctx_msg_raw);
        if (plen >= sizeof(g_edit_reply_prefix)) {
            plen = sizeof(g_edit_reply_prefix) - 1;
        }
        memcpy(g_edit_reply_prefix, g_ctx_msg_raw, plen);
        g_edit_reply_prefix[plen] = '\0';
    } else {
        g_edit_reply_prefix[0] = '\0';
    }

    if (w_compose_lbl != NULL && w_compose_bar != NULL) {
        gtk_label_set_text(GTK_LABEL(w_compose_lbl), "✏ Editing message...");
        gtk_widget_set_visible(w_compose_bar, TRUE);
    }
    if (w_send_btn != NULL) {
        gtk_button_set_label(GTK_BUTTON(w_send_btn), "Save");
    }
    if (w_msg_entry != NULL) {
        gtk_entry_set_text(GTK_ENTRY(w_msg_entry), body_only);
        gtk_widget_grab_focus(w_msg_entry);
        gtk_editable_set_position(GTK_EDITABLE(w_msg_entry), -1);
    }
}

static void on_msg_delete_activate(GtkMenuItem *item, gpointer data) {
    (void)item;
    (void)data;
    if (g_ctx_msg_id <= 0 || g_active_peer[0] == '\0') {
        return;
    }

    pthread_mutex_lock(&g_mtx);
    long cur_id = g_my_id;
    pthread_mutex_unlock(&g_mtx);
    if (cur_id <= 0) {
        return;
    }

    long del_id = g_ctx_msg_id;
    int is_room = g_active_is_room;
    char peer[MAX_NAME];
    snprintf(peer, sizeof(peer), "%.*s", MAX_NAME - 1, g_active_peer);

    drain_type(g_msqid, g_reply_type);

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = CMD_MSG_DELETE;
    req.sender_id = cur_id;
    req.reply_type = g_reply_type;
    req.target_id = is_room ? 1 : 0;
    req.msg_id = del_id;
    snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, peer);

    if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) < 0) {
        ui_append_error("Failed to send delete request.");
        return;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
        ui_append_error("No response from arbiter.");
        return;
    }
    if (rep.target_id > 0) {
        struct dialog_entry *d = find_dialog(peer, is_room);
        ui_apply_msg_delete(d, del_id);
        if (g_edit_msg_id == del_id) {
            on_compose_cancel_clicked(NULL, NULL);
        }
    } else {
        rep.text[MAX_TEXT - 1] = '\0';
        char err[MAX_TEXT + 32];
        snprintf(err, sizeof(err), "Delete failed: %s", rep.text);
        ui_append_error(err);
    }
}

static gboolean on_chat_view_button_press(GtkWidget *widget, GdkEventButton *event, gpointer data) {
    (void)data;
    if (event == NULL || event->type != GDK_BUTTON_PRESS || event->button != 3) {
        return FALSE;
    }
    if (g_active_peer[0] == '\0' || w_msg_menu == NULL) {
        return FALSE;
    }
    struct dialog_entry *d = find_dialog(g_active_peer, g_active_is_room);
    if (d == NULL || d->buf == NULL) {
        return FALSE;
    }

    gint bx = 0;
    gint by = 0;
    gtk_text_view_window_to_buffer_coords(GTK_TEXT_VIEW(widget), GTK_TEXT_WINDOW_WIDGET,
                                          (gint)event->x, (gint)event->y, &bx, &by);
    GtkTextIter click_it;
    gtk_text_view_get_iter_at_location(GTK_TEXT_VIEW(widget), &click_it, bx, by);

    struct msg_slot *hit = NULL;
    for (int i = 0; i < d->msg_count; i++) {
        GtkTextIter ls;
        GtkTextIter le;
        gtk_text_buffer_get_iter_at_mark(d->buf, &ls, d->msgs[i].line_start);
        gtk_text_buffer_get_iter_at_mark(d->buf, &le, d->msgs[i].line_end);
        if (gtk_text_iter_compare(&click_it, &ls) >= 0 && gtk_text_iter_compare(&click_it, &le) < 0) {
            hit = &d->msgs[i];
            break;
        }
    }
    if (hit == NULL) {
        return FALSE;
    }

    g_ctx_msg_id = hit->msg_id;
    snprintf(g_ctx_msg_sender, sizeof(g_ctx_msg_sender), "%.*s", MAX_NAME - 1, hit->sender);
    copy_utf8_safe(g_ctx_msg_raw, sizeof(g_ctx_msg_raw), hit->raw_text);

    pthread_mutex_lock(&g_mtx);
    int is_dbg = g_is_debug;
    pthread_mutex_unlock(&g_mtx);

    gtk_widget_set_visible(w_mi_msg_reply, TRUE);
    gtk_widget_set_visible(w_mi_msg_edit, (hit->is_outgoing && !hit->is_file && hit->msg_id > 0 && !is_dbg) ? TRUE : FALSE);
    gtk_widget_set_visible(w_mi_msg_delete, (hit->is_outgoing && hit->msg_id > 0 && !is_dbg) ? TRUE : FALSE);

    gtk_menu_popup_at_pointer(GTK_MENU(w_msg_menu), (GdkEvent *)event);
    return TRUE;
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
    (void)set_target;
    if (w_chat_view == NULL) {
        return;

    }

    ui_cancel_compose();
    ui_close_search();

    if (peer == NULL || peer[0] == '\0') {
        g_active_peer[0] = '\0';
        g_active_is_room = 0;
        gtk_text_view_set_buffer(GTK_TEXT_VIEW(w_chat_view), g_sys_buf);
        ui_scroll_if_active(g_sys_buf);
        ui_update_chat_header();
        if (w_new_chat_btn != NULL) {
            gtk_widget_set_visible(w_new_chat_btn, TRUE);
        }
        if (w_file_btn != NULL) {
            gtk_widget_set_visible(w_file_btn, FALSE);
        }
        if (w_msg_entry != NULL) {
            gtk_widget_set_sensitive(w_msg_entry, FALSE);
            gtk_entry_set_placeholder_text(GTK_ENTRY(w_msg_entry), "Select a chat or click '+' to start...");
        }
        if (w_send_btn != NULL) {
            gtk_widget_set_sensitive(w_send_btn, FALSE);
        }
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
    ui_attach_dialog_widgets(d);
    ui_scroll_if_active(d->buf);
    ui_update_chat_header();

    if (w_new_chat_btn != NULL) {
        gtk_widget_set_visible(w_new_chat_btn, FALSE);
    }
    if (w_file_btn != NULL) {
        gtk_widget_set_visible(w_file_btn, TRUE);
    }
    if (w_msg_entry != NULL) {
        gtk_widget_set_sensitive(w_msg_entry, TRUE);
        gtk_entry_set_placeholder_text(GTK_ENTRY(w_msg_entry), "Message...");
    }
    if (w_send_btn != NULL) {
        gtk_widget_set_sensitive(w_send_btn, TRUE);
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

    GtkWidget *confirm = gtk_message_dialog_new(
        GTK_WINDOW(w_window),
        GTK_DIALOG_MODAL | GTK_DIALOG_DESTROY_WITH_PARENT,
        GTK_MESSAGE_QUESTION,
        GTK_BUTTONS_NONE,
        is_room ? "Delete room #%s?" : "Delete chat with %s?",
        peer
    );
    gtk_message_dialog_format_secondary_text(
        GTK_MESSAGE_DIALOG(confirm),
        is_room ? "You will leave this room and clear its history for yourself."
                : "Chat history will be removed for you."
    );
    gtk_dialog_add_buttons(GTK_DIALOG(confirm),
                           "_Cancel", GTK_RESPONSE_CANCEL,
                           "_Delete", GTK_RESPONSE_ACCEPT,
                           NULL);
    gint cresp = gtk_dialog_run(GTK_DIALOG(confirm));
    gtk_widget_destroy(confirm);
    if (cresp != GTK_RESPONSE_ACCEPT) {
        return;
    }
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

static void on_chat_menu_clicked(GtkWidget *widget, gpointer data) {
    (void)data;
    if (w_chat_menu == NULL || g_active_peer[0] == '\0') {
        return;
    }
    if (g_active_is_room) {
        struct dialog_entry *d = find_dialog(g_active_peer, 1);
        gtk_widget_set_visible(w_mi_leave, (d != NULL && d->joined) ? TRUE : FALSE);
        gtk_menu_item_set_label(GTK_MENU_ITEM(w_mi_delete), "Delete Room");
    } else {
        gtk_widget_set_visible(w_mi_leave, FALSE);
        gtk_menu_item_set_label(GTK_MENU_ITEM(w_mi_delete), "Delete Chat");
    }
    gtk_widget_show(w_mi_delete);

    if (w_mi_search != NULL) {
        gtk_widget_show(w_mi_search);
    }
    gtk_menu_popup_at_widget(
        GTK_MENU(w_chat_menu),
        widget,
        GDK_GRAVITY_SOUTH_EAST,
        GDK_GRAVITY_NORTH_EAST,
        NULL
    );
}

static void on_new_chat_clicked(GtkWidget *widget, gpointer data) {
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

    GtkWidget *dlg = gtk_dialog_new_with_buttons(
        "New Chat",
        GTK_WINDOW(w_window),
        GTK_DIALOG_MODAL | GTK_DIALOG_DESTROY_WITH_PARENT,
        "_Cancel", GTK_RESPONSE_CANCEL,
        "_Open", GTK_RESPONSE_ACCEPT,
        NULL
    );
    gtk_dialog_set_default_response(GTK_DIALOG(dlg), GTK_RESPONSE_ACCEPT);

    GtkWidget *content = gtk_dialog_get_content_area(GTK_DIALOG(dlg));
    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 8);
    gtk_widget_set_margin_top(box, 12);
    gtk_widget_set_margin_bottom(box, 12);
    gtk_widget_set_margin_start(box, 14);
    gtk_widget_set_margin_end(box, 14);

    GtkWidget *lbl = gtk_label_new("Enter username or #room:");
    gtk_widget_set_halign(lbl, GTK_ALIGN_START);

    GtkWidget *entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(entry), "alice or #general");
    gtk_entry_set_max_length(GTK_ENTRY(entry), MAX_NAME);
    gtk_entry_set_activates_default(GTK_ENTRY(entry), TRUE);

    gtk_box_pack_start(GTK_BOX(box), lbl, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(box), entry, FALSE, FALSE, 0);
    gtk_container_add(GTK_CONTAINER(content), box);
    gtk_widget_show_all(dlg);

    char input[MAX_NAME + 2] = "";
    if (gtk_dialog_run(GTK_DIALOG(dlg)) == GTK_RESPONSE_ACCEPT) {
        const char *txt = gtk_entry_get_text(GTK_ENTRY(entry));
        if (txt != NULL) {
            snprintf(input, sizeof(input), "%.*s", MAX_NAME, txt);
        }
    }
    gtk_widget_destroy(dlg);

    const char *p = input;
    while (*p == ' ') {
        p++;
    }
    if (*p == '\0') {
        return;
    }

    if (*p == '#') {
        do_join_room(p);
        return;
    }

    if (!is_valid_ident(p, MAX_NAME)) {
        ui_append_error("Invalid username (no spaces, '#' or ',').");
        return;
    }
    if (strcmp(p, cur_name) == 0) {
        ui_append_error("Cannot start chat with yourself.");
        return;
    }
    if (strcasecmp(p, "arbiter") == 0) {
        ui_append_error("Cannot start chat with arbiter.");
        return;
    }

    ui_switch_dialog(p, 0, 1);
    ui_select_sidebar_tab("users");
    if (w_msg_entry != NULL) {
        gtk_widget_grab_focus(w_msg_entry);
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

        snprintf(status, sizeof(status), "%s%s",

                 name, is_debug ? " [DEBUG]" : "");
        snprintf(title, sizeof(title), "DeepSpeak — %s%s",
                 name, is_debug ? " [DEBUG]" : "");
        gtk_label_set_text(GTK_LABEL(w_status_lbl), status);
        gtk_window_set_title(GTK_WINDOW(w_window), title);
        gtk_label_set_text(GTK_LABEL(w_auth_err), "");
        gtk_entry_set_text(GTK_ENTRY(w_auth_pass), "");
        gtk_stack_set_visible_child_name(GTK_STACK(w_main_stack), "chat");

        if (w_acct_info_lbl != NULL) {
            char acct[128];
            snprintf(acct, sizeof(acct), "%s%s\nID: %ld", name, is_debug ? " [DEBUG]" : "", id);
            gtk_label_set_text(GTK_LABEL(w_acct_info_lbl), acct);
        }
        ui_switch_dialog("", 0, 0);
    } else {

        if (w_menu_popover != NULL) {
            gtk_popover_popdown(GTK_POPOVER(w_menu_popover));
        }
        clear_all_dialogs();
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


            ui_append_dialog_msg_ts(d, ev->msg_id, (time_t)ev->timestamp, 0, 0, "incoming", ev->sender, prefix, ev->text);

            if (d->typing_until > 0 && strcmp(d->typing_user, ev->sender) == 0) {
                d->typing_until = 0;
                d->typing_user[0] = '\0';
                if (!g_active_is_room && strcmp(g_active_peer, d->peer) == 0) {
                    ui_update_chat_header();
                }
            }

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
                ui_append_dialog_msg_ts(d, ev->msg_id, (time_t)ev->timestamp, 1, ev->is_edited,
                                        "outgoing", ev->sender, "you: ", ev->text);
            } else {
                char prefix[MAX_NAME + 8];
                snprintf(prefix, sizeof(prefix), "%s: ", ev->sender);
                ui_append_dialog_msg_ts(d, ev->msg_id, (time_t)ev->timestamp, 0, ev->is_edited,
                                        "incoming", ev->sender, prefix, ev->text);



                if (ev->is_unread) {

                    if (g_active_is_room == ev->is_room && strcmp(g_active_peer, d->peer) == 0) {

                        send_read_ack(ev->msg_id, ev->sender);

                        send_mark_read(d->peer, ev->is_room);

                    } else {

                        d->unread++;

                        queue_dialog_ack(d, ev->msg_id, ev->sender);

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


                ui_append_dialog_msg_ts(d, ev->msg_id, (time_t)ev->timestamp, 0, 0, "incoming", ev->sender, prefix, ev->text);

                if (d->typing_until > 0 && strcmp(d->typing_user, ev->sender) == 0) {
                    d->typing_until = 0;
                    d->typing_user[0] = '\0';
                    if (g_active_is_room && strcmp(g_active_peer, d->peer) == 0) {
                        ui_update_chat_header();
                    }
                }

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

    } else if (ev->kind == EV_TYPING) {
        struct dialog_entry *d = find_dialog(ev->room, ev->is_room);
        if (d != NULL) {
            d->typing_until = time(NULL) + 3;
            snprintf(d->typing_user, sizeof(d->typing_user), "%.*s", MAX_NAME - 1, ev->sender);
            if (g_active_is_room == ev->is_room && strcmp(g_active_peer, d->peer) == 0) {
                ui_update_chat_header();
            }
        }

    } else if (ev->kind == EV_MSG_EDIT) {
        struct dialog_entry *d = find_dialog(ev->room, ev->is_room);
        ui_apply_msg_edit(d, ev->msg_id, ev->text);
    } else if (ev->kind == EV_MSG_DELETE) {
        struct dialog_entry *d = find_dialog(ev->room, ev->is_room);
        ui_apply_msg_delete(d, ev->msg_id);
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
                    ev->is_outgoing = (msg.sender_id & 1) != 0;
                    ev->is_unread = (msg.sender_id & 2) != 0;
                    ev->is_edited = (msg.sender_id & 4) != 0;

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


            } else if (msg.cmd == CMD_TYPING) {
                msg.sender_name[MAX_NAME - 1] = '\0';

                struct ui_event *ev = calloc(1, sizeof(*ev));
                if (ev != NULL) {
                    ev->kind = EV_TYPING;
                    ev->is_room = (msg.target_id != 0);
                    copy_utf8_safe(ev->room, sizeof(ev->room), msg.name);
                    copy_utf8_safe(ev->sender, sizeof(ev->sender), msg.sender_name);
                    g_idle_add(ui_handle_event, ev);
                }

            } else if (msg.cmd == CMD_MSG_EDIT) {
                msg.name[MAX_NAME - 1] = '\0';
                msg.sender_name[MAX_NAME - 1] = '\0';
                msg.text[MAX_TEXT - 1] = '\0';

                struct ui_event *ev = calloc(1, sizeof(*ev));
                if (ev != NULL) {
                    ev->kind = EV_MSG_EDIT;
                    ev->is_room = (msg.target_id != 0);
                    ev->msg_id = msg.msg_id;
                    copy_utf8_safe(ev->room, sizeof(ev->room), msg.name);
                    copy_utf8_safe(ev->sender, sizeof(ev->sender), msg.sender_name);
                    copy_utf8_safe(ev->text, sizeof(ev->text), msg.text);
                    g_idle_add(ui_handle_event, ev);
                }
            } else if (msg.cmd == CMD_MSG_DELETE) {
                msg.name[MAX_NAME - 1] = '\0';
                msg.sender_name[MAX_NAME - 1] = '\0';

                struct ui_event *ev = calloc(1, sizeof(*ev));
                if (ev != NULL) {
                    ev->kind = EV_MSG_DELETE;
                    ev->is_room = (msg.target_id != 0);
                    ev->msg_id = msg.msg_id;
                    copy_utf8_safe(ev->room, sizeof(ev->room), msg.name);
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

    if (w_menu_popover != NULL) {
        gtk_popover_popdown(GTK_POPOVER(w_menu_popover));
    }
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


    if (g_active_peer[0] == '\0') {
        ui_append_error("Select a chat or click '+' to start a new one.");
        return;
    }
    const char *text = gtk_entry_get_text(GTK_ENTRY(w_msg_entry));
    if (text == NULL || text[0] == '\0') {
        return;
    }

    if (g_edit_msg_id > 0) {
        char final_text[MAX_TEXT];
        if (g_edit_reply_prefix[0] != '\0') {
            char combined[MAX_TEXT + 128];
            snprintf(combined, sizeof(combined), "%s%s", g_edit_reply_prefix, text);
            copy_utf8_safe(final_text, sizeof(final_text), combined);
        } else {
            copy_utf8_safe(final_text, sizeof(final_text), text);
        }

        drain_type(g_msqid, g_reply_type);
        struct qmsg req;
        memset(&req, 0, sizeof(req));
        req.mtype = ARBITER_TYPE;
        req.cmd = CMD_MSG_EDIT;
        req.sender_id = cur_id;
        req.reply_type = g_reply_type;
        req.target_id = g_active_is_room ? 1 : 0;
        req.msg_id = g_edit_msg_id;
        snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, g_active_peer);
        copy_utf8_safe(req.text, sizeof(req.text), final_text);

        if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) < 0) {
            ui_append_error("Failed to send edit.");
            return;
        }
        struct qmsg rep;
        if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
            ui_append_error("No response from arbiter.");
            return;
        }
        if (rep.target_id > 0) {
            struct dialog_entry *d = find_dialog(g_active_peer, g_active_is_room);
            ui_apply_msg_edit(d, g_edit_msg_id, final_text);
            gtk_entry_set_text(GTK_ENTRY(w_msg_entry), "");
            ui_cancel_compose();
        } else {
            rep.text[MAX_TEXT - 1] = '\0';
            char err[MAX_TEXT + 32];
            snprintf(err, sizeof(err), "Edit failed: %s", rep.text);
            ui_append_error(err);
        }
        gtk_widget_grab_focus(w_msg_entry);
        return;
    }

    char payload[MAX_TEXT];
    if (g_reply_author[0] != '\0') {
        char combined[MAX_TEXT + 128];
        snprintf(combined, sizeof(combined), "[[REPLY:%s|%s]]%s", g_reply_author, g_reply_snippet, text);
        copy_utf8_safe(payload, sizeof(payload), combined);
    } else {
        copy_utf8_safe(payload, sizeof(payload), text);
    }
    if (g_active_is_room) {
        const char *rname = g_active_peer;
        long msg_id = g_next_msg_id++;

        struct qmsg req;
        memset(&req, 0, sizeof(req));
        req.mtype = ARBITER_TYPE;
        req.cmd = CMD_ROOM_MSG;
        req.sender_id = cur_id;
        req.msg_id = msg_id;
        req.reply_type = g_reply_type;
        snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 2, rname);

        copy_utf8_safe(req.text, sizeof(req.text), payload);

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


        long eff_id = (rep.msg_id > 0) ? rep.msg_id : msg_id;
        ui_append_outgoing_to(d, eff_id, payload);
        gtk_entry_set_text(GTK_ENTRY(w_msg_entry), "");
        ui_cancel_compose();
        gtk_widget_grab_focus(w_msg_entry);
        return;
    }


    const char *target = g_active_peer;
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

    copy_utf8_safe(chat.text, sizeof(chat.text), payload);

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



    long eff_id = (rep.msg_id > 0) ? rep.msg_id : msg_id;
    ui_append_outgoing_to(d, eff_id, payload);

    record_unacked_id(d, eff_id);
    gtk_entry_set_text(GTK_ENTRY(w_msg_entry), "");
    ui_cancel_compose();

    gtk_widget_grab_focus(w_msg_entry);
}



static void do_send_attachment(int images_only) {
    pthread_mutex_lock(&g_mtx);
    long cur_id = g_my_id;
    char cur_name[MAX_NAME];
    snprintf(cur_name, sizeof(cur_name), "%.*s", MAX_NAME - 1, g_my_name);
    pthread_mutex_unlock(&g_mtx);

    if (cur_id <= 0) {
        ui_append_error("Login first.");
        return;
    }


    if (g_active_peer[0] == '\0') {
        ui_append_error("Select a chat or click '+' to start a new one.");
        return;
    }
    int is_room = g_active_is_room;
    const char *peer = g_active_peer;
    char target[MAX_NAME + 2];
    if (is_room) {
        snprintf(target, sizeof(target), "#%s", peer);
    } else {
        snprintf(target, sizeof(target), "%s", peer);
    }
    if (!is_room && (strcmp(peer, cur_name) == 0 || strcasecmp(peer, "arbiter") == 0)) {
        ui_append_error("Cannot send file to yourself or arbiter.");
        return;
    }

    GtkWidget *dlg = gtk_file_chooser_dialog_new(
        images_only ? "Send Photo" : "Send File",
        GTK_WINDOW(w_window),
        GTK_FILE_CHOOSER_ACTION_OPEN,
        "_Cancel", GTK_RESPONSE_CANCEL,
        "_Send", GTK_RESPONSE_ACCEPT,
        NULL
    );

    if (images_only) {
        GtkFileFilter *img_filter = gtk_file_filter_new();
        gtk_file_filter_set_name(img_filter, "Images (PNG, JPG, GIF, WebP, BMP)");
        gtk_file_filter_add_pixbuf_formats(img_filter);
        gtk_file_chooser_add_filter(GTK_FILE_CHOOSER(dlg), img_filter);

        GtkFileFilter *all_filter = gtk_file_filter_new();
        gtk_file_filter_set_name(all_filter, "All files");
        gtk_file_filter_add_pattern(all_filter, "*");
        gtk_file_chooser_add_filter(GTK_FILE_CHOOSER(dlg), all_filter);
    }

    char *fpath = NULL;
    if (gtk_dialog_run(GTK_DIALOG(dlg)) == GTK_RESPONSE_ACCEPT) {
        fpath = gtk_file_chooser_get_filename(GTK_FILE_CHOOSER(dlg));
    }
    gtk_widget_destroy(dlg);

    if (fpath == NULL) {
        return;
    }

    long fsize = 0;
    char err[64] = "";
    int shmid = shm_alloc_from_file(fpath, &fsize, err, sizeof(err));
    if (shmid < 0) {
        char msg[128];
        snprintf(msg, sizeof(msg), "File error: %s", err);
        ui_append_error(msg);
        g_free(fpath);
        return;
    }

    char fname[64];
    sanitize_filename(fpath, fname, sizeof(fname));
    g_free(fpath);

    long msg_id = g_next_msg_id++;
    drain_type(g_msqid, g_reply_type);

    struct qmsg req;
    memset(&req, 0, sizeof(req));
    req.mtype = ARBITER_TYPE;
    req.cmd = CMD_FILE_SEND;
    req.sender_id = cur_id;
    req.msg_id = msg_id;
    req.reply_type = g_reply_type;
    req.target_id = shmid;
    req.timestamp = fsize;
    snprintf(req.name, sizeof(req.name), "%.*s", MAX_NAME - 1, target);
    snprintf(req.sender_name, sizeof(req.sender_name), "%.*s", MAX_NAME - 1, cur_name);
    snprintf(req.text, sizeof(req.text), "%.*s", MAX_TEXT - 1, fname);

    if (msgsnd(g_msqid, &req, MSG_SIZE, IPC_NOWAIT) < 0) {
        shmctl(shmid, IPC_RMID, NULL);
        ui_append_error(errno == EAGAIN
            ? "Message queue is full, try again."
            : "Failed to send file.");
        return;
    }

    struct qmsg rep;
    if (wait_reply(g_msqid, g_reply_type, &rep, 200) < 0) {
        ui_append_error("No response from arbiter.");
        return;
    }
    rep.text[MAX_TEXT - 1] = '\0';

    if (rep.target_id < 0) {
        char emsg[MAX_TEXT + 32];
        snprintf(emsg, sizeof(emsg), "File send failed: %s", rep.text);
        ui_append_error(emsg);
        return;
    }

    struct dialog_entry *d = get_or_create_dialog(peer, is_room);
    if (d == NULL) {
        return;
    }
    if (is_room) {
        d->joined = 1;
    } else {
        d->online = (rep.target_id > 0);
    }

    if (g_active_is_room != is_room || strcmp(g_active_peer, d->peer) != 0) {
        ui_switch_dialog(d->peer, is_room, 0);
    } else {
        ui_update_chat_header();
    }


    long eff_id = (rep.msg_id > 0) ? rep.msg_id : msg_id;
    ui_append_outgoing_to(d, eff_id, rep.text);
    if (!is_room) {
        record_unacked_id(d, eff_id);
    }
    gtk_widget_grab_focus(w_msg_entry);
}

static void on_menu_attach_photo(GtkMenuItem *item, gpointer data) {
    (void)item;
    (void)data;
    do_send_attachment(1);
}

static void on_menu_attach_file(GtkMenuItem *item, gpointer data) {
    (void)item;
    (void)data;
    do_send_attachment(0);
}

static void on_attach_clicked(GtkWidget *widget, gpointer data) {
    (void)data;
    if (w_attach_menu == NULL) {
        return;
    }
    gtk_widget_show_all(w_attach_menu);
    gtk_menu_popup_at_widget(
        GTK_MENU(w_attach_menu),
        widget,
        GDK_GRAVITY_NORTH_WEST,
        GDK_GRAVITY_SOUTH_WEST,
        NULL
    );
}




static gboolean on_window_configure(GtkWidget *widget, GdkEventConfigure *event, gpointer data) {

    (void)data;

    if (g_manual_font_size > 0 || widget == NULL || event == NULL) {

        return FALSE;

    }

    if (event->window != gtk_widget_get_window(widget)) {

        return FALSE;

    }

    int w = 0;

    int h = 0;
    gtk_window_get_size(GTK_WINDOW(widget), &w, &h);
    if (w <= 0 || h <= 0) {
        return FALSE;
    }
    apply_font_size(compute_auto_font_size(w, h));
    return FALSE;
}

static void on_window_destroy(GtkWidget *widget, gpointer data) {
    (void)widget;
    (void)data;
    w_chat_view = NULL;
    w_chat_header = NULL;

    w_menu_btn = NULL;
    w_menu_popover = NULL;
    w_acct_info_lbl = NULL;
    w_scale_combo = NULL;
    w_chat_menu_btn = NULL;

    w_mi_search = NULL;
    w_search_bar = NULL;
    w_search_entry = NULL;
    w_search_count_lbl = NULL;
    w_mi_leave = NULL;
    w_mi_delete = NULL;
    w_new_chat_btn = NULL;
    w_msg_entry = NULL;
    w_send_btn = NULL;
    w_file_btn = NULL;
    w_theme_combo = NULL;

    w_compose_bar = NULL;
    w_compose_lbl = NULL;
    w_mi_msg_reply = NULL;
    w_mi_msg_edit = NULL;
    w_mi_msg_delete = NULL;
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

    if (g_typing_timer_id > 0) {
        g_source_remove(g_typing_timer_id);
        g_typing_timer_id = 0;
    }
    send_logout();


    if (w_chat_menu != NULL) {
        gtk_widget_destroy(w_chat_menu);
        w_chat_menu = NULL;

    }
    if (w_msg_menu != NULL) {
        gtk_widget_destroy(w_msg_menu);
        w_msg_menu = NULL;
    }
    if (w_attach_menu != NULL) {
        gtk_widget_destroy(w_attach_menu);
        w_attach_menu = NULL;
    }
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

    w_menu_btn = gtk_button_new_with_label("☰");
    gtk_widget_set_tooltip_text(w_menu_btn, "Menu");
    gtk_style_context_add_class(gtk_widget_get_style_context(w_menu_btn), "tab-btn");
    g_signal_connect(w_menu_btn, "clicked", G_CALLBACK(on_menu_btn_clicked), NULL);

    w_menu_popover = gtk_popover_new(w_menu_btn);
    GtkWidget *pop_box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 10);
    gtk_widget_set_size_request(pop_box, 220, -1);

    /* 1. Account info */
    w_acct_info_lbl = gtk_label_new("Not logged in");
    gtk_widget_set_halign(w_acct_info_lbl, GTK_ALIGN_START);
    gtk_style_context_add_class(gtk_widget_get_style_context(w_acct_info_lbl), "acct-info");
    gtk_box_pack_start(GTK_BOX(pop_box), w_acct_info_lbl, FALSE, FALSE, 0);

    gtk_box_pack_start(GTK_BOX(pop_box), gtk_separator_new(GTK_ORIENTATION_HORIZONTAL), FALSE, FALSE, 0);

    /* 2. Scale selector */
    GtkWidget *scale_lbl = gtk_label_new("Scale");
    gtk_widget_set_halign(scale_lbl, GTK_ALIGN_START);
    gtk_box_pack_start(GTK_BOX(pop_box), scale_lbl, FALSE, FALSE, 0);

    w_scale_combo = gtk_combo_box_text_new();
    gtk_combo_box_text_append_text(GTK_COMBO_BOX_TEXT(w_scale_combo), "Auto");
    gtk_combo_box_text_append_text(GTK_COMBO_BOX_TEXT(w_scale_combo), "Small (10pt)");
    gtk_combo_box_text_append_text(GTK_COMBO_BOX_TEXT(w_scale_combo), "Normal (12pt)");
    gtk_combo_box_text_append_text(GTK_COMBO_BOX_TEXT(w_scale_combo), "Large (14pt)");
    gtk_combo_box_text_append_text(GTK_COMBO_BOX_TEXT(w_scale_combo), "XL (16pt)");
    gtk_combo_box_text_append_text(GTK_COMBO_BOX_TEXT(w_scale_combo), "XXL (18pt)");
    gtk_combo_box_set_active(GTK_COMBO_BOX(w_scale_combo), 0);
    g_signal_connect(w_scale_combo, "changed", G_CALLBACK(on_scale_changed), NULL);
    gtk_box_pack_start(GTK_BOX(pop_box), w_scale_combo, FALSE, FALSE, 0);

    /* Theme selector */
    GtkWidget *theme_lbl = gtk_label_new("Theme");
    gtk_widget_set_halign(theme_lbl, GTK_ALIGN_START);
    gtk_box_pack_start(GTK_BOX(pop_box), theme_lbl, FALSE, FALSE, 0);

    w_theme_combo = gtk_combo_box_text_new();
    for (int i = 0; i < THEME_COUNT; i++) {
        gtk_combo_box_text_append_text(GTK_COMBO_BOX_TEXT(w_theme_combo), g_themes[i].name);
    }
    gtk_combo_box_set_active(GTK_COMBO_BOX(w_theme_combo), g_theme_idx);
    g_signal_connect(w_theme_combo, "changed", G_CALLBACK(on_theme_changed), NULL);
    gtk_box_pack_start(GTK_BOX(pop_box), w_theme_combo, FALSE, FALSE, 0);

    gtk_box_pack_start(GTK_BOX(pop_box), gtk_separator_new(GTK_ORIENTATION_HORIZONTAL), FALSE, FALSE, 0);

    /* 3. Logout */
    w_logout_btn = gtk_button_new_with_label("Logout");
    gtk_style_context_add_class(gtk_widget_get_style_context(w_logout_btn), "leave-btn");
    g_signal_connect(w_logout_btn, "clicked", G_CALLBACK(on_logout_clicked), NULL);
    gtk_box_pack_start(GTK_BOX(pop_box), w_logout_btn, FALSE, FALSE, 0);

    gtk_widget_show_all(pop_box);
    gtk_container_add(GTK_CONTAINER(w_menu_popover), pop_box);

    w_status_lbl = gtk_label_new("DeepSpeak");
    gtk_widget_set_halign(w_status_lbl, GTK_ALIGN_START);
    gtk_box_pack_start(GTK_BOX(top_bar), w_menu_btn, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(top_bar), w_status_lbl, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(root), top_bar, FALSE, FALSE, 0);

    GtkWidget *mid_box = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 10);

    GtkWidget *chat_col = gtk_box_new(GTK_ORIENTATION_VERTICAL, 6);


    GtkWidget *header_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);
    w_chat_header = gtk_label_new("★ System");
    gtk_widget_set_halign(w_chat_header, GTK_ALIGN_START);

    w_chat_menu = gtk_menu_new();

    w_mi_search = gtk_menu_item_new_with_label("🔍  Search");
    g_signal_connect(w_mi_search, "activate", G_CALLBACK(on_search_menu_activate), NULL);
    gtk_menu_shell_append(GTK_MENU_SHELL(w_chat_menu), w_mi_search);
    w_mi_leave = gtk_menu_item_new_with_label("Leave Room");
    w_mi_delete = gtk_menu_item_new_with_label("Delete Chat");
    g_signal_connect(w_mi_leave, "activate", G_CALLBACK(on_leave_room_clicked), NULL);
    g_signal_connect(w_mi_delete, "activate", G_CALLBACK(on_delete_clicked), NULL);
    gtk_menu_shell_append(GTK_MENU_SHELL(w_chat_menu), w_mi_leave);
    gtk_menu_shell_append(GTK_MENU_SHELL(w_chat_menu), w_mi_delete);

    w_chat_menu_btn = gtk_button_new_with_label("⋮");
    gtk_widget_set_tooltip_text(w_chat_menu_btn, "Chat options");
    gtk_style_context_add_class(gtk_widget_get_style_context(w_chat_menu_btn), "tab-btn");
    g_signal_connect(w_chat_menu_btn, "clicked", G_CALLBACK(on_chat_menu_clicked), NULL);

    gtk_box_pack_start(GTK_BOX(header_bar), w_chat_header, TRUE, TRUE, 0);
    gtk_box_pack_end(GTK_BOX(header_bar), w_chat_menu_btn, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(chat_col), header_bar, FALSE, FALSE, 0);

    w_search_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);

    w_search_entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(w_search_entry), "Search in chat...");
    g_signal_connect(w_search_entry, "changed", G_CALLBACK(on_search_entry_changed), NULL);
    g_signal_connect(w_search_entry, "activate", G_CALLBACK(on_search_prev_clicked), NULL);
    g_signal_connect(w_search_entry, "key-press-event", G_CALLBACK(on_search_entry_key_press), NULL);

    w_search_count_lbl = gtk_label_new("");

    GtkWidget *s_prev = gtk_button_new_with_label("▲");
    gtk_widget_set_tooltip_text(s_prev, "Previous match");
    gtk_style_context_add_class(gtk_widget_get_style_context(s_prev), "tab-btn");
    g_signal_connect(s_prev, "clicked", G_CALLBACK(on_search_prev_clicked), NULL);

    GtkWidget *s_next = gtk_button_new_with_label("▼");
    gtk_widget_set_tooltip_text(s_next, "Next match");
    gtk_style_context_add_class(gtk_widget_get_style_context(s_next), "tab-btn");
    g_signal_connect(s_next, "clicked", G_CALLBACK(on_search_next_clicked), NULL);

    GtkWidget *s_close = gtk_button_new_with_label("✕");
    gtk_widget_set_tooltip_text(s_close, "Close search");
    gtk_style_context_add_class(gtk_widget_get_style_context(s_close), "tab-btn");
    g_signal_connect(s_close, "clicked", G_CALLBACK(on_search_close_clicked), NULL);

    gtk_box_pack_start(GTK_BOX(w_search_bar), w_search_entry, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(w_search_bar), w_search_count_lbl, FALSE, FALSE, 4);
    gtk_box_pack_start(GTK_BOX(w_search_bar), s_prev, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(w_search_bar), s_next, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(w_search_bar), s_close, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(chat_col), w_search_bar, FALSE, FALSE, 0);

    GtkWidget *scroll = gtk_scrolled_window_new(NULL, NULL);
    gtk_scrolled_window_set_policy(GTK_SCROLLED_WINDOW(scroll), GTK_POLICY_AUTOMATIC, GTK_POLICY_AUTOMATIC);

    w_chat_view = gtk_text_view_new_with_buffer(g_sys_buf);
    gtk_text_view_set_editable(GTK_TEXT_VIEW(w_chat_view), FALSE);
    gtk_text_view_set_cursor_visible(GTK_TEXT_VIEW(w_chat_view), FALSE);
    gtk_text_view_set_wrap_mode(GTK_TEXT_VIEW(w_chat_view), GTK_WRAP_WORD_CHAR);

    g_signal_connect(w_chat_view, "button-press-event", G_CALLBACK(on_chat_view_button_press), NULL);
    gtk_text_view_set_left_margin(GTK_TEXT_VIEW(w_chat_view), 10);
    gtk_text_view_set_right_margin(GTK_TEXT_VIEW(w_chat_view), 10);
    gtk_text_view_set_top_margin(GTK_TEXT_VIEW(w_chat_view), 8);
    gtk_text_view_set_bottom_margin(GTK_TEXT_VIEW(w_chat_view), 8);

    gtk_container_add(GTK_CONTAINER(scroll), w_chat_view);
    gtk_box_pack_start(GTK_BOX(chat_col), scroll, TRUE, TRUE, 0);

    w_msg_menu = gtk_menu_new();
    w_mi_msg_reply = gtk_menu_item_new_with_label("↩  Reply");
    w_mi_msg_edit = gtk_menu_item_new_with_label("✏  Edit");
    w_mi_msg_delete = gtk_menu_item_new_with_label("🗑  Delete");
    g_signal_connect(w_mi_msg_reply, "activate", G_CALLBACK(on_msg_reply_activate), NULL);
    g_signal_connect(w_mi_msg_edit, "activate", G_CALLBACK(on_msg_edit_activate), NULL);
    g_signal_connect(w_mi_msg_delete, "activate", G_CALLBACK(on_msg_delete_activate), NULL);
    gtk_menu_shell_append(GTK_MENU_SHELL(w_msg_menu), w_mi_msg_reply);
    gtk_menu_shell_append(GTK_MENU_SHELL(w_msg_menu), w_mi_msg_edit);
    gtk_menu_shell_append(GTK_MENU_SHELL(w_msg_menu), w_mi_msg_delete);
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



    w_compose_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 6);
    gtk_style_context_add_class(gtk_widget_get_style_context(w_compose_bar), "compose-bar");
    w_compose_lbl = gtk_label_new("");
    gtk_widget_set_halign(w_compose_lbl, GTK_ALIGN_START);
    gtk_label_set_ellipsize(GTK_LABEL(w_compose_lbl), PANGO_ELLIPSIZE_END);
    GtkWidget *c_close = gtk_button_new_with_label("✕");
    gtk_style_context_add_class(gtk_widget_get_style_context(c_close), "tab-btn");
    g_signal_connect(c_close, "clicked", G_CALLBACK(on_compose_cancel_clicked), NULL);
    gtk_box_pack_start(GTK_BOX(w_compose_bar), w_compose_lbl, TRUE, TRUE, 0);
    gtk_box_pack_end(GTK_BOX(w_compose_bar), c_close, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(root), w_compose_bar, FALSE, FALSE, 0);
    GtkWidget *bottom_bar = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 8);

    w_new_chat_btn = gtk_button_new_with_label("+");
    gtk_widget_set_tooltip_text(w_new_chat_btn, "New chat or #room");
    gtk_style_context_add_class(gtk_widget_get_style_context(w_new_chat_btn), "tab-btn");
    g_signal_connect(w_new_chat_btn, "clicked", G_CALLBACK(on_new_chat_clicked), NULL);

    w_attach_menu = gtk_menu_new();
    GtkWidget *mi_photo = gtk_menu_item_new_with_label("📷  Photo");
    GtkWidget *mi_file = gtk_menu_item_new_with_label("📄  File");
    g_signal_connect(mi_photo, "activate", G_CALLBACK(on_menu_attach_photo), NULL);
    g_signal_connect(mi_file, "activate", G_CALLBACK(on_menu_attach_file), NULL);
    gtk_menu_shell_append(GTK_MENU_SHELL(w_attach_menu), mi_photo);
    gtk_menu_shell_append(GTK_MENU_SHELL(w_attach_menu), mi_file);

    w_file_btn = gtk_button_new_with_label("📎");
    gtk_widget_set_tooltip_text(w_file_btn, "Attach photo or file");
    gtk_style_context_add_class(gtk_widget_get_style_context(w_file_btn), "tab-btn");
    g_signal_connect(w_file_btn, "clicked", G_CALLBACK(on_attach_clicked), NULL);

    w_msg_entry = gtk_entry_new();
    gtk_entry_set_placeholder_text(GTK_ENTRY(w_msg_entry), "Select a chat or click '+' to start...");
    gtk_entry_set_max_length(GTK_ENTRY(w_msg_entry), MAX_TEXT - 1);
    g_signal_connect(w_msg_entry, "activate", G_CALLBACK(on_send_clicked), NULL);

    g_signal_connect(w_msg_entry, "changed", G_CALLBACK(on_msg_entry_changed), NULL);

    g_signal_connect(w_msg_entry, "key-press-event", G_CALLBACK(on_msg_entry_key_press), NULL);

    w_send_btn = gtk_button_new_with_label("Send");
    g_signal_connect(w_send_btn, "clicked", G_CALLBACK(on_send_clicked), NULL);

    gtk_box_pack_start(GTK_BOX(bottom_bar), w_new_chat_btn, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(bottom_bar), w_file_btn, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(bottom_bar), w_msg_entry, TRUE, TRUE, 0);
    gtk_box_pack_start(GTK_BOX(bottom_bar), w_send_btn, FALSE, FALSE, 0);
    gtk_box_pack_start(GTK_BOX(root), bottom_bar, FALSE, FALSE, 0);
    gtk_stack_add_named(GTK_STACK(w_main_stack), root, "chat");

    apply_theme(g_theme_idx);
    gtk_widget_show_all(w_window);
    ui_select_sidebar_tab("users");
    ui_set_connected_state(0, "", 0, 0);

    g_typing_timer_id = g_timeout_add(1000, ui_typing_tick, NULL);
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