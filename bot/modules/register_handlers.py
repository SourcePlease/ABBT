"""
register_handlers.py

Registers all commands on batch_bot (Bot 2) and movie_bot (Bot 3).
Bot 1 (bot) already has all handlers via plugins=dict(root='bot/modules').

Bot 1 (Ongoing)  — all shared + genlink, batch, schedule
Bot 2 (Completed)— all shared + genlink, batch
Bot 3 (Movies)   — all shared + genlink
"""

from pyrogram import filters
from pyrogram.handlers import (
    MessageHandler as MH,
    CallbackQueryHandler as CBH,
    ChatJoinRequestHandler as CJRH,
    ChatMemberUpdatedHandler as CMUH,
)
from pyrogram.filters import command, private

from bot import Var, admin


def _cmd(names):
    return command(names) & private


def _owner_filter():
    return filters.user(Var.OWNER_ID)


def _build_shared_handlers():
    """Build the list of handlers shared across all 3 bots."""

    # ── cmds.py ───────────────────────────────────────────────────────────────
    # /dlt_time was removed; the auto-delete timer is now set via the
    # /settings → ⏱ Auto Delete sub-menu (see bot/modules/settings.py).
    # /pause /resume /reboot /clearqueue /queue (and all queue_* callbacks)
    # were removed in favour of /settings → 📊 Dashboard & Queue
    # (see bot/modules/dashboard.py).
    # /addlink /addtask /rtask /addmagnet were removed in favour of
    # /settings → 📋 RSS & Manual Tasks (see bot/modules/rss_tasks.py).
    # /connect, /connectchannel, /removeconnection, /removechannel removed —
    # the connect / remove flow is now under /settings → 📺 Channel Management
    # (see bot/modules/channel_manager.py). The step-2/3/4 callback handlers
    # below are still imported because the inline flow re-uses them after
    # the panel's input phase.
    from bot.modules.cmds import (
        start_msg, close_cb,
        connect_step2_anime_name, connect_step3_pick_anime,
        pc_searchagain, pc_dbtype_cb, pc_skip_cb, pc_upall_cb,
        pc_upseason_cb, pc_upseason_do_cb, pc_upseason_page_cb, pc_upep_cb,
        pc_upmovie_all_cb, pc_upmovie_pick_cb, pc_upmovie_page_cb,
        pc_upmovie_single_cb, pc_remove_cb, pc_confirm_remove_cb,
        pc_anipg_cb, noop_cb,
        send_log,
    )

    # ── broadcast.py ─────────────────────────────────────────────────────────
    from bot.modules.broadcast import (
        delete_broadcast, send_pin_text, send_text,
    )

    # ── settings.py (replaces /ban /unban /banlist /add_admin /deladmin
    #     /admins and /users — those text commands no longer exist) ──────────
    from bot.modules.settings import (
        settings_command, settings_callbacks, settings_input_catcher,
        has_pending_settings_input, SETTINGS_INPUT_FILTER,
    )

    # ── fsub.py ───────────────────────────────────────────────────────────────
    # /addchnl /delchnl /listchnl /fsub_mode have been folded into /settings
    # → Force Sub (see bot/modules/settings.py). Only the user-facing refresh
    # callback is still wired here.
    from bot.modules.fsub import refresh_fsub_callback

    # ── force_subscription.py ─────────────────────────────────────────────────
    # /fsub_help, /fsubstats and /clearlogs (join-request logs) were removed.
    # The whole module file was deleted. Force-sub admin tooling now lives
    # under /settings → 📡 Force Sub (see bot/modules/settings.py).

    # ── useless.py ────────────────────────────────────────────────────────────
    # /check_dlt_time was removed; see /settings → ⏱ Auto Delete instead.
    from bot.modules.useless import help_command, help_callbacks

    # ── index.py ──────────────────────────────────────────────────────────────
    from bot.modules.index import index_command, uindex_command

    # ── channel_manager.py ────────────────────────────────────────────────────
    # /listconnections removed — list now lives under /settings → 📺 Channel
    # Management. The acm_* callback handlers are still wired because the
    # inline anime browser keyboard (paginated list) uses them.
    from bot.modules.channel_manager import (
        acm_page, acm_info, acm_back,
        acm_remove, acm_confirm_remove, acm_upload_all,
        acm_upmovie_all, acm_upmovie_pick, acm_upmovie_page,
        acm_upmovie_single, acm_pick_season, acm_upload_season,
        acm_season_page, acm_upload_episode, acm_ep_reply,
    )

    # ── status.py ─────────────────────────────────────────────────────────────
    # /status was removed; the live dashboard now lives under /settings →
    # 📊 Dashboard & Queue (see bot/modules/dashboard.py).
    # /processbatch /retryfailed /cleantasks /processpending were removed; the
    # logic now lives under /settings → 📋 RSS & Manual Tasks
    # (see bot/modules/rss_tasks.py). The status.py file is now an empty stub.

    # ── dev.py ────────────────────────────────────────────────────────────────
    from bot.modules.dev import (
        shell_handler, eval_handler, stats_handler, update_handler,
        downloads_handler, downloads_browse, downloads_back,
        downloads_delete, dl_noop,
        fixbatchlink_handler, fixbatchlink_reply,
        importusers_handler,
    )

    _owner = _owner_filter()

    # Text filter for connect step 2 (catches anime name input).
    # /connectchannel and /removechannel are aliases — they MUST be in the
    # exclude list otherwise typing /connectchannel would be misread as the
    # anime-name input for an in-flight connect flow.
    _connect_step2_filter = (
        filters.private & admin & filters.text & ~filters.command([
            "start","help","shell","eval","stats","update","downloads",
            "fixbatchlink","restart","batch","genlink","settings",
            "set","index",
            "connect","connectchannel","listconnections","removeconnection",
            "removechannel",
            "cancel","broadcast","fsub","adfsub","rmfsub",
            "listfsub",
            "schedule","log","importusers",
        ])
    )

    # Text filter for fixbatchlink reply
    _fixbatch_reply_filter = (
        filters.private & _owner & filters.text & ~filters.command([
            "start","help","shell","eval","stats","update","downloads",
            "fixbatchlink","restart","batch","genlink","settings",
            "set","index",
            "connect","listconnections","removeconnection",
            "cancel","broadcast","fsub","adfsub","rmfsub",
            "listfsub","schedule","log","importusers",
        ])
    )

    # The /settings input filter is defined in bot/modules/settings.py
    # (SETTINGS_INPUT_FILTER) and re-used here so Bot 2 / Bot 3 share the
    # exact same exclude list as Bot 1. The handler short-circuits when the
    # user has no pending state.
    _settings_input_filter = SETTINGS_INPUT_FILTER

    return [
        # ── User ──────────────────────────────────────────────────────────────
        MH(start_msg,                _cmd("start")),

        # ── Settings panel (replaces /ban /unban /banlist /add_admin /deladmin
        #     /admins and /users) ────────────────────────────────────────────
        MH(settings_command,         _cmd("settings") & admin),
        # group=2 to coexist with the connect-step2 text catcher; the
        # handler short-circuits when the user has no pending settings state.
        (MH(settings_input_catcher,  _settings_input_filter), 3),

        # ── Broadcast ─────────────────────────────────────────────────────────
        MH(delete_broadcast,         _cmd("dbroadcast") & _owner),
        MH(send_pin_text,            _cmd("pbroadcast") & _owner),
        MH(send_text,                _cmd("broadcast") & _owner),

        # ── FSub ──────────────────────────────────────────────────────────────
        # /addchnl /delchnl /listchnl /fsub_mode now live under /settings → Force Sub.
        # /fsub_help, /fsubstats, /clearlogs (join-request logs) were removed.
        # /dlt_time and /check_dlt_time were removed too — Auto Delete is
        # now a /settings sub-menu.

        # ── Help ──────────────────────────────────────────────────────────────
        MH(help_command,             _cmd("help") & admin),

        # ── Index ─────────────────────────────────────────────────────────────
        MH(index_command,            _cmd("index") & admin),
        MH(uindex_command,           _cmd("uindex") & admin),

        # ── Connect channel flow ──────────────────────────────────────────────
        # /connect, /connectchannel, /removeconnection, /removechannel and
        # /listconnections were removed. The flow is now driven from
        # /settings → 📺 Channel Management. The step-2 text catcher is
        # still wired here because once the panel hands off to step 1,
        # the user types the anime name as a plain message which step-2
        # picks up (DB pending_connection state, group=2).
        (MH(connect_step2_anime_name, _connect_step2_filter), 2),
        (MH(acm_ep_reply,             filters.private & admin), 1),

        # ── Status & queue ────────────────────────────────────────────────────
        # /status, /queue, /clearqueue, /pause, /resume, /reboot all moved
        # into /settings → 📊 Dashboard & Queue (see bot/modules/dashboard.py).
        # /processbatch, /retryfailed, /cleantasks, /processpending, /addlink,
        # /addtask, /rtask, /addmagnet all moved into /settings → 📋 RSS &
        # Manual Tasks (see bot/modules/rss_tasks.py).
        MH(send_log,                 _cmd("log") & admin),

        # ── Dev / Owner ───────────────────────────────────────────────────────
        MH(stats_handler,            _cmd("stats") & _owner),
        MH(importusers_handler,      _cmd("importusers") & _owner),
        MH(shell_handler,            _cmd("shell") & _owner),
        MH(eval_handler,             _cmd("eval") & _owner),
        MH(update_handler,           _cmd("update") & _owner),
        MH(downloads_handler,        _cmd("downloads") & _owner),
        MH(fixbatchlink_handler,     _cmd("fixbatchlink") & _owner),
        MH(fixbatchlink_reply,       _fixbatch_reply_filter),

        # ── Callbacks ─────────────────────────────────────────────────────────
        CBH(help_callbacks,                filters.regex(r"^help:")),
        CBH(settings_callbacks,            filters.regex(r"^s:")),
        CBH(close_cb,                      filters.regex(r"^close$")),
        CBH(refresh_fsub_callback,         filters.regex("refresh_fsub")),
        # fsub_toggle_ / fsub_back / refresh_channel_list moved into /settings
        # status_refresh / queue_refresh / queue_pick / queue_act /
        # queue_clear_all_do moved into /settings → 📊 Dashboard & Queue
        # (handled by bot.modules.settings.settings_callbacks → dashboard.py).
        CBH(downloads_browse,              filters.regex(r"^dlb:")),
        CBH(downloads_back,                filters.regex(r"^dlu:")),
        CBH(downloads_delete,              filters.regex(r"^dld:")),
        CBH(dl_noop,                       filters.regex(r"^dl_noop$")),
        CBH(pc_anipg_cb,                   filters.regex(r"^pc_anipg\|")),
        CBH(noop_cb,                       filters.regex(r"^noop$")),
        CBH(pc_searchagain,                filters.regex(r"^pc_searchagain$")),
        CBH(connect_step3_pick_anime,      filters.regex(r"^pc_pickani\|")),
        CBH(pc_dbtype_cb,                  filters.regex(r"^pc_dbtype\|")),
        CBH(pc_skip_cb,                    filters.regex(r"^pc_skip\|")),
        CBH(pc_upall_cb,                   filters.regex(r"^pc_upall\|")),
        CBH(pc_upseason_cb,                filters.regex(r"^pc_upseason\|")),
        CBH(pc_upseason_do_cb,             filters.regex(r"^pc_upseason_do\|.*\|\d+$")),
        CBH(pc_upseason_page_cb,           filters.regex(r"^pc_upseason_do\|.*_page\|\d+$")),
        CBH(pc_upep_cb,                    filters.regex(r"^pc_upep\|")),
        CBH(pc_upmovie_all_cb,             filters.regex(r"^pc_upmovie_all\|")),
        CBH(pc_upmovie_pick_cb,            filters.regex(r"^pc_upmovie_pick\|")),
        CBH(pc_upmovie_page_cb,            filters.regex(r"^pc_upmovie_page\|")),
        CBH(pc_upmovie_single_cb,          filters.regex(r"^pc_upmovie_single\|")),
        CBH(pc_remove_cb,                  filters.regex(r"^pc_remove\|")),
        CBH(pc_confirm_remove_cb,          filters.regex(r"^pc_confirm_remove\|")),
        CBH(acm_page,                      filters.regex(r"^acm_page\|(\d+)$")),
        CBH(acm_info,                      filters.regex(r"^acm_info\|")),
        CBH(acm_back,                      filters.regex(r"^acm_back\|")),
        CBH(acm_remove,                    filters.regex(r"^acm_remove\|")),
        CBH(acm_confirm_remove,            filters.regex(r"^acm_confirm_remove\|")),
        CBH(acm_upload_all,                filters.regex(r"^acm_upall\|")),
        CBH(acm_upmovie_all,               filters.regex(r"^acm_upmovie_all\|")),
        CBH(acm_upmovie_pick,              filters.regex(r"^acm_upmovie_pick\|")),
        CBH(acm_upmovie_page,              filters.regex(r"^acm_upmovie_page\|")),
        CBH(acm_upmovie_single,            filters.regex(r"^acm_upmovie_single\|")),
        CBH(acm_pick_season,               filters.regex(r"^acm_pickseason\|")),
        CBH(acm_upload_season,             filters.regex(r"^acm_upseason\|.*\|\d+$")),
        CBH(acm_season_page,               filters.regex(r"^acm_upseason\|.*_page\|\d+$")),
        CBH(acm_upload_episode,            filters.regex(r"^acm_upep\|")),
    ]


async def register_all(batch_bot, movie_bot):
    """
    Register all handlers on Bot 2 and Bot 3.
    Called from __main__.py after all bots are started.
    """
    from bot import bot
    # REQUEST-MODE FIX: handle_join_request and handle_chat_members are decorated
    # with @bot.on_*() in bot/__init__.py, so they only fire for the main bot's
    # update stream. If a force-sub channel has only batch_bot or movie_bot as
    # admin (a common setup when the three bots have different roles), join
    # requests landed in those channels never reached the database — meaning
    # request-mode users were forever stuck on "❌ Not Requested" no matter how
    # many times they hit the join-request link. We re-attach the same two
    # handlers to every additional bot so every join request is recorded.
    from bot import handle_join_request, handle_chat_members

    shared = _build_shared_handlers()

    def _add_all(client, handlers):
        """Add handlers to client, supporting (handler, group) tuples."""
        for h in handlers:
            if isinstance(h, tuple):
                client.add_handler(h[0], group=h[1])
            else:
                client.add_handler(h)

    def _add_fsub_event_handlers(client):
        """Mirror the on_chat_join_request + on_chat_member_updated handlers
        from the main bot onto a secondary bot client."""
        client.add_handler(CJRH(handle_join_request))
        client.add_handler(CMUH(handle_chat_members))

    # ── Bot 2 (Completed / BDRip batch) ──────────────────────────────────────
    if batch_bot and batch_bot is not bot:
        from bot.modules.cmds import genlink_cmd, batch_cmd
        _add_all(batch_bot, shared)
        batch_bot.add_handler(MH(genlink_cmd, _cmd("genlink") & admin))
        batch_bot.add_handler(MH(batch_cmd,   _cmd("batch") & admin))
        _add_fsub_event_handlers(batch_bot)

    # ── Bot 3 (Movies) ────────────────────────────────────────────────────────
    if movie_bot and movie_bot is not bot:
        from bot.modules.cmds import genlink_cmd
        _add_all(movie_bot, shared)
        movie_bot.add_handler(MH(genlink_cmd, _cmd("genlink") & admin))
        _add_fsub_event_handlers(movie_bot)
