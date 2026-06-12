import asyncio
from pyrogram.filters import command, private, user
from pyrogram.errors import FloodWait, UserIsBlocked, InputUserDeactivated, PeerIdInvalid

from bot import bot, Var
from bot.core.database import db
from bot.core.func_utils import sendMessage, new_task

# Max concurrent sends — high enough to be fast, low enough to avoid Telegram flood
BROADCAST_CONCURRENCY = 25


async def _send_one(broadcast_msg, chat_id: int, counters: dict, semaphore: asyncio.Semaphore):
    """Send a single broadcast message. Updates counters in-place."""
    async with semaphore:
        try:
            sent = await broadcast_msg.copy(chat_id)
            counters["successful"] += 1
            return chat_id, sent.id
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                sent = await broadcast_msg.copy(chat_id)
                counters["successful"] += 1
                return chat_id, sent.id
            except Exception:
                counters["unsuccessful"] += 1
                return chat_id, None
        except UserIsBlocked:
            await db.del_user(chat_id)
            counters["blocked"] += 1
            return chat_id, None
        except InputUserDeactivated:
            await db.del_user(chat_id)
            counters["deleted"] += 1
            return chat_id, None
        except PeerIdInvalid:
            await db.del_user(chat_id)
            counters["deleted"] += 1
            return chat_id, None
        except Exception:
            counters["unsuccessful"] += 1
            return chat_id, None


async def _send_and_pin_one(client, broadcast_msg, chat_id: int, counters: dict, semaphore: asyncio.Semaphore):
    """Send and pin a single message."""
    async with semaphore:
        try:
            sent = await broadcast_msg.copy(chat_id)
            await client.pin_chat_message(chat_id=chat_id, message_id=sent.id, both_sides=True)
            counters["successful"] += 1
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                sent = await broadcast_msg.copy(chat_id)
                await client.pin_chat_message(chat_id=chat_id, message_id=sent.id, both_sides=True)
                counters["successful"] += 1
            except Exception:
                counters["unsuccessful"] += 1
        except UserIsBlocked:
            await db.del_user(chat_id)
            counters["blocked"] += 1
        except InputUserDeactivated:
            await db.del_user(chat_id)
            counters["deleted"] += 1
        except PeerIdInvalid:
            await db.del_user(chat_id)
            counters["deleted"] += 1
        except Exception:
            counters["unsuccessful"] += 1


async def _progress_updater(pls_wait, counters: dict, total: int, stop_event: asyncio.Event):
    """Edit the status message every 5 seconds until stop_event is set."""
    while not stop_event.is_set():
        await asyncio.sleep(5)
        done = counters["successful"] + counters["blocked"] + counters["deleted"] + counters["unsuccessful"]
        pct = done / total if total else 0
        filled = int(pct * 20)
        bar = "█" * filled + "░" * (20 - filled)
        try:
            await pls_wait.edit(
                f"<i>📤 Broadcasting...</i>\n"
                f"<code>[{bar}]</code> {pct*100:.1f}%\n"
                f"<code>{done}/{total}</code> users done"
            )
        except Exception:
            pass


@bot.on_message(command('broadcast') & private & user(Var.OWNER_ID))
@new_task
async def send_text(client, message):
    if not message.reply_to_message:
        msg = await message.reply(Var.REPLY_ERROR)
        await asyncio.sleep(8)
        await msg.delete()
        return

    query        = await db.full_userbase()
    broadcast_msg = message.reply_to_message
    total        = len(query)
    counters     = {"successful": 0, "blocked": 0, "deleted": 0, "unsuccessful": 0}
    semaphore    = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    stop_event   = asyncio.Event()

    pls_wait = await message.reply(f"<i>📤 Starting broadcast to {total} users...</i>")

    progress_task = asyncio.create_task(
        _progress_updater(pls_wait, counters, total, stop_event)
    )

    tasks = [_send_one(broadcast_msg, chat_id, counters, semaphore) for chat_id in query]
    await asyncio.gather(*tasks)

    stop_event.set()
    await progress_task

    await pls_wait.edit(
        f"<b><u>Broadcast Completed</u></b>\n\n"
        f"<b>Total Users:</b> <code>{total}</code>\n"
        f"<b>Successful:</b> <code>{counters['successful']}</code>\n"
        f"<b>Blocked:</b> <code>{counters['blocked']}</code>\n"
        f"<b>Deleted Accounts:</b> <code>{counters['deleted']}</code>\n"
        f"<b>Failed:</b> <code>{counters['unsuccessful']}</code>"
    )


@bot.on_message(command('dbroadcast') & private & user(Var.OWNER_ID))
@new_task
async def delete_broadcast(client, message):
    """
    Reply to a message + /dbroadcast <seconds>
    Sends to ALL users concurrently, then deletes after the duration.
    """
    if not message.reply_to_message:
        msg = await message.reply("<b>Reply to a message to broadcast it with auto-delete.</b>")
        await asyncio.sleep(8)
        await msg.delete()
        return

    try:
        duration = int(message.command[1])
    except (IndexError, ValueError):
        await message.reply("<b>Usage:</b> /dbroadcast &lt;seconds&gt;\nExample: /dbroadcast 3600")
        return

    query         = await db.full_userbase()
    broadcast_msg = message.reply_to_message
    total         = len(query)
    counters      = {"successful": 0, "blocked": 0, "deleted": 0, "unsuccessful": 0}
    semaphore     = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    stop_event    = asyncio.Event()

    pls_wait = await message.reply(f"<i>📤 Starting broadcast to {total} users...</i>")

    progress_task = asyncio.create_task(
        _progress_updater(pls_wait, counters, total, stop_event)
    )

    # ── Phase 1: Send concurrently ────────────────────────────────────────
    tasks   = [_send_one(broadcast_msg, chat_id, counters, semaphore) for chat_id in query]
    results = await asyncio.gather(*tasks)

    stop_event.set()
    await progress_task

    # Build sent_ids map from results
    sent_ids = {chat_id: msg_id for chat_id, msg_id in results if msg_id is not None}

    await pls_wait.edit(
        f"<b>✅ Sent to {counters['successful']}/{total} users.</b>\n"
        f"<i>⏳ Will delete in {duration}s ({duration//60}m {duration%60}s)...</i>"
    )

    # ── Phase 2: Wait, then delete concurrently ───────────────────────────
    await asyncio.sleep(duration)

    del_counters = {"success": 0, "fail": 0}
    del_sem      = asyncio.Semaphore(BROADCAST_CONCURRENCY)

    async def _delete_one(chat_id, msg_id):
        async with del_sem:
            try:
                await client.delete_messages(chat_id, msg_id)
                del_counters["success"] += 1
            except Exception:
                del_counters["fail"] += 1

    await asyncio.gather(*[_delete_one(cid, mid) for cid, mid in sent_ids.items()])

    await pls_wait.edit(
        f"<b><u>Broadcast with Auto-Delete Completed</u></b>\n\n"
        f"<b>Total Users:</b> <code>{total}</code>\n"
        f"<b>Sent:</b> <code>{counters['successful']}</code>\n"
        f"<b>Deleted:</b> <code>{del_counters['success']}</code>\n"
        f"<b>Blocked:</b> <code>{counters['blocked']}</code>\n"
        f"<b>Deleted Accounts:</b> <code>{counters['deleted']}</code>\n"
        f"<b>Failed:</b> <code>{counters['unsuccessful']}</code>"
    )


@bot.on_message(command('pbroadcast') & private & user(Var.OWNER_ID))
@new_task
async def send_pin_text(client, message):
    if not message.reply_to_message:
        msg = await message.reply("<b>Reply to a message to broadcast and pin it.</b>")
        await asyncio.sleep(8)
        await msg.delete()
        return

    query         = await db.full_userbase()
    broadcast_msg = message.reply_to_message
    total         = len(query)
    counters      = {"successful": 0, "blocked": 0, "deleted": 0, "unsuccessful": 0}
    semaphore     = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    stop_event    = asyncio.Event()

    pls_wait = await message.reply(f"<i>📤 Starting pin-broadcast to {total} users...</i>")

    progress_task = asyncio.create_task(
        _progress_updater(pls_wait, counters, total, stop_event)
    )

    tasks = [_send_and_pin_one(client, broadcast_msg, chat_id, counters, semaphore) for chat_id in query]
    await asyncio.gather(*tasks)

    stop_event.set()
    await progress_task

    await pls_wait.edit(
        f"<b><u>Pin Broadcast Completed</u></b>\n\n"
        f"<b>Total Users:</b> <code>{total}</code>\n"
        f"<b>Successful:</b> <code>{counters['successful']}</code>\n"
        f"<b>Blocked:</b> <code>{counters['blocked']}</code>\n"
        f"<b>Deleted Accounts:</b> <code>{counters['deleted']}</code>\n"
        f"<b>Failed:</b> <code>{counters['unsuccessful']}</code>"
    )
