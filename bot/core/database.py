from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta
import re
import time
from bot.core.reporter import rep


class Database:
    def __init__(self):
        self.client = None
        self.db = None
        # FIX #10: Track whether the seen_torrents TTL index has been created so
        # we only issue create_index() once at connection time, not on every single
        # mark_torrent_seen() call (which could be hundreds of times per minute).
        self._ttl_index_created = False

    async def connect(self):
        """Connect to MongoDB."""
        try:
            from bot import Var
            self.client = AsyncIOMotorClient(Var.MONGO_URI)
            self.db = self.client[Var.DB_NAME]
            await self.db.command("ping")
            # FIX #10: Create TTL index exactly once, here at connection time.
            await self._ensure_ttl_index()
            await rep.report(f"MongoDB connected ({Var.DB_NAME})", "info", log=False)
            return True
        except Exception as e:
            await rep.report(f"MongoDB connection error: {str(e)}", "error")
            return False

    async def _ensure_ttl_index(self):
        """Create the seen_torrents TTL index once. Safe to call multiple times."""
        if self._ttl_index_created:
            return
        try:
            await self.db.seen_torrents.create_index(
                "expires_at", expireAfterSeconds=0, background=True
            )
            # Performance indexes on hot query paths
            await self.db.users.create_index("user_id", unique=True, background=True)
            await self.db.admins.create_index("user_id", unique=True, background=True)
            await self.db.anime_data.create_index(
                [("anime_id", 1), ("episode_number", 1), ("quality_key", 1)],
                background=True
            )
            await self.db.anime_channels.create_index("anime_name", background=True)
            await self.db.anime_channels.create_index("ani_id", background=True, sparse=True)
            await self.db.batch_links.create_index("ani_id", background=True)
            await self.db.batch_links.create_index("torrent_name", background=True, sparse=True)
            await self.db.task_queue.create_index(
                [("status", 1), ("source_priority", 1), ("created_at", 1)],
                background=True
            )
            self._ttl_index_created = True
        except Exception:
            pass  # non-critical — indexes improve speed but don't break functionality

    async def disconnect(self):
        """Disconnect from MongoDB."""
        if self.client:
            self.client.close()

    # ── USER MANAGEMENT ───────────────────────────────────────────────────────

    async def add_user(self, user_id, username=None, first_name=None, last_name=None):
        """Add or update user. date_joined is only set on first insert, never overwritten."""
        try:
            if self.db is None:
                await self.connect()
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            await self.db.users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "username":   username,
                        "first_name": first_name,
                        "last_name":  last_name,
                    },
                    "$setOnInsert": {
                        "user_id":     user_id,
                        "date_joined": current_time,
                        "is_banned":   False,
                    },
                },
                upsert=True
            )
        except Exception as e:
            await rep.report(f"Error adding user: {str(e)}", "error")

    async def present_user(self, user_id):
        """Check if user exists in database."""
        try:
            if self.db is None:
                await self.connect()
            user = await self.db.users.find_one({"user_id": user_id})
            return bool(user)
        except Exception as e:
            await rep.report(f"Error checking user presence: {str(e)}", "error")
            return False

    async def is_banned(self, user_id):
        """Check if user is banned."""
        try:
            if self.db is None:
                await self.connect()
            user = await self.db.users.find_one({"user_id": user_id})
            return user.get("is_banned", False) if user else False
        except Exception as e:
            await rep.report(f"Error checking ban status: {str(e)}", "error")
            return False

    async def add_ban_user(self, user_id):
        """Ban user."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.users.update_one(
                {"user_id": user_id}, {"$set": {"is_banned": True}}, upsert=True
            )
            return True
        except Exception as e:
            await rep.report(f"Error banning user: {str(e)}", "error")
            return False

    async def del_ban_user(self, user_id):
        """Unban user."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.users.update_one(
                {"user_id": user_id}, {"$set": {"is_banned": False}}
            )
            return True
        except Exception as e:
            await rep.report(f"Error unbanning user: {str(e)}", "error")
            return False

    async def get_ban_users(self):
        """Get all banned users."""
        try:
            if self.db is None:
                await self.connect()
            cursor = self.db.users.find({"is_banned": True})
            banned_users = []
            async for user in cursor:
                banned_users.append(user["user_id"])
            return banned_users
        except Exception as e:
            await rep.report(f"Error getting banned users: {str(e)}", "error")
            return []

    async def del_user(self, user_id):
        """Delete user."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.users.delete_one({"user_id": user_id})
            return True
        except Exception as e:
            await rep.report(f"Error deleting user: {str(e)}", "error")
            return False

    async def full_userbase(self):
        """Get all users."""
        try:
            if self.db is None:
                await self.connect()
            cursor = self.db.users.find({})
            users = []
            async for user in cursor:
                users.append(user["user_id"])
            return users
        except Exception as e:
            await rep.report(f"Error getting userbase: {str(e)}", "error")
            return []

    # ── ADMIN MANAGEMENT ──────────────────────────────────────────────────────

    async def add_admin(self, user_id):
        """Add admin."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.admins.update_one(
                {"user_id": user_id},
                {"$set": {"user_id": user_id,
                          "date_added": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
                upsert=True,
            )
            return True
        except Exception as e:
            await rep.report(f"Error adding admin: {str(e)}", "error")
            return False

    async def del_admin(self, user_id):
        """Remove admin."""
        try:
            if self.db is None:
                await self.connect()
            result = await self.db.admins.delete_one({"user_id": user_id})
            return result.deleted_count > 0
        except Exception as e:
            await rep.report(f"Error removing admin: {str(e)}", "error")
            return False

    async def get_all_admins(self):
        """Get all admins."""
        try:
            if self.db is None:
                await self.connect()
            cursor = self.db.admins.find({})
            admins = []
            async for admin in cursor:
                admins.append(admin["user_id"])
            return admins
        except Exception as e:
            await rep.report(f"Error getting admins: {str(e)}", "error")
            return []

    async def is_admin(self, user_id):
        """Check if user is admin."""
        try:
            if self.db is None:
                await self.connect()
            admin = await self.db.admins.find_one({"user_id": user_id})
            return admin is not None
        except Exception as e:
            await rep.report(f"Error checking admin status: {str(e)}", "error")
            return False

    # ── ANIME DATA MANAGEMENT ─────────────────────────────────────────────────

    async def saveAnime(self, anime_id, episode_number, quality, post_id, audio="Sub"):
        """Save anime episode data — keyed by anime_id + episode + quality + audio."""
        try:
            if self.db is None:
                await self.connect()
            quality_key = f"{quality}_{audio}"
            anime_data = {
                "anime_id":        anime_id,
                "episode_number":  str(episode_number),
                "quality":         quality,
                "audio":           audio,
                "quality_key":     quality_key,
                "post_id":         post_id,
                "date_added":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            await self.db.anime_data.update_one(
                {"anime_id": anime_id, "episode_number": str(episode_number),
                 "quality_key": quality_key},
                {"$set": anime_data},
                upsert=True,
            )
        except Exception as e:
            await rep.report(f"Error saving anime: {str(e)}", "error")

    async def getAnime(self, anime_id):
        """
        Get anime data structured as:
        { episode_number: { "quality_key (e.g. 720_Sub)": post_id } }
        """
        try:
            if self.db is None:
                await self.connect()
            cursor = self.db.anime_data.find({"anime_id": anime_id})
            anime_data = {}
            async for record in cursor:
                episode = str(record["episode_number"])
                key     = record.get("quality_key") or record["quality"]
                post_id = record["post_id"]
                if episode not in anime_data:
                    anime_data[episode] = {}
                anime_data[episode][key] = post_id
            return anime_data if anime_data else None
        except Exception as e:
            await rep.report(f"Error getting anime: {str(e)}", "error")
            return None

    async def reboot(self):
        """Clear anime cache/data."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.anime_data.delete_many({})
        except Exception as e:
            await rep.report(f"Error rebooting: {str(e)}", "error")

    # ── ANIME CHANNELS MANAGEMENT ─────────────────────────────────────────────

    async def add_anime_channel(self, anime_name, channel_id, channel_title,
                                 invite_link=None, db_type="ongoing", ani_id=None):
        """Add anime channel mapping. db_type: 'ongoing', 'completed', or 'movie'."""
        try:
            if self.db is None:
                await self.connect()
            channel_data = {
                "anime_name":    anime_name,
                "channel_id":    channel_id,
                "channel_title": channel_title,
                "invite_link":   invite_link,
                "db_type":       db_type,
                "date_added":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            if ani_id is not None:
                channel_data["ani_id"] = ani_id
            await self.db.anime_channels.update_one(
                {"anime_name": anime_name}, {"$set": channel_data}, upsert=True
            )
            return True
        except Exception as e:
            await rep.report(f"Error adding anime channel: {str(e)}", "error")
            return False

    async def find_channel_by_anime_title(self, torrent_name, extra_titles=None,
                                          db_type: str | None = None,
                                          ani_id: int | None = None):
        """
        Find channel by matching anime title or AniList ID.

        Fast path: if ani_id is given, does a direct indexed lookup first.
        Slow path: full collection scan with fuzzy name matching (fallback).

        db_type: if given, only return channels whose stored db_type matches.
        """
        try:
            if self.db is None:
                await self.connect()

            _base_query = {}
            if db_type is not None:
                _base_query["db_type"] = db_type

            # ── Fast path: direct ani_id lookup (indexed) ──────────────────
            if ani_id:
                channel = await self.db.anime_channels.find_one(
                    {**_base_query, "ani_id": ani_id}
                )
                if channel:
                    return {
                        "anime_name":    channel["anime_name"],
                        "channel_id":    channel["channel_id"],
                        "channel_title": channel["channel_title"],
                        "invite_link":   channel.get("invite_link"),
                        "db_type":       channel.get("db_type", "ongoing"),
                        "ani_id":        channel.get("ani_id"),
                    }

            # ── Slow path: fuzzy name scan ─────────────────────────────────
            cursor = self.db.anime_channels.find(_base_query)
            clean_torrent = self.clean_name_for_matching(torrent_name)
            search_terms = {clean_torrent.lower()}
            for t in (extra_titles or []):
                if t:
                    cleaned = self.clean_name_for_matching(str(t))
                    if cleaned:
                        search_terms.add(cleaned.lower())

            async for channel in cursor:
                anime_name  = channel["anime_name"]
                clean_anime = self.clean_name_for_matching(anime_name).lower()
                if any(
                    clean_anime in term or term in clean_anime
                    for term in search_terms if term
                ):
                    return {
                        "anime_name":    anime_name,
                        "channel_id":    channel["channel_id"],
                        "channel_title": channel["channel_title"],
                        "invite_link":   channel.get("invite_link"),
                        "db_type":       channel.get("db_type", "ongoing"),
                        "ani_id":        channel.get("ani_id"),
                    }
            return None
        except Exception as e:
            await rep.report(f"Error finding channel: {str(e)}", "error")
            return None

    async def get_all_anime_channels(self):
        """Get all anime channel mappings including ani_id and db_type."""
        try:
            if self.db is None:
                await self.connect()
            cursor = self.db.anime_channels.find({}).sort("date_added", -1)
            mappings = []
            async for channel in cursor:
                mappings.append({
                    "anime_name":    channel["anime_name"],
                    "channel_id":    channel["channel_id"],
                    "channel_title": channel["channel_title"],
                    "invite_link":   channel.get("invite_link"),
                    "db_type":       channel.get("db_type", "ongoing"),
                    "ani_id":        channel.get("ani_id"),
                })
            return mappings
        except Exception as e:
            await rep.report(f"Error getting anime channels: {str(e)}", "error")
            return []

    async def remove_anime_channel(self, anime_name):
        """Remove anime channel mapping (connection only, no data purge)."""
        try:
            if self.db is None:
                await self.connect()
            result = await self.db.anime_channels.delete_one(
                {"anime_name": {"$regex": f"^{re.escape(anime_name)}$", "$options": "i"}}
            )
            return result.deleted_count > 0
        except Exception as e:
            await rep.report(f"Error removing anime channel: {str(e)}", "error")
            return False

    async def remove_anime_channel_and_data(self, anime_name: str) -> tuple:
        """
        Remove anime channel connection AND purge ALL associated data.

        Cleans every collection that stores data for this anime:
          - anime_channels, anime_data, batch_links, batch_ep_links (all season variants)
          - ending_posts, schedule_posts, seen_torrents
          - download folders from disk

        Returns (success: bool, deleted_record_count: int).
        """
        try:
            if self.db is None:
                await self.connect()

            # 1. Find the channel record to get ani_id + channel_id
            record = await self.db.anime_channels.find_one(
                {"anime_name": {"$regex": f"^{re.escape(anime_name)}$", "$options": "i"}}
            )

            deleted = 0

            if record:
                ani_id     = record.get("ani_id")
                channel_id = record.get("channel_id")

                # ── Build the full set of ani_id variants to hit every doc ──
                _ani_ids = []
                if ani_id:
                    _ani_ids.extend([ani_id, str(ani_id)])
                    if str(ani_id).isdigit():
                        _ani_ids.append(int(ani_id))
                _ani_ids = list(dict.fromkeys(_ani_ids))

                # 2. anime_data (ongoing pipeline episode records)
                for _id in _ani_ids:
                    r = await self.db.anime_data.delete_many({"anime_id": _id})
                    deleted += r.deleted_count
                # name-based fallback
                r = await self.db.anime_data.delete_many({"anime_name": anime_name})
                deleted += r.deleted_count

                # 3. batch_ep_links — all season variants (s1–s10) + legacy integer key
                for _id in _ani_ids:
                    for s in range(1, 11):
                        r = await self.db.batch_ep_links.delete_one({"ani_id": f"{_id}_s{s}"})
                        deleted += r.deleted_count
                    # legacy: integer / string key with no season suffix
                    r = await self.db.batch_ep_links.delete_one({"ani_id": _id})
                    deleted += r.deleted_count

                # 4. batch_links — new schema (ani_id_raw) + legacy key prefix + name-keyed
                for _id in _ani_ids:
                    r = await self.db.batch_links.delete_many({"ani_id_raw": _id})
                    deleted += r.deleted_count
                    _id_str = str(_id)
                    if _id_str.isdigit():
                        r = await self.db.batch_links.delete_many(
                            {"ani_id": {"$regex": f"^{_id_str}_", "$options": ""}}
                        )
                        deleted += r.deleted_count
                r = await self.db.batch_links.delete_many(
                    {"torrent_name": {"$regex": re.escape(anime_name), "$options": "i"}}
                )
                deleted += r.deleted_count

                # 5. ending_posts (keyed by channel_id)
                if channel_id:
                    r = await self.db.ending_posts.delete_one({"channel_id": channel_id})
                    deleted += r.deleted_count

                # 6. schedule_posts (keyed by channel_id)
                if channel_id:
                    r = await self.db.schedule_posts.delete_one({"channel_id": channel_id})
                    deleted += r.deleted_count

                # 7. seen_torrents — remove entries matching anime name so it can be re-found
                try:
                    r = await self.db.seen_torrents.delete_many(
                        {"key": {"$regex": re.escape(anime_name), "$options": "i"}}
                    )
                    deleted += r.deleted_count
                except Exception:
                    pass

                # 8. Remove download folders from disk
                # (numbering kept relative to fields above; banner-cleanup step removed)
                import os as _os
                import shutil as _sh
                import re as _re2
                _safe = _re2.sub(r"[^\w\s-]", " ", anime_name)
                _safe = _re2.sub(r"\s+", " ", _safe).strip().replace(" ", "_")[:50]
                for _base in ["./downloads/batch", "./downloads/ongoing", "./downloads/movies"]:
                    _target = _os.path.join(_base, _safe)
                    if _os.path.isdir(_target):
                        try:
                            _sh.rmtree(_target)
                        except Exception:
                            pass

                # 10. Flush in-memory ani_cache
                if ani_id:
                    try:
                        from bot.core.auto_animes import ani_cache
                        # ani_cache['ongoing'] and ['completed'] are dicts, not sets.
                        # .discard() doesn't exist on dict — use .pop() instead.
                        for _cache_dict in (ani_cache.get("ongoing", {}), ani_cache.get("completed", {})):
                            keys_to_drop = [k for k in _cache_dict if k.startswith(f"{ani_id}_")]
                            for k in keys_to_drop:
                                _cache_dict.pop(k, None)
                    except Exception:
                        pass

                # 11. Remove the channel connection record itself (last step)
                await self.db.anime_channels.delete_one({"_id": record["_id"]})

                return True, deleted
            else:
                return False, 0

        except Exception as e:
            await rep.report(f"Error removing anime channel and data: {str(e)}", "error")
            return False, 0

    # ── PENDING CONNECTIONS ───────────────────────────────────────────────────

    async def add_pending_connection(self, user_id, anime_name, invite_link, extra=None):
        """Add pending channel connection."""
        try:
            if self.db is None:
                await self.connect()
            connection_data = {
                "user_id":    user_id,
                "anime_name": anime_name,
                "invite_link": invite_link,
                "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "extra":      extra or {},
            }
            await self.db.pending_connections.update_one(
                {"user_id": user_id}, {"$set": connection_data}, upsert=True
            )
        except Exception as e:
            await rep.report(f"Error adding pending connection: {str(e)}", "error")

    async def get_pending_connection(self, user_id):
        """Get pending connection for user."""
        try:
            if self.db is None:
                await self.connect()
            connection = await self.db.pending_connections.find_one({"user_id": user_id})
            if connection:
                return {
                    "anime_name":  connection["anime_name"],
                    "invite_link": connection["invite_link"],
                    "extra":       connection.get("extra", {}),
                }
            return None
        except Exception as e:
            await rep.report(f"Error getting pending connection: {str(e)}", "error")
            return None

    async def remove_pending_connection(self, user_id):
        """Remove pending connection for user."""
        try:
            if self.db is None:
                await self.connect()
            result = await self.db.pending_connections.delete_one({"user_id": user_id})
            return result.deleted_count > 0
        except Exception as e:
            await rep.report(f"Error removing pending connection: {str(e)}", "error")
            return False

    def clean_name_for_matching(self, name):
        """Clean anime name for better matching."""
        patterns_to_remove = [
            r'\[.*?\]',
            r'\(.*?\)',
            r'- \d+',
            r'S\d+',
            r'1080p|720p|480p|HEVC|x264|x265',
            r'SubsPlease|Erai-raws|HorribleSubs',
        ]
        cleaned = name
        for pattern in patterns_to_remove:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        return cleaned.strip()

    # ── SETTINGS ──────────────────────────────────────────────────────────────

    async def get_del_timer(self):
        """Get auto-delete timer."""
        try:
            if self.db is None:
                await self.connect()
            settings = await self.db.settings.find_one({"key": "del_timer"})
            return int(settings.get("value", 600)) if settings else 600
        except Exception as e:
            await rep.report(f"Error getting delete timer: {str(e)}", "error")
            return 600

    async def set_del_timer(self, timer):
        """Set auto-delete timer."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.settings.update_one(
                {"key": "del_timer"},
                {"$set": {"key": "del_timer", "value": timer}},
                upsert=True,
            )
        except Exception as e:
            await rep.report(f"Error setting delete timer: {str(e)}", "error")

    # ── FORCE SUBSCRIPTION ────────────────────────────────────────────────────

    async def add_channel(self, channel_id, bot_type: str = "all"):
        """Add force subscription channel for a specific bot type.
        bot_type: 'ongoing' | 'completed' | 'movie' | 'all'
        """
        try:
            if self.db is None:
                await self.connect()
            channel_data = {
                "channel_id": channel_id,
                "mode":       "off",
                "bot_type":   bot_type,
                "date_added": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            await self.db.force_sub_channels.update_one(
                {"channel_id": channel_id}, {"$set": channel_data}, upsert=True
            )
            return True
        except Exception as e:
            await rep.report(f"Error adding force sub channel: {str(e)}", "error")
            return False

    async def rem_channel(self, channel_id):
        """Remove force subscription channel."""
        try:
            if self.db is None:
                await self.connect()
            result = await self.db.force_sub_channels.delete_one({"channel_id": channel_id})
            await self.db.join_request_channels.delete_many({"channel_id": channel_id})
            return result.deleted_count > 0
        except Exception as e:
            await rep.report(f"Error removing force sub channel: {str(e)}", "error")
            return False

    async def show_channels(self, bot_type: str = None):
        """Get force subscription channels, optionally filtered by bot_type.
        If bot_type given, returns channels for that type + channels set to 'all'.
        """
        try:
            if self.db is None:
                await self.connect()
            if bot_type:
                cursor = self.db.force_sub_channels.find({
                    "bot_type": {"$in": [bot_type, "all"]}
                })
            else:
                cursor = self.db.force_sub_channels.find({})
            channels = []
            async for channel in cursor:
                channels.append(channel["channel_id"])
            return channels
        except Exception as e:
            await rep.report(f"Error getting force sub channels: {str(e)}", "error")
            return []

    async def set_channel_mode(self, channel_id, mode):
        """Set channel mode (on/off for request mode)."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.force_sub_channels.update_one(
                {"channel_id": channel_id}, {"$set": {"mode": mode}}, upsert=True
            )
            return True
        except Exception as e:
            await rep.report(f"Error setting channel mode: {str(e)}", "error")
            return False

    async def get_channel_mode(self, channel_id):
        """Get channel mode."""
        try:
            if self.db is None:
                await self.connect()
            channel = await self.db.force_sub_channels.find_one({"channel_id": channel_id})
            return channel.get("mode", "off") if channel else "off"
        except Exception as e:
            await rep.report(f"Error getting channel mode: {str(e)}", "error")
            return "off"

    async def reqChannel_exist(self, channel_id):
        """Check if channel exists in force sub list."""
        try:
            if self.db is None:
                await self.connect()
            channel_ids = await self.show_channels()
            return int(channel_id) in channel_ids
        except Exception as e:
            await rep.report(f"Error checking if channel exists: {str(e)}", "error")
            return False

    async def store_invite_link(self, channel_id, invite_link, mode=None, expire_date=None):
        """Store invite link for a channel.

        CREATE-ONCE-REUSE: each channel keeps a SEPARATE cached link per
        mode under `invite_links.on` and `invite_links.off`. Once a mode's
        link is created, it is reused forever — toggling the channel mode
        does NOT regenerate either link, it just switches which cached
        link the gate hands out. This eliminates redundant
        create_chat_invite_link API calls and stops cluttering the
        channel's invite-link list with one new entry per toggle.

        Backwards-compat: the legacy single-link fields
        (`invite_link`, `link_mode`) are also written so older readers /
        admin views keep working without a migration.
        """
        try:
            if self.db is None:
                await self.connect()
            update = {
                "invite_link":      invite_link,                # legacy mirror
                "link_expire_date": expire_date,
                "updated_at":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            if mode in ("on", "off"):
                update[f"invite_links.{mode}"] = invite_link
                update["link_mode"] = mode                       # legacy mirror
            await self.db.force_sub_channels.update_one(
                {"channel_id": channel_id},
                {"$set": update},
                upsert=True,
            )
            return True
        except Exception as e:
            await rep.report(f"Error storing invite link: {str(e)}", "error")
            return False

    async def get_invite_link(self, channel_id, expected_mode=None):
        """Get stored invite link for a channel.

        CREATE-ONCE-REUSE: when `expected_mode` is given, returns the
        link cached specifically for THAT mode (`invite_links.<mode>`).
        Falls back to the legacy single-link fields when the per-mode
        slot is empty but the legacy entry was created for the same
        mode. Returns None only when no suitable link is cached, which
        is the signal for the caller to create one (exactly once for
        that mode — it then sticks around forever via store_invite_link).
        """
        try:
            if self.db is None:
                await self.connect()
            data = await self.db.force_sub_channels.find_one({"channel_id": channel_id})
            if not data:
                return None

            # Expiry check (only meaningful when set; we don't expire by default)
            if data.get("link_expire_date"):
                if int(time.time()) >= data["link_expire_date"]:
                    return None

            per_mode = data.get("invite_links") or {}
            legacy_link = data.get("invite_link")
            legacy_mode = data.get("link_mode")

            if expected_mode in ("on", "off"):
                # 1) Prefer the per-mode cached link
                if per_mode.get(expected_mode):
                    return per_mode[expected_mode]
                # 2) Fall back to the legacy field if it was created for
                #    the same mode (legacy entries with no recorded mode
                #    are treated as "off" since fsub_validate_channel
                #    only ever creates non-request links).
                effective_legacy_mode = legacy_mode or "off"
                if legacy_link and effective_legacy_mode == expected_mode:
                    return legacy_link
                return None

            # No mode requested — return whatever is cached (admin views).
            return per_mode.get("on") or per_mode.get("off") or legacy_link or None
        except Exception as e:
            await rep.report(f"Error getting invite link: {str(e)}", "error")
            return None

    async def clear_invite_link(self, channel_id):
        """Forget every cached invite link for a channel.

        Kept for emergency / future admin use (e.g. an explicit
        /relinkfsub command). Normal mode toggles do NOT call this any
        more — see CREATE-ONCE-REUSE above.
        """
        try:
            if self.db is None:
                await self.connect()
            await self.db.force_sub_channels.update_one(
                {"channel_id": channel_id},
                {"$unset": {
                    "invite_link":      "",
                    "link_expire_date": "",
                    "link_mode":        "",
                    "invite_links":     "",
                }},
            )
            return True
        except Exception as e:
            await rep.report(f"Error clearing invite link: {str(e)}", "error")
            return False

    async def req_user(self, channel_id, user_id):
        """Add user to join request list."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.join_request_channels.update_one(
                {"channel_id": int(channel_id)},
                {"$addToSet": {"user_ids": int(user_id)}},
                upsert=True,
            )
            return True
        except Exception as e:
            await rep.report(f"Error adding user to request list: {str(e)}", "error")
            return False

    async def del_req_user(self, channel_id, user_id):
        """Remove user from join request list."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.join_request_channels.update_one(
                {"channel_id": int(channel_id)},
                {"$pull": {"user_ids": int(user_id)}},
            )
            return True
        except Exception as e:
            await rep.report(f"Error removing user from request list: {str(e)}", "error")
            return False

    async def req_user_exist(self, channel_id, user_id):
        """Check if user exists in join request list."""
        try:
            if self.db is None:
                await self.connect()
            found = await self.db.join_request_channels.find_one({
                "channel_id": int(channel_id),
                "user_ids":   int(user_id),
            })
            return bool(found)
        except Exception as e:
            await rep.report(f"Error checking request list: {str(e)}", "error")
            return False

    # ── TOKEN MANAGEMENT ──────────────────────────────────────────────────────

    async def store_token(self, user_id, token, expire_seconds):
        """Store verification token with expiry."""
        try:
            if self.db is None:
                await self.connect()
            expire_time = time.time() + expire_seconds
            await self.db.tokens.update_one(
                {"user_id": user_id},
                {"$set": {"token": token, "expire_time": expire_time,
                          "created_time": time.time()}},
                upsert=True,
            )
            return True
        except Exception as e:
            await rep.report(f"Error storing token: {str(e)}", "error")
            return False

    async def is_token_valid(self, token):
        """Check if token exists and is not expired."""
        try:
            if self.db is None:
                await self.connect()
            token_data = await self.db.tokens.find_one({
                "token":       token,
                "expire_time": {"$gt": time.time()},
            })
            return bool(token_data)
        except Exception as e:
            await rep.report(f"Error validating token: {str(e)}", "error")
            return False

    async def remove_token(self, token):
        """Remove token after successful verification."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.tokens.delete_one({"token": token})
            return True
        except Exception as e:
            await rep.report(f"Error removing token: {str(e)}", "error")
            return False

    async def get_user_token(self, user_id):
        """Get user's current valid token."""
        try:
            if self.db is None:
                await self.connect()
            token_data = await self.db.tokens.find_one({
                "user_id":     user_id,
                "expire_time": {"$gt": time.time()},
            })
            return token_data.get("token") if token_data else None
        except Exception as e:
            await rep.report(f"Error getting user token: {str(e)}", "error")
            return None

    # ── VERIFICATION STATUS ───────────────────────────────────────────────────

    async def get_verify_status(self, user_id):
        """Get user verification status."""
        _default = {"is_verified": False, "verified_time": 0, "verify_token": "", "link": ""}
        try:
            if self.db is None:
                await self.connect()
            user = await self.db.users.find_one({"user_id": user_id})
            if user and "verify_status" in user:
                return user["verify_status"]
            return _default
        except Exception as e:
            await rep.report(f"Error getting verify status: {str(e)}", "error")
            return _default

    async def update_verify_status(self, user_id, verify_token="", is_verified=False,
                                    verified_time=0, link=""):
        """Update user verification status."""
        try:
            if self.db is None:
                await self.connect()
            verify_status = {
                "verify_token":  verify_token,
                "is_verified":   is_verified,
                "verified_time": verified_time,
                "link":          link,
            }
            await self.db.users.update_one(
                {"user_id": user_id},
                {"$set": {"verify_status": verify_status}},
                upsert=True,
            )
            return True
        except Exception as e:
            await rep.report(f"Error updating verify status: {str(e)}", "error")
            return False

    async def get_verify_count(self, user_id):
        """Get user verification count."""
        try:
            if self.db is None:
                await self.connect()
            user = await self.db.verify_counts.find_one({"user_id": user_id})
            return user.get("verify_count", 0) if user else 0
        except Exception as e:
            await rep.report(f"Error getting verify count: {str(e)}", "error")
            return 0

    async def set_verify_count(self, user_id, count):
        """Set user verification count."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.verify_counts.update_one(
                {"user_id": user_id}, {"$set": {"verify_count": count}}, upsert=True
            )
            return True
        except Exception as e:
            await rep.report(f"Error setting verify count: {str(e)}", "error")
            return False

    # ── ANIME INDEX ───────────────────────────────────────────────────────────

    async def get_index_message_ids(self, channel_type: str = "ongoing") -> list:
        """Get index slot message IDs for a specific channel type.
        channel_type: 'ongoing' | 'completed' | 'movie'
        """
        try:
            if self.db is None:
                await self.connect()
            doc = await self.db.anime_index_config.find_one({"_id": f"index_slots_{channel_type}"})
            if doc:
                return doc.get("message_ids", [])
            # Legacy fallback — old single-type index
            if channel_type == "ongoing":
                old = await self.db.anime_index_config.find_one({"_id": "index_slots"})
                return old.get("message_ids", []) if old else []
            return []
        except Exception as e:
            await rep.report(f"Error getting index message IDs: {e}", "error")
            return []

    async def set_index_message_ids(self, message_ids: list, channel_type: str = "ongoing"):
        """Save index slot message IDs for a specific channel type."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.anime_index_config.update_one(
                {"_id": f"index_slots_{channel_type}"},
                {"$set": {"message_ids": message_ids, "channel_type": channel_type}},
                upsert=True
            )
        except Exception as e:
            await rep.report(f"Error setting index message IDs: {e}", "error")

    async def get_all_indexed_anime(self) -> list:
        """Return all anime with a dedicated channel, sorted alphabetically."""
        try:
            if self.db is None:
                await self.connect()
            cursor = self.db.anime_channels.find(
                {}, {"anime_name": 1, "channel_id": 1, "invite_link": 1}
            ).sort("anime_name", 1)
            return await cursor.to_list(length=1000)
        except Exception as e:
            await rep.report(f"Error getting indexed anime: {e}", "error")
            return []

    # ── ACM PENDING EPISODE ───────────────────────────────────────────────────

    async def set_pending_episode(self, user_id: int, anime_name: str):
        """Store pending episode upload request for a user."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.acm_pending_ep.update_one(
                {"user_id": user_id},
                {"$set": {"anime_name": anime_name, "action": "episode"}},
                upsert=True,
            )
        except Exception as e:
            await rep.report(f"Error setting pending episode: {e}", "error")

    async def get_pending_episode(self, user_id: int) -> dict | None:
        """Get pending episode upload request for a user."""
        try:
            if self.db is None:
                await self.connect()
            return await self.db.acm_pending_ep.find_one({"user_id": user_id})
        except Exception as e:
            await rep.report(f"Error getting pending episode: {e}", "error")
            return None

    async def clear_pending_episode(self, user_id: int):
        """Remove pending episode upload request."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.acm_pending_ep.delete_one({"user_id": user_id})
        except Exception as e:
            await rep.report(f"Error clearing pending episode: {e}", "error")

    # ── BATCH LINK STORAGE ────────────────────────────────────────────────────

    async def save_batch_link(self, ani_id: int, first_msg_id: int,
                               last_msg_id: int, file_store: int,
                               season: str = "s1", extra: dict = None):
        """Save batch link data for one season of an anime.

        New schema (per-season docs):
          Key: {ani_id}_{abs(file_store)}_{season}  e.g. "150672_1927283666_s3"
          Fields stored directly (no season prefix): index_post_id, first_Hdri, etc.

        Legacy schema (single doc with s1_/s2_ prefixed fields) is still readable
        via get_batch_link() for backward compatibility with existing data.
        """
        try:
            if self.db is None:
                await self.connect()
            _doc_key = f"{ani_id}_{abs(file_store)}_{season}" if file_store else f"{ani_id}_{season}"
            _set = {
                "ani_id":       _doc_key,
                "ani_id_raw":   ani_id,
                "season":       season,
                "first_msg_id": first_msg_id,
                "last_msg_id":  last_msg_id,
                "file_store":   file_store,
                "updated_at":   datetime.utcnow(),
            }
            if extra:
                _set.update(extra)
            await self.db.batch_links.update_one(
                {"ani_id": _doc_key}, {"$set": _set}, upsert=True
            )
        except Exception as e:
            await rep.report(f"Error saving batch link: {e}", "error")

    async def get_batch_link(self, ani_id: int, file_store: int = None,
                              season: str = "s1") -> dict | None:
        """Get saved batch link data for one season of an anime.

        Lookup order:
          1. New per-season key: {ani_id}_{file_store}_{season}
          2. Legacy single-doc key: {ani_id}_{file_store}  (fields have s1_/s2_ prefix)
          3. Legacy integer key: {ani_id}
        For legacy docs, extracts season-prefixed fields into flat dict for compatibility.
        """
        try:
            if self.db is None:
                await self.connect()

            # 1. New per-season key
            if file_store:
                _new_key = f"{ani_id}_{abs(file_store)}_{season}"
                doc = await self.db.batch_links.find_one({"ani_id": _new_key})
                if doc:
                    return doc

            # 2. Legacy key — single doc with all seasons
            _legacy_key = f"{ani_id}_{abs(file_store)}" if file_store else None
            legacy_doc = None
            if _legacy_key:
                legacy_doc = await self.db.batch_links.find_one({"ani_id": _legacy_key})
            if not legacy_doc:
                legacy_doc = await self.db.batch_links.find_one({"ani_id": ani_id})

            if legacy_doc:
                # Flatten season-prefixed fields to unprefixed for callers
                flat = {k: v for k, v in legacy_doc.items() if not k.startswith("s")}
                sk = season  # e.g. "s3"
                for field in ["index_post_id", "index_post_channel", "notify_post_id",
                               "first_Hdri", "last_Hdri", "first_1080", "last_1080",
                               "first_720", "last_720", "first_480", "last_480"]:
                    val = legacy_doc.get(f"{sk}_{field}")
                    if val:
                        flat[field] = val
                if flat.get("index_post_id") or flat.get("first_msg_id"):
                    flat["_legacy"] = True
                    return flat

            return None
        except Exception as e:
            await rep.report(f"Error getting batch link: {e}", "error")
            return None

    async def get_latest_batch_links(self, limit: int = 10) -> list:
        """Get the most recently completed batches, newest first."""
        try:
            if self.db is None:
                await self.connect()
            cursor = self.db.batch_links.find({}, sort=[("updated_at", -1)]).limit(limit)
            return await cursor.to_list(length=limit)
        except Exception as e:
            await rep.report(f"Error getting latest batch links: {e}", "error")
            return []

    # ── SEEN TORRENTS (dedup) ─────────────────────────────────────────────────

    async def is_torrent_seen(self, key: str) -> bool:
        """Check if a torrent key exists in the seen_torrents collection."""
        try:
            if self.db is None:
                await self.connect()
            return bool(await self.db.seen_torrents.find_one({"_id": key}, {"_id": 1}))
        except Exception:
            return False  # on DB error, allow through (non-critical)

    async def is_episode_done(self, anime_id: int, episode_number, audio: str = "Sub") -> bool:
        """Check if ALL qualities for an episode+audio combo are already in anime_data.
        Used by ongoing pipeline to skip re-processing without loading the full
        episode dict into RAM.
        """
        try:
            if self.db is None:
                await self.connect()
            from bot import Var as _Var
            for qual in _Var.QUALS:
                qkey = f"{qual}_{audio}"
                exists = await self.db.anime_data.find_one(
                    {"anime_id": anime_id, "episode_number": episode_number,
                     "quality_key": qkey},
                    {"_id": 1},
                )
                if not exists:
                    return False
            return True
        except Exception:
            return False

    async def mark_torrent_seen(self, key: str, ttl_hours: int = 48):
        """Mark a torrent URL/name as seen. Expires after ttl_hours.

        FIX #10: The TTL index is now created once in connect(), not here.
        Previously create_index() was called on every single invocation of this
        method — potentially hundreds of times per minute — generating unnecessary
        MongoDB load. The _ensure_ttl_index() call in connect() is sufficient.
        """
        try:
            if self.db is None:
                await self.connect()
            expires_at = datetime.utcnow() + timedelta(hours=ttl_hours)
            await self.db.seen_torrents.update_one(
                {"_id": key},
                {"$set": {"_id": key, "expires_at": expires_at}},
                upsert=True,
            )
        except Exception:
            pass  # dedup failure is non-critical

    async def get_seen_torrents(self, limit: int = 2000) -> set:
        """Return set of recently seen torrent keys to warm the in-memory cache."""
        try:
            if self.db is None:
                await self.connect()
            cursor = self.db.seen_torrents.find({}, {"_id": 1}).limit(limit)
            docs = await cursor.to_list(length=limit)
            return {d["_id"] for d in docs}
        except Exception:
            return set()

    # ── BATCH EPISODE LINKS ───────────────────────────────────────────────────

    async def save_batch_ep_link(self, ani_id: int, ep_num: int, qual: str, link: str, season: str = "s1"):
        """Save a single episode+quality download link for batch resume."""
        try:
            if self.db is None:
                await self.connect()
            db_key = f"{ani_id}_{season}"
            await self.db.batch_ep_links.update_one(
                {"ani_id": db_key},
                {"$set": {f"links.{ep_num}.{qual}": link,
                          "updated_at": datetime.utcnow()}},
                upsert=True,
            )
        except Exception:
            pass

    async def get_batch_ep_links(self, ani_id: int, season: str = "s1") -> dict:
        """Get all saved episode links for a batch. Returns {ep_num: {qual: link}}."""
        try:
            if self.db is None:
                await self.connect()
            db_key = f"{ani_id}_{season}"
            doc = await self.db.batch_ep_links.find_one({"ani_id": db_key})
            if doc:
                return {int(k): v for k, v in doc.get("links", {}).items()}
            return {}
        except Exception:
            return {}

    # ── ENDING POST TRACKING ──────────────────────────────────────────────────

    async def save_ending_post(self, channel_id: int, msg_id: int):
        """Store the latest ending-card message ID for a channel."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.ending_posts.update_one(
                {"channel_id": channel_id},
                {"$set": {"channel_id": channel_id, "msg_id": msg_id,
                           "updated_at": datetime.utcnow()}},
                upsert=True
            )
        except Exception:
            pass

    async def get_ending_post(self, channel_id: int) -> int | None:
        """Get the latest ending-card message ID for a channel, or None."""
        try:
            if self.db is None:
                await self.connect()
            doc = await self.db.ending_posts.find_one({"channel_id": channel_id})
            return doc["msg_id"] if doc else None
        except Exception:
            return None

    async def delete_ending_post_record(self, channel_id: int):
        """Remove the ending post record after deletion."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.ending_posts.delete_one({"channel_id": channel_id})
        except Exception:
            pass

    async def get_batch_link_by_name(self, torrent_name: str,
                                       season: str = "s1") -> dict | None:
        """Fallback batch-link lookup by torrent name + season."""
        try:
            if self.db is None:
                await self.connect()
            # New: torrent_name + season compound lookup
            doc = await self.db.batch_links.find_one(
                {"torrent_name": torrent_name, "season": season}
            )
            if doc:
                return doc
            # Legacy: torrent_name only (old single-doc format)
            legacy = await self.db.batch_links.find_one({"torrent_name": torrent_name})
            if legacy:
                flat = {k: v for k, v in legacy.items() if not k.startswith("s") or k == "season"}
                sk = season
                for field in ["index_post_id", "index_post_channel", "notify_post_id",
                               "first_Hdri", "last_Hdri", "first_1080", "last_1080",
                               "first_720", "last_720", "first_480", "last_480"]:
                    val = legacy.get(f"{sk}_{field}")
                    if val:
                        flat[field] = val
                if flat.get("index_post_id") or flat.get("first_msg_id"):
                    flat["_legacy"] = True
                    return flat
            return None
        except Exception:
            return None

    async def save_batch_link_by_name(self, torrent_name: str, update: dict,
                                       season: str = "s1"):
        """Upsert batch-link data keyed by torrent name + season."""
        try:
            if self.db is None:
                await self.connect()
            update["torrent_name"] = torrent_name
            update["season"] = season
            update["updated_at"] = datetime.utcnow()
            await self.db.batch_links.update_one(
                {"torrent_name": torrent_name, "season": season},
                {"$set": update}, upsert=True
            )
        except Exception as e:
            await rep.report(f"Error saving batch link by name: {e}", "error")


    # ── SCHEDULE POSTS ────────────────────────────────────────────────────────

    async def save_schedule_posts(self, channel_id: int, msg_ids: list):
        """Store the list of schedule message IDs posted to a channel."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.schedule_posts.update_one(
                {"channel_id": channel_id},
                {"$set": {"channel_id": channel_id, "msg_ids": msg_ids,
                           "updated_at": datetime.utcnow()}},
                upsert=True
            )
        except Exception:
            pass

    async def get_schedule_posts(self, channel_id: int) -> list:
        """Get previously posted schedule message IDs for a channel."""
        try:
            if self.db is None:
                await self.connect()
            doc = await self.db.schedule_posts.find_one({"channel_id": channel_id})
            return doc["msg_ids"] if doc else []
        except Exception:
            return []

    async def delete_schedule_posts(self, channel_id: int):
        """Clear stored schedule post IDs after deletion."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.schedule_posts.delete_one({"channel_id": channel_id})
        except Exception:
            pass


# ── Module-level singletons ───────────────────────────────────────────────────

db = Database()


class BatchDatabase(Database):
    """Separate DB connection for completed/batch anime — uses BATCH_DB_NAME."""

    async def connect(self):
        try:
            from bot import Var
            self.client = AsyncIOMotorClient(Var.MONGO_URI)
            self.db = self.client[Var.BATCH_DB_NAME]
            await self.db.command("ping")
            # FIX #10: ensure TTL index here too for the batch DB connection.
            await self._ensure_ttl_index()
            await rep.report(f"Batch MongoDB connected ({Var.BATCH_DB_NAME})", "info", log=False)
            return True
        except Exception as e:
            await rep.report(f"Batch MongoDB connection error: {str(e)}", "error")
            return False


    # ── SKIPPED BATCH INBOX ───────────────────────────────────────────────────

    async def save_skipped_batch(self, name: str, torrent: str) -> str:
        """
        Store a skipped batch torrent with a short numeric ID.
        Returns the short ID (e.g. '001') so admin can use /processbatch 001.
        """
        try:
            if self.db is None:
                await self.connect()
            # Auto-increment short ID
            counter = await self.db.skipped_batches.count_documents({})
            short_id = str(counter + 1).zfill(3)
            await self.db.skipped_batches.update_one(
                {"torrent": torrent},
                {"$set": {
                    "short_id": short_id,
                    "name": name,
                    "torrent": torrent,
                    "saved_at": datetime.utcnow(),
                }},
                upsert=True
            )
            return short_id
        except Exception:
            return ""

    async def get_skipped_batch(self, short_id: str) -> dict | None:
        """Get a skipped batch by its short ID."""
        try:
            if self.db is None:
                await self.connect()
            return await self.db.skipped_batches.find_one({"short_id": short_id})
        except Exception:
            return None

    async def get_all_skipped_batches(self) -> list:
        """List all pending skipped batches."""
        try:
            if self.db is None:
                await self.connect()
            cursor = self.db.skipped_batches.find({}, sort=[("saved_at", -1)])
            return await cursor.to_list(length=50)
        except Exception:
            return []

    async def delete_skipped_batch(self, short_id: str):
        """Remove a skipped batch after processing."""
        try:
            if self.db is None:
                await self.connect()
            await self.db.skipped_batches.delete_one({"short_id": short_id})
        except Exception:
            pass


batch_db = BatchDatabase()


class MovieDatabase(Database):
    """Separate DB connection for movies — uses MOVIE_DB_NAME."""

    async def connect(self):
        try:
            from bot import Var
            self.client = AsyncIOMotorClient(Var.MONGO_URI)
            self.db = self.client[Var.MOVIE_DB_NAME]
            await self.db.command("ping")
            await self._ensure_ttl_index()
            await rep.report(f"Movie MongoDB connected ({Var.MOVIE_DB_NAME})", "info", log=False)
            return True
        except Exception as e:
            await rep.report(f"Movie MongoDB connection error: {str(e)}", "error")
            return False


movie_db = MovieDatabase()
