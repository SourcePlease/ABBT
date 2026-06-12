"""
Persistent Task Queue — survives bot restarts via MongoDB.

Each task document:
  {
    _id: ObjectId,
    name: str,           # torrent name
    torrent: str,        # magnet or .torrent URL
    alt_torrents: [str], # fallback torrent URLs in priority order (top 2 after primary)
    status: "pending" | "downloading" | "encoding" | "uploading" | "done" | "failed",
    quals_done: [str],   # qualities already uploaded successfully
    is_batch: bool,      # FIX #14: written at enqueue time so resume query works
    ani_id: int | None,
    ep_no: str | None,
    post_id: int | None,
    retry_count: int,
    created_at: datetime,
    updated_at: datetime,
    error: str | None,
    source_priority: int  # lower = higher priority (0 = SubsPlease, 1 = default)
  }
"""

from datetime import datetime, timedelta
from traceback import format_exc

from bot.core.reporter import rep


MAX_RETRIES = 3


class PersistentTaskQueue:
    def __init__(self):
        self._db = None

    async def _col(self):
        from bot.core.database import db
        # Ensure DB is connected before accessing
        if db.db is None:
            await db.connect()
        return db.db["task_queue"]

    # ── CRUD ────────────────────────────────────────────────────────────────

    async def enqueue(self, name: str, torrent: str, source_priority: int = 1,
                      is_batch: bool = False,
                      alt_torrents: list | None = None) -> str | None:
        """Add a new task. Returns inserted id as str, or None if duplicate.

        FIX #14: Added is_batch parameter. Previously enqueue() never wrote an
        is_batch field, so get_resumable_tasks()'s {is_batch: True} filter always
        returned 0 results -- no batch tasks were ever resumed after a restart.

        alt_torrents: up to 2 fallback torrent URLs (indices 1 and 2 from the
        ranked candidate list). If the primary torrent produces corrupt/unreadable
        files the pipeline pops the first alt and re-queues from scratch.
        """
        try:
            col = await self._col()
            # Deduplicate: same torrent URL that isn't done/failed yet
            existing = await col.find_one({"torrent": torrent, "status": {"$nin": ["done", "failed"]}})
            if existing:
                return str(existing["_id"])
            now = datetime.utcnow()
            doc = {
                "name": name,
                "torrent": torrent,
                "alt_torrents": (alt_torrents or [])[:2],  # store max 2 fallbacks
                "status": "pending",
                "quals_done": [],
                "is_batch": is_batch,   # FIX #14: persist the flag so resume works
                "ani_id": None,
                "ep_no": None,
                "post_id": None,
                "retry_count": 0,
                "created_at": now,
                "updated_at": now,
                "error": None,
                "source_priority": source_priority,
            }
            result = await col.insert_one(doc)
            return str(result.inserted_id)
        except Exception:
            await rep.report(format_exc(), "error")
            return None

    async def pop_alt_torrent(self, task_id: str) -> tuple | None:
        """Atomically pop the first alt_torrent from the list.

        Returns (name, url) of the next candidate, or None if the list is empty.
        The popped entry is removed from DB so it won't be retried twice.
        """
        try:
            from bson import ObjectId
            col = await self._col()
            # Fetch BEFORE the pop so we can read the first alt
            doc = await col.find_one_and_update(
                {"_id": ObjectId(task_id), "alt_torrents.0": {"$exists": True}},
                {"$pop": {"alt_torrents": -1}, "$set": {"updated_at": datetime.utcnow()}},
                return_document=False,  # return doc state BEFORE update
            )
            if not doc:
                return None
            alts = doc.get("alt_torrents") or []
            if not alts:
                return None
            next_url = alts[0]
            next_name = doc.get("name", "Unknown") + " [alt]"
            return (next_name, next_url)
        except Exception:
            await rep.report(format_exc(), "error")
            return None

    async def update_task(self, task_id: str, **fields):
        """Patch arbitrary fields on a task."""
        try:
            from bson import ObjectId
            col = await self._col()
            fields["updated_at"] = datetime.utcnow()
            await col.update_one({"_id": ObjectId(task_id)}, {"$set": fields})
        except Exception:
            await rep.report(format_exc(), "error")

    async def mark_qual_done(self, task_id: str, qual: str):
        """Record that a quality was successfully uploaded."""
        try:
            from bson import ObjectId
            col = await self._col()
            await col.update_one(
                {"_id": ObjectId(task_id)},
                {"$addToSet": {"quals_done": qual}, "$set": {"updated_at": datetime.utcnow()}}
            )
        except Exception:
            await rep.report(format_exc(), "error")

    async def mark_done(self, task_id: str):
        await self.update_task(task_id, status="done")

    async def mark_failed(self, task_id: str, error: str = ""):
        await self.update_task(task_id, status="failed", error=error[:500])

    async def increment_retry(self, task_id: str) -> int:
        """Bump retry count; returns new count."""
        try:
            from bson import ObjectId
            col = await self._col()
            result = await col.find_one_and_update(
                {"_id": ObjectId(task_id)},
                {"$inc": {"retry_count": 1}, "$set": {"updated_at": datetime.utcnow()}},
                return_document=True
            )
            return result["retry_count"] if result else MAX_RETRIES + 1
        except Exception:
            await rep.report(format_exc(), "error")
            return MAX_RETRIES + 1

    async def get_task(self, task_id: str) -> dict | None:
        try:
            from bson import ObjectId
            col = await self._col()
            return await col.find_one({"_id": ObjectId(task_id)})
        except Exception:
            return None

    # ── RESUME / RECOVERY ───────────────────────────────────────────────────

    async def get_resumable_tasks(self) -> list[dict]:
        """
        Return all batch tasks that should be resumed on startup:
        - Any pending batch task regardless of quals_done (includes fresh ones
          interrupted before any quality was uploaded)
        - Excludes tasks that have exhausted retries (status=failed with
          retry_count >= MAX_RETRIES)

        Single-episode ongoing tasks are NOT resumed — they arrive naturally
        from the RSS feed.

        FIX #14: Now works correctly because enqueue() properly writes is_batch=True
        for batch tasks. Previously this query always returned 0 results.
        """
        try:
            col = await self._col()
            cursor = col.find(
                {
                    "status": "pending",
                    "is_batch": True,
                    "retry_count": {"$lt": MAX_RETRIES},
                },
                sort=[("source_priority", 1), ("created_at", 1)]
            )
            return await cursor.to_list(length=20)
        except Exception:
            await rep.report(format_exc(), "error")
            return []

    async def reset_stuck_tasks(self):
        """
        On startup: reset all in-progress tasks back to pending so they are
        picked up by get_resumable_tasks() and retried automatically.

        We never mark them failed here — failed status is reserved for tasks
        that have exhausted all retries via increment_retry().
        """
        try:
            col = await self._col()
            result = await col.update_many(
                {"status": {"$in": ["downloading", "encoding", "uploading"]}},
                {"$set": {
                    "status": "pending",
                    "error": None,
                    "updated_at": datetime.utcnow(),
                }}
            )
            if result.modified_count:
                await rep.report(
                    f"🔄 Reset {result.modified_count} interrupted task(s) to pending on startup.",
                    "info"
                )
        except Exception:
            await rep.report(format_exc(), "error")

    async def clear_pending_tasks(self) -> int:
        """Delete all tasks with status 'pending' — stale unstarted entries."""
        try:
            col = await self._col()
            result = await col.delete_many({"status": "pending"})
            return result.deleted_count
        except Exception:
            await rep.report(format_exc(), "error")
            return 0

    async def clear_failed_tasks(self) -> int:
        """Delete all tasks with status 'failed'."""
        try:
            col = await self._col()
            result = await col.delete_many({"status": "failed"})
            return result.deleted_count
        except Exception:
            await rep.report(format_exc(), "error")
            return 0

    # ── STATUS INFO ──────────────────────────────────────────────────────────

    async def queue_stats(self) -> dict:
        """Return counts per status + list of active tasks."""
        try:
            col = await self._col()
            pipeline = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
            counts = {doc["_id"]: doc["count"] async for doc in col.aggregate(pipeline)}

            active_cursor = col.find(
                {"status": {"$in": ["downloading", "encoding", "uploading"]}},
                {"name": 1, "status": 1, "quals_done": 1, "retry_count": 1}
            ).sort("updated_at", -1).limit(5)
            active = await active_cursor.to_list(length=5)

            recent_cursor = col.find(
                {"status": "done"},
                {"name": 1, "quals_done": 1, "updated_at": 1}
            ).sort("updated_at", -1).limit(5)
            recent = await recent_cursor.to_list(length=5)

            failed_cursor = col.find(
                {"status": "failed"},
                {"name": 1, "error": 1, "retry_count": 1, "updated_at": 1}
            ).sort("updated_at", -1).limit(3)
            failed = await failed_cursor.to_list(length=3)

            return {
                "counts": counts,
                "active": active,
                "recent_done": recent,
                "recent_failed": failed,
            }
        except Exception:
            await rep.report(format_exc(), "error")
            return {"counts": {}, "active": [], "recent_done": [], "recent_failed": []}

    async def get_pending_by_priority(self) -> list[dict]:
        """Return pending tasks ordered by priority then age."""
        try:
            col = await self._col()
            cursor = col.find(
                {"status": "pending", "retry_count": {"$lt": MAX_RETRIES}},
                sort=[("source_priority", 1), ("created_at", 1)]
            )
            return await cursor.to_list(length=50)
        except Exception:
            return []


# Singleton
task_queue = PersistentTaskQueue()


class BatchTaskQueue(PersistentTaskQueue):
    """Persistent task queue for completed/batch anime — uses batch_db."""

    async def _col(self):
        from bot.core.database import batch_db
        if batch_db.db is None:
            await batch_db.connect()
        return batch_db.db["task_queue"]

    async def enqueue(self, name: str, torrent: str, source_priority: int = 1,
                      is_batch: bool = True,
                      alt_torrents: list | None = None) -> str | None:
        """Batch queue always sets is_batch=True by default."""
        return await super().enqueue(name, torrent, source_priority, is_batch=True,
                                     alt_torrents=alt_torrents)


batch_task_queue = BatchTaskQueue()


class MovieTaskQueue(PersistentTaskQueue):
    """Persistent task queue for movies — uses movie_db."""

    async def _col(self):
        from bot.core.database import movie_db
        if movie_db.db is None:
            await movie_db.connect()
        return movie_db.db["task_queue"]

    async def enqueue(self, name: str, torrent: str, source_priority: int = 1,
                      is_batch: bool = False) -> str | None:
        """Movie queue — is_batch always False (movies are single files)."""
        return await super().enqueue(name, torrent, source_priority, is_batch=False)


movie_task_queue = MovieTaskQueue()
