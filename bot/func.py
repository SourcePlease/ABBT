import asyncio
import json
import os
import math
import re

from pathlib import Path

import aiofiles
import aiohttp

OK = {}


async def genss(file):
    # FIX #11: was using subprocess.Popen().communicate() which blocks the event
    # loop. Now uses asyncio.create_subprocess_exec so it's fully non-blocking.
    process = await asyncio.create_subprocess_exec(
        "mediainfo", file, "--Output=JSON",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await process.communicate()
    out = stdout.decode().strip()
    z = json.loads(out)
    p = z["media"]["track"][0]["Duration"]
    # FIX #7: Original used `int(p.split(".")[-2])` which raises IndexError
    # when the duration has no decimal point (e.g. "120" → split gives ["120"],
    # so [-2] is out of range). `int(float(p))` handles both "1234.567" and "120"
    # correctly and also tolerates leading/trailing whitespace.
    return int(float(p))


async def duration_s(file):
    tsec = await genss(file)
    x = round(tsec / 5)
    y = round(tsec / 5 + 30)
    pin = convertTime(x)
    if y < tsec:
        pon = convertTime(y)
    else:
        pon = convertTime(tsec)
    return pin, pon


def convertTime(s: int) -> str:
    m, s = divmod(int(s), 60)
    hr, m = divmod(m, 60)
    days, hr = divmod(hr, 24)
    convertedTime = (f"{int(days)}d, " if days else "") + \
                    (f"{int(hr)}h, " if hr else "") + \
                    (f"{int(m)}m, " if m else "") + \
                    (f"{int(s)}s, " if s else "")
    return convertedTime[:-2]


async def gen_ss_sam(hash, filename, log):
    try:
        ss_path, sp_path = None, None
        os.mkdir(hash)
        tsec = await genss(filename)
        fps = 10 / tsec
        ncmd = f"ffmpeg -i '{filename}' -vf fps={fps} -vframes 10 '{hash}/pic%01d.png'"
        process = await asyncio.create_subprocess_shell(
            ncmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await process.communicate()
        ss, dd = await duration_s(filename)

        # FIX #7: Original used filename.split(".mkv")[-2] which raises IndexError
        # for non-mkv files (mp4, avi, etc.). Use rsplit with maxsplit=1 instead
        # to correctly strip any extension regardless of type.
        base = filename.rsplit(".", 1)[0]
        out = base + "_sample.mkv"

        _ncmd = f'ffmpeg -i """{filename}""" -preset ultrafast -ss {ss} -to {dd} -c:v copy -crf 27 -map 0:v -c:a aac -map 0:a -c:s copy -map 0:s? """{out}""" -y'
        process = await asyncio.create_subprocess_shell(
            _ncmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        er = stderr.decode().strip()
        # FIX #6: Original had `except Exception:` (bare, no `as e`) followed by
        # `print(e)`, which raises a secondary NameError because `e` is undefined
        # in a bare except clause, completely masking the original error.
        try:
            if er:
                if not os.path.exists(out) or os.path.getsize(out) == 0:
                    log.error(str(er))
                    return (ss_path, sp_path)
        except Exception as e:
            # FIX: was print(e) — debug spam to stdout that didn't go through
            # the configured logger, didn't include a timestamp, and never
            # reached the log channel. Route through the module logger.
            log.error(f"sample-clip postprocess: {e}")
        return hash, out
    except Exception as err:
        log.error(str(err))
