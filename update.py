from os import path as opath, getenv
from logging import FileHandler, StreamHandler, INFO, basicConfig, error as log_error, info as log_info
from logging.handlers import RotatingFileHandler
from subprocess import run as srun
from dotenv import load_dotenv

if opath.exists("log.txt"):
    with open("log.txt", 'r+') as f:
        f.truncate(0)

basicConfig(format="[%(asctime)s] [%(name)s | %(levelname)s] - %(message)s [%(filename)s:%(lineno)d]",
            datefmt="%m/%d/%Y, %H:%M:%S %p",
            handlers=[FileHandler('log.txt'), StreamHandler()],
            level=INFO)

load_dotenv('config.env', override=True)

UPSTREAM_REPO = getenv('UPSTREAM_REPO')
UPSTREAM_BRANCH = getenv('UPSTREAM_BRANCH')

if UPSTREAM_REPO is not None:
    if opath.exists('.git'):
        srun(["rm", "-rf", ".git"])
        
    # FIX: was using `git config --global` with a hardcoded third-party email,
    # which permanently polluted the VPS's ~/.gitconfig. Now uses --local
    # (per-repo only, written to .git/config which we wipe each run anyway)
    # and a generic identity that doesn't leak anyone's real email address.
    update = srun([f"git init -q \
                     && git config --local user.email bot@local \
                     && git config --local user.name fixed-anime-bot \
                     && git add . \
                     && git commit -sm update -q \
                     && git remote add origin {UPSTREAM_REPO} \
                     && git fetch origin -q \
                     && git reset --hard origin/{UPSTREAM_BRANCH} -q"], shell=True)

    if update.returncode == 0:
        log_info('Successfully updated with latest commit from UPSTREAM_REPO')
    else:
        log_error('Something went wrong while updating, check UPSTREAM_REPO if valid or not!')
