#!/usr/bin/env python3
"""Set or reset the dashboard login password.

    python3 set_password.py                 # prompts for username and password
    python3 set_password.py --user psardar  # prompts for the password only

In Docker:

    docker compose exec backend python /app/set_password.py

The password is prompted for, never passed as an argument, so it stays out of your
shell history and out of `ps`. Only a hash is stored, in configs/nvShares.db, which is
git-ignored — nothing secret reaches the repository.

Running this also signs out every browser that was already logged in.
"""
import argparse
import getpass
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
_BACKEND_DIR = os.path.join(_REPO_ROOT, "dashboard", "backend")
for _p in (_REPO_ROOT, _BACKEND_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from helpers import config  # noqa: E402
from services import auth  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description="Set the dashboard login password.")
    ap.add_argument("--user", help="Username (prompted for if omitted)")
    args = ap.parse_args()

    if not os.path.isfile(config.DB_PATH):
        sys.exit("Database not found: {}".format(config.DB_PATH))

    existing = auth.account()
    if existing:
        print("Resetting the password for '{}'.".format(existing["username"]))
    else:
        print("No account yet — creating one.")

    username = args.user or (existing or {}).get("username") or ""
    if not username:
        username = input("Username: ").strip()

    password = getpass.getpass("New password (min {} chars): ".format(auth.MIN_PASSWORD_LEN))
    if password != getpass.getpass("Confirm: "):
        sys.exit("Passwords do not match.")

    try:
        auth.set_password(username, password)
    except ValueError as e:
        sys.exit(str(e))

    print("Password set for '{}'. Any existing sessions were signed out.".format(username))


if __name__ == "__main__":
    main()
