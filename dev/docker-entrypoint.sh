#!/bin/bash
set -eo pipefail

if [[ -n "$CONTAINER_UID" ]] && [[ -n "$CONTAINER_GID" ]]; then
    # Create group and user only if they don't exist
    [[ ! $(getent group tdp) ]] && groupadd --gid "$CONTAINER_GID" --system tdp
    if [[ ! $(getent passwd tdp) ]]; then
        useradd --uid "$CONTAINER_UID" --system --gid tdp --home-dir /home/tdp tdp
        # Avoid useradd warning if home dir already exists by making home dir ourselves.
        # Home dir can exists if mounted via "docker run -v ...:/home/tdp/...".
        chown tdp:tdp /home/tdp
        gosu tdp cp -r /etc/skel/. /home/tdp
    fi
    # Avoid changing dir if a work dir is specified
    [[ "$PWD" == "/root" ]] && cd /home/tdp
    if [[ -z "$@" ]]; then
        exec gosu tdp /bin/bash
    else
        exec gosu tdp "$@"
    fi
fi

exec "$@"
