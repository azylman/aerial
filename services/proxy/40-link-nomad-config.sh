#!/bin/sh
set -e

if [ -s /local/default.conf ]; then
    ln -sf /local/default.conf /etc/nginx/conf.d/default.conf
fi
