#!/bin/sh
set -e

# Ensure /local/default.conf exists for the user-route `include /local/default.conf;` directive.
# If a legacy monolithic config (containing top-level `log_format`) is still mounted during cutover,
# clear it so nginx starts cleanly until Hangar syncs the location-only user overlay.
if [ -f /local/default.conf ] && grep -q "log_format " /local/default.conf; then
    : > /local/default.conf
fi
touch /local/default.conf
