#!/bin/bash
# The container runs as the host uid so that everything it writes under ./data
# belongs to the host user. That uid has no passwd entry inside the image, and
# Hadoop's login module refuses to start without a username. nss_wrapper fakes
# the entry, the same trick the official Spark entrypoint uses for OpenShift.
set -eo pipefail

if ! getent passwd "$(id -u)" >/dev/null 2>&1; then
  for wrapper in /usr/lib/*/libnss_wrapper.so /usr/lib/libnss_wrapper.so; do
    if [ -s "$wrapper" ]; then
      NSS_WRAPPER_PASSWD="$(mktemp)"
      NSS_WRAPPER_GROUP="$(mktemp)"
      printf 'foz:x:%s:%s:foz:/tmp:/bin/false\n' "$(id -u)" "$(id -g)" > "$NSS_WRAPPER_PASSWD"
      printf 'foz:x:%s:\n' "$(id -g)" > "$NSS_WRAPPER_GROUP"
      export LD_PRELOAD="$wrapper" NSS_WRAPPER_PASSWD NSS_WRAPPER_GROUP
      break
    fi
  done
fi

exec /usr/bin/tini -s -- /opt/spark/bin/spark-submit "$@"
