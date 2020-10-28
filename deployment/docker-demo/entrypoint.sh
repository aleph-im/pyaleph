#!/bin/bash

set -euo pipefail

# Initialize IPFS if it has not been done yet
if [ ! -f /var/lib/ipfs/config ]; then
  ipfs init
fi

/usr/bin/supervisord -c /etc/supervisor/supervisord.conf
