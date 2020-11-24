#!/bin/bash

set -euo pipefail

# Initialize IPFS if it has not been done yet
if [ ! -f /var/lib/ipfs/config ]; then
  chown -R ipfs:ipfs /var/lib/ipfs
  su ipfs -c "/opt/go-ipfs/ipfs init"
fi

chown -R mongodb:mongodb /var/lib/mongodb
chown -R mongodb:mongodb /opt/pyaleph/data

/usr/bin/supervisord -c /etc/supervisor/supervisord.conf
