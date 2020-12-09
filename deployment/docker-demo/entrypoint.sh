#!/bin/bash

set -euo pipefail

# Initialize IPFS if it has not been done yet
if [ ! -f /var/lib/ipfs/config ]; then
  chown -R ipfs:ipfs /var/lib/ipfs
  su ipfs -c "/opt/go-ipfs/ipfs init --profile server"
fi

chown -R mongodb:mongodb /var/lib/mongodb

/usr/bin/supervisord -c /etc/supervisor/supervisord.conf
