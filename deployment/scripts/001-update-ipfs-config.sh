#!/bin/sh

# Kubo doc => https://github.com/ipfs/kubo/blob/master/docs/config.md

CONFIG_FILE="/data/ipfs/config"

if [ -f $CONFIG_FILE ]; then
    cp "$CONFIG_FILE" "$CONFIG_FILE.backup"
fi

echo "Updating IPFS config file..."

# Enable the V1+V2 service
ipfs config AutoNAT.ServiceMode 'enabled'

# Only announce recursively pinned CIDs
ipfs config Reprovider.Strategy 'pinned'

# ONLY use the Amino DHT (no HTTP routers).
ipfs config Routing.Type "dhtserver"

# Improve latency and read/write for large dataset
ipfs config Routing.AcceleratedDHTClient --json 'true'

# Aleph + Public Bootstrap peers
ipfs config Bootstrap --json '[
    "/ip4/51.159.57.71/tcp/4001/p2p/12D3KooWSdcuGvLfXgc6BPgDEqWYQirGpBWUmyXRwK5RmyM1T7Di",
    "/ip4/46.255.204.209/tcp/4001/p2p/12D3KooWHWNCn8t9NKQPBPZU61Fq6BoVw9XV37YsWTuMLwZXrEtj",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
    "/dnsaddr/va1.bootstrap.libp2p.io/p2p/12D3KooWKnDdG3iXw9eTFijk3EWSunZcFi54Zka4wmtqtt6rPxc8",
    "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
    "/ip4/104.131.131.82/udp/4001/quic-v1/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
]'

# soft upper limit to trigger GC
ipfs config Datastore.StorageMax '10GB'

# time duration specifying how frequently to run a garbage collection
ipfs config Datastore.GCPeriod '12h'

# Enable hole punching for NAT traversal when port forwarding is not possible
ipfs config Swarm.EnableHolePunching --json 'true'

# Disable providing /p2p-circuit v2 relay service to other peers on the network.
ipfs config Swarm.RelayService.Enabled --json 'false'

# Disable advertising networks (**Add your server provider network if you receive a netscan alert**) 
ipfs config Swarm.AddrFilters --json '[
    "/ip4/10.0.0.0/ipcidr/8",
    "/ip4/100.64.0.0/ipcidr/10",
    "/ip4/169.254.0.0/ipcidr/16",
    "/ip4/172.16.0.0/ipcidr/12",
    "/ip4/192.0.0.0/ipcidr/24",
    "/ip4/192.0.2.0/ipcidr/24",
    "/ip4/192.168.0.0/ipcidr/16",
    "/ip4/198.18.0.0/ipcidr/15",
    "/ip4/198.51.100.0/ipcidr/24",
    "/ip4/203.0.113.0/ipcidr/24",
    "/ip4/240.0.0.0/ipcidr/4",
    "/ip6/100::/ipcidr/64",
    "/ip6/2001:2::/ipcidr/48",
    "/ip6/2001:db8::/ipcidr/32",
    "/ip6/fc00::/ipcidr/7",
    "/ip6/fe80::/ipcidr/10",
    "/ip4/86.84.0.0/ipcidr/16"
]'

echo "IPFS config updated!"
