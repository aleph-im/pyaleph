#!/bin/sh
set -eu

# Kubo doc => https://github.com/ipfs/kubo/blob/master/docs/config.md

CONFIG_FILE="/data/ipfs/config"

# Apply one config key, recording instead of aborting when kubo rejects it, so
# a key removed by a future kubo release cannot stop the remaining settings from
# being applied (that is how a single stale key broke startup before). Failures
# are tallied and the script exits non-zero at the end, so a node never quietly
# runs on defaults.
config_failures=0
set_config() {
    if ! ipfs config "$@"; then
        echo "ERROR: failed to apply ipfs config $*" >&2
        config_failures=$((config_failures + 1))
    fi
}

if [ -f "$CONFIG_FILE" ]; then
    cp "$CONFIG_FILE" "$CONFIG_FILE.backup"
fi

echo "Updating IPFS config file..."

# Only announce recursively pinned CIDs. The reprovide strategy key changed in
# kubo v0.38.0: Reprovider/Provider were deprecated and superseded by the
# unified Provide key. v0.43 refuses to start when the deprecated keys carry
# values (an older version of this script set Reprovider.Strategy), but tolerates
# them empty, so blank them there. On older kubo those keys are still ACTIVE and
# must not be erased. Repo-format migration is handled by the daemon's --migrate.
#
# Assume the current (>= v0.38) layout unless the version string clearly says
# otherwise: an unparseable version, or any kubo 1.x+, must not fall through to
# the legacy branch and re-add keys that stop a modern daemon from starting.
kubo_version=$(ipfs version --number 2>/dev/null || true)
kubo_major=$(printf '%s' "$kubo_version" | cut -d. -f1)
kubo_minor=$(printf '%s' "$kubo_version" | cut -d. -f2)
case "$kubo_major" in
    ''|*[!0-9]*) kubo_major=0 ;;
esac
case "$kubo_minor" in
    ''|*[!0-9]*) kubo_minor=38 ;;
esac

if [ "$kubo_major" -gt 0 ] || [ "$kubo_minor" -ge 38 ]; then
    # Carry the deprecated values over to their Provide equivalents before
    # blanking, so a node that tuned them does not silently lose the setting.
    # Mapping matches kubo's own fs-repo-17-to-18 migration:
    # Provider.Enabled -> Provide.Enabled, Provider.WorkerCount ->
    # Provide.DHT.MaxWorkers, Reprovider.Interval -> Provide.DHT.Interval.
    # (Provider.Strategy is intentionally not carried: the migration skips it as
    # unused. Reprovider.Strategy is superseded by the Provide.Strategy set
    # below.)
    provider_enabled=$(ipfs config Provider.Enabled 2>/dev/null || true)
    provider_workers=$(ipfs config Provider.WorkerCount 2>/dev/null || true)
    reprovider_interval=$(ipfs config Reprovider.Interval 2>/dev/null || true)

    set_config --json Reprovider '{}'
    set_config --json Provider '{}'

    case "$provider_enabled" in
        true|false) set_config --json Provide.Enabled "$provider_enabled" ;;
    esac
    case "$provider_workers" in
        ''|*[!0-9]*) ;;
        *) set_config --json Provide.DHT.MaxWorkers "$provider_workers" ;;
    esac
    # Interval is a duration string (e.g. "22h"), so only skip empty/null.
    case "$reprovider_interval" in
        ''|null) ;;
        *) set_config Provide.DHT.Interval "$reprovider_interval" ;;
    esac

    set_config Provide.Strategy 'pinned'
else
    set_config Reprovider.Strategy 'pinned'
fi

# Enable the V1+V2 service
set_config AutoNAT.ServiceMode 'enabled'

# CCNs propagate messages over pubsub, so the daemon must have it enabled
set_config Pubsub.Enabled --json 'true'

# ONLY use the Amino DHT (no HTTP routers).
set_config Routing.Type "dhtserver"

# Improve latency and read/write for large dataset
set_config Routing.AcceleratedDHTClient --json 'true'

# Aleph + Public Bootstrap peers
set_config Bootstrap --json '[
    "/ip4/51.159.57.71/tcp/4001/p2p/12D3KooWSdcuGvLfXgc6BPgDEqWYQirGpBWUmyXRwK5RmyM1T7Di",
    "/ip4/46.255.204.209/tcp/4001/p2p/12D3KooWHWNCn8t9NKQPBPZU61Fq6BoVw9XV37YsWTuMLwZXrEtj",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
    "/dnsaddr/va1.bootstrap.libp2p.io/p2p/12D3KooWKnDdG3iXw9eTFijk3EWSunZcFi54Zka4wmtqtt6rPxc8",
    "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
    "/ip4/104.131.131.82/udp/4001/quic-v1/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
]'

# soft upper limit to trigger GC
set_config Datastore.StorageMax '10GB'

# time duration specifying how frequently to run a garbage collection
set_config Datastore.GCPeriod '12h'

# Enable hole punching for NAT traversal when port forwarding is not possible
set_config Swarm.EnableHolePunching --json 'true'

# Disable providing /p2p-circuit v2 relay service to other peers on the network.
set_config Swarm.RelayService.Enabled --json 'false'

# Disable advertising networks (**Add your server provider network if you receive a netscan alert**)
set_config Swarm.AddrFilters --json '[
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

if [ "$config_failures" -ne 0 ]; then
    echo "IPFS config update FAILED for $config_failures key(s)" >&2
    exit 1
fi

echo "IPFS config updated!"
