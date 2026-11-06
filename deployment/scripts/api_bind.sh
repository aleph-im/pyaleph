# Computes the gunicorn bind addresses of the API server. Meant to be sourced.

# Prints the space-separated list of gunicorn bind addresses for the given port.
#
# CCN_CONFIG_API_BIND (space-separated gunicorn addresses) wins when set. Otherwise we listen
# on IPv4 and, if the container has IPv6, on IPv6 too. In IPv6-first clusters pod IPs are IPv6
# and nothing would be reachable with a 0.0.0.0-only bind.
#
# Test hooks: CCN_IPV6_PROBE_FILE (default /proc/net/if_inet6) and
# CCN_BINDV6ONLY_FILE (default /proc/sys/net/ipv6/bindv6only).
function compute_api_binds() {
  local port="$1"
  local probe_file="${CCN_IPV6_PROBE_FILE:-/proc/net/if_inet6}"
  local v6only_file="${CCN_BINDV6ONLY_FILE:-/proc/sys/net/ipv6/bindv6only}"
  # Whitespace-only counts as unset: an empty bind list would make gunicorn fall back to 127.0.0.1:8000.
  local explicit_binds
  read -ra explicit_binds <<< "${CCN_CONFIG_API_BIND:-}"

  if [ "${#explicit_binds[@]}" -gt 0 ]; then
    echo "${explicit_binds[*]}"
  # /proc files report a size of 0, so test the content instead of using -s.
  elif [ -r "${probe_file}" ] && grep -q . "${probe_file}"; then
    # Binding 0.0.0.0 and [::] on the same port fails with EADDRINUSE because gunicorn does not
    # set IPV6_V6ONLY: the dual-stack [::] socket already accepts IPv4 (as ::ffff:a.b.c.d).
    # Only when the host forces v6-only sockets does [::] need a companion IPv4 bind.
    if [ -r "${v6only_file}" ] && [ "$(cat "${v6only_file}")" = "1" ]; then
      echo "0.0.0.0:${port} [::]:${port}"
    else
      echo "[::]:${port}"
    fi
  else
    echo "0.0.0.0:${port}"
  fi
}
