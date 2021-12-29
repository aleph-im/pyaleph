# PyAleph Deployment with Monitoring

This directory contains a configuration to run a PyAleph node in production with Monitoring.
It is aimed at a starting point for node operators interested in easily getting pre-made basic 
metrics on their node. 

This directory contains the [Docker Compose](https://docs.docker.com/compose/) file
to run an Aleph Node in production using the official Docker images on [Docker Hub](https://hub.docker.com/)
with performance monitoring using [Prometheus](https://prometheus.io/) and [Grafana](https://grafana.com/).
[Caddy](https://caddyserver.com/) is used as a reverse proxy.

### Other links

See [../docker-build](../docker-build) to PyAleph without the monitoring.

See the [Docker-Compose documentation on readthedocs.io](https://pyaleph.readthedocs.io/en/latest/guides/docker-compose.html)
for the documentation.

See [../docker-build](../docker-build) to build your own image of PyAleph and run it with Docker-Compose.

## Configuration

### Password

Grafana is configured by default with a default username and insecure password, 
defined in the `docker-compose.yml` file. You are encouraged to change them.

### Hostnames

By default, Grafana will be available on HTTP, port 80.

For more secure setup, create a public domain name pointing to the server,
uncomment and edit the command to configure the Caddy reverse proxy in `docker-compose.yml`:
```
caddy reverse-proxy --from grafana.aleph-node.example.org --to grafana:3000
```
Where `grafana.aleph-node.example.org` should be replaced with your domain name.

Restart `docker-compose` with `docker-compose up -d`. 
Caddy should now expose Grafana using secure HTTPS.

### System metrics

Monitoring the performance of the host system requires installing `prometheus-node-exporter`.
On a Debian/Ubuntu system:

```shell
sudo apt-get install prometheus-node-exporter
```

**Security**: The default configuration of `prometheus-node-exporter` publishes the
metrics of the system publicly on HTTP port 9100. You mway want to use change this 
or use a firewall to restrict access to it.

## Dashboards

Two dashboards are provided out of the box: one to monitor the internals of the Aleph Node,
and one to monitor the performance of the host system.
