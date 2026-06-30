# Service Discovery with Consul — A Reflection

> This is a write-up of how I added Consul-based service discovery to the gateway
> in this project, why I made the choices I did, and the one subtle failure that
> taught me the most. I'm leaving it here as a reflection — partly as documentation,
> partly as a note to my future self. It's meant to be readable whether you're
> seeing service discovery for the first time or you've run it in production and
> just want to know what's wired up here.

---

## 1. The problem I was solving

Before this change, APISIX reached the backends through **hardcoded upstream nodes**
in `apisix/declarative.yaml`:

```yaml
upstream:
  name: listing-upstream
  scheme: grpc
  type: roundrobin
  nodes:
    - host: listing      # the compose service name
      port: 50051
      weight: 1
```

This works fine for a fixed, single-instance compose stack. But it bakes two
assumptions into a config file:

1. **There is exactly one instance**, reachable at a name I typed by hand.
2. **That instance is always healthy** — APISIX has no idea whether `listing` is
   actually up; it just forwards traffic and hopes.

The moment you imagine scaling `listing` to two replicas, or replacing an instance,
or wanting the gateway to stop sending traffic to a dead backend, the hardcoded list
stops being good enough. The address of *where* a service lives should be discovered
at runtime, not frozen in a routing file.

So the goal: **let services announce themselves, and let the gateway discover them
dynamically — including dropping ones that fail health checks.**

---

## 2. The architecture I landed on

I used **Consul** as the service registry and **self-registration** from each
service. The flow:

```text
                         ┌──────────────────────────┐
   (1) on startup        │         Consul           │
   listing/order  ──────▶│  registry + health checks │
   register {Name,IP,Port}└──────────┬───────────────┘
                                     │  (3) APISIX polls Consul
                                     │      every few seconds
                                     ▼
                          ┌──────────────────────────┐
   client ──HTTP──▶ 9080  │          APISIX          │──gRPC──▶ listing / order
                          │ discovery_type: consul   │         (at discovered IP)
                          │ service_name: "listing"  │
                          └──────────────────────────┘
   (2) Consul TCP-health-checks each instance and prunes dead ones
```

The pieces, concretely:

| Piece | Where | Role |
| --- | --- | --- |
| Consul agent | `docker-compose.yml` (`consul` service, `agent -dev`) | The registry + health checker |
| Self-registration | `src/consul.ts` | Each service `PUT`s itself into Consul on boot |
| Discovery config | `apisix/config.yaml` (`discovery.consul.servers`) | Tells APISIX where Consul lives |
| Upstream binding | `apisix/declarative.yaml` (`discovery_type` + `service_name`) | Tells APISIX *which* Consul service to resolve |

### 2.1 Why self-registration (and not a sidecar)?

The two common patterns are:

- **Self-registration** — the service calls the registry's API itself on startup
  and deregisters on shutdown.
- **Third-party / sidecar registration** — a separate agent (e.g. a Consul agent
  per host, or a sidecar container) watches the service and registers it.

I chose self-registration because it's the smallest amount of moving parts for a
learning project: no extra container per service, no orchestration glue, and the
registration logic is right there in the codebase where it's easy to read
(`src/consul.ts`). The tradeoff is that the application now has a dependency on the
registry's API and has to handle registration failures gracefully — which is why
`registerWithConsul` **logs but never throws**: a Consul hiccup should never take
down an otherwise-healthy gRPC server.

In a real production setup you'd often prefer the sidecar/agent model so that
registration survives an application that crashed before it could deregister — but
that's exactly the kind of complexity this project is deliberately avoiding.

### 2.2 Consul in `-dev` mode (and what that hides)

The compose file runs Consul as:

```yaml
consul:
  image: hashicorp/consul:latest
  command: agent -dev -client=0.0.0.0
  ports:
    - "8500:8500"
```

`-dev` mode is a single in-memory agent: no persistence, no clustering, no ACLs.
That's perfect for local learning, but it's worth naming what it papers over:

- **One shared agent.** Every service registers into the *same* agent. In a real
  Consul deployment each node runs its own agent and a service registering without
  an explicit address inherits that node's address. Here, because the agent is
  shared, a service that doesn't supply its own address has no sensible default —
  which becomes important in §4.
- **No persistence.** Restart Consul and the registry is empty until services
  re-register. Fine here; not fine in production.

---

## 3. How the binding actually works — the `name` is the join key

The single most important thing to understand is that one string ties three
separate places together. For the listing service that string is `"listing"`.

**(a) The service registers under that `Name`** — `src/consul.ts`, with
`CONSUL_SERVICE_NAME=listing` from compose:

```jsonc
{ "Name": "listing", "Address": "172.18.0.15", "Port": 50051, "Check": { ... } }
```

**(b) The APISIX upstream asks for that same name** — `apisix/declarative.yaml`:

```yaml
upstream:
  scheme: grpc
  type: roundrobin
  discovery_type: consul
  service_name: listing      # ← must equal the Consul Name
```

**(c) APISIX's discovery dump groups instances under that name** —
`GET http://localhost:9090/v1/discovery/consul/dump`:

```jsonc
{
  "listing": [                                  // ← the key IS the Name
    { "host": "172.18.0.15", "port": 50051, "weight": 1 }
  ]
}
```

If those three don't match **byte for byte**, the join silently breaks: APISIX
queries a name nothing registered under, gets an empty node list, and you get a
`503 no healthy upstream` (not an obvious error pointing at the typo).

### One easy trap: name ≠ OTEL service name

The Consul `Name` is intentionally the **compose service name** (`listing`,
`order`), **not** `OTEL_SERVICE_NAME` (which is `listing-service`,
`order-service`). They look similar enough that "fixing" one to match the other is
a tempting mistake — and it would break discovery, because `declarative.yaml` says
`service_name: listing`. `src/consul.ts` carries a comment calling this out
specifically.

---

## 4. The decision that actually mattered: register an **IP**, not a hostname

This is the part I got wrong first, and the part most worth writing down.

My initial `consul.ts` defaulted the registered `Address` to the service **name**:

```ts
const address = reg.address ?? reg.name;   // "listing"
```

Everything *looked* healthy — Consul showed both services PASSING — but every call
through the gateway returned **502 Bad Gateway**. The APISIX error log gave it away:

```
balancer.lua:388: run(): failed to set server peer [listing:50051]
  err: no host allowed while connecting to upstream
```

### Why it failed

When an upstream uses `discovery_type: consul`, APISIX takes whatever `host` Consul
returns and hands it straight to the nginx balancer (`set_current_peer`). **That
function accepts IP literals only.** APISIX does *not* run discovery-provided
hostnames through DNS resolution — it assumes a service registry already contains
resolved addresses.

The discovery dump confirmed exactly what APISIX was trying to dial:

```jsonc
{ "listing": [ { "host": "listing", "port": 50051 } ] }   // a name, not an IP
```

### Why "just add a DNS resolver" doesn't fix it

This was the tempting shortcut, and it's wrong. I checked, and **APISIX already has
a resolver configured** — the generated `nginx.conf` contains
`resolver 127.0.0.11` and `dns_resolver = { "127.0.0.11" }`, auto-picked from
Docker's `/etc/resolv.conf`. The resolver is present; the discovery path simply
never uses it. Adding `dns_resolver` to `apisix/config.yaml` changes nothing.

### Why it worked *before* the swap (and why that misled me)

The old static `nodes: [{ host: listing }]` config went through a **different code
path** — APISIX *does* resolve domain names for statically-configured upstream
nodes. Service discovery skips that step entirely. So "it used to work with a
hostname" was true and irrelevant.

### Why the hostname resolves *everywhere else* (the real red herring)

On the shared Docker network, `listing` resolves fine — Consul's health check dials
`listing:50051` and passes, and the `order → listing` direct gRPC call dials
`listing:50051` and works. But those consumers (Consul, the gRPC client) **do their
own DNS resolution**. The APISIX discovery balancer is the one consumer that
doesn't, so it's the one that needs a pre-resolved address.

### The fix

Register the container's own routable IPv4 instead of the name —
`src/consul.ts`:

```ts
function localIPv4(): string | undefined {
  for (const iface of Object.values(networkInterfaces())) {
    for (const ni of iface ?? []) {
      if (ni.family === "IPv4" && !ni.internal) return ni.address;
    }
  }
  return undefined;
}

// ...
const address = reg.address ?? localIPv4() ?? reg.name;
```

After this, the dump shows `"host": "172.18.0.15"`, the balancer dials an IP, and
the 502 is gone. The IP is still a `172.x` address *on the same Docker network* —
I'm just handing APISIX the resolved form of the same endpoint.

### The mental model I walked away with

> **Service registry = "where is this instance, concretely?"** It should hold an
> `IP:port`, not a name. Registering a *name* and offloading resolution to each
> consumer is the unusual path — and it breaks on any consumer that can't resolve.
>
> **Rule of thumb:** with **service discovery**, register an **IP**. With **static
> upstreams**, a **hostname** is fine.

`Name` answers *which* service. `Address` answers *where* that instance is. The fix
was purely about the second one.

---

## 5. Health checks and lifecycle

Each registration includes a TCP health check (`src/consul.ts`):

```jsonc
"Check": {
  "TCP": "172.18.0.15:50051",
  "Interval": "10s",
  "DeregisterCriticalServiceAfter": "1m"
}
```

- Consul opens a TCP connection to the gRPC port every 10s. If the service is down,
  the check goes critical, APISIX stops getting it as a node, and after 1 minute of
  failing Consul removes the registration entirely.
- On graceful shutdown, the service deregisters itself. `SIGTERM`/`SIGINT` are wired
  to a `shutdown()` that calls the deregister function returned by
  `registerWithConsul` *before* closing the gRPC server.

### A real wrinkle: stale registrations

While debugging the IP fix, I rebuilt the services and noticed Consul briefly held
**two** instances per service — the new IP-based one and an old hostname-based one
left from a previous container generation. The old one stayed `passing` because its
health check (`listing:50051`) resolved via Docker DNS to the *new* container, so
Consul never pruned it — and APISIX round-robined onto the stale (un-dialable) node.

Two lessons there:

1. **Ungraceful shutdown leaves orphans.** If a container is `SIGKILL`ed (e.g. the
   compose stop-grace window expires while `server.tryShutdown()` waits on in-flight
   RPCs), `deregister` never runs. The TCP-check + `DeregisterCriticalServiceAfter`
   is the safety net, but it only fires if the check actually starts failing — which
   a hostname check won't if the name now points at a live replacement.
2. **IP-based health checks self-clean better.** A check pinned to a dead instance's
   *IP* goes critical and gets pruned; a check pinned to a shared *hostname* can be
   kept alive by an unrelated instance. Another quiet point in favor of registering
   IPs.

If you ever see intermittent (not constant) 502s after a redeploy, suspect a stale
registration and check the discovery dump for an extra node.

---

## 6. How to verify and debug it

All read-only. These are the exact checks I used.

```bash
# What Consul thinks is registered (expect an IP in ServiceAddress, not a name):
curl -s http://localhost:8500/v1/catalog/service/listing | jq '.[] | {ServiceAddress, ServicePort}'

# Health status of every instance:
curl -s http://localhost:8500/v1/health/service/order | jq '.[] | {addr: .Service.Address, checks: [.Checks[].Status]}'

# Every raw registration in the agent (great for spotting stale orphans):
curl -s http://localhost:8500/v1/agent/services | jq 'to_entries[] | {ID: .value.ID, Service: .value.Service, Address: .value.Address}'

# What APISIX actually resolved — the single most useful command here.
# host values MUST be IPs, and there should be one node per healthy instance:
curl -s http://localhost:9090/v1/discovery/consul/dump | jq '.services'

# Confirm the upstream is discovery-backed (no static nodes). 9180 isn't published
# to the host, so query it from inside the apisix container:
KEY=edd1c9f034335f136f87ad84b625c8f1
docker compose exec apisix sh -c \
  "curl -s -H 'X-API-KEY: $KEY' http://127.0.0.1:9180/apisix/admin/upstreams" \
  | jq '.list[].value | {name, discovery_type, service_name, nodes}'

# The smoking gun when it's broken:
docker compose exec apisix tail -n 50 /usr/local/apisix/logs/error.log | grep -i "no host allowed"
```

A quick triage table:

| Symptom | Likely cause |
| --- | --- |
| `502` + `no host allowed` in error log | Discovery returned a hostname; register an IP (§4) |
| `503 no healthy upstream` | `service_name` ≠ Consul `Name`, or nothing registered/passing (§3) |
| Intermittent `502` after redeploy | Stale registration round-robined into the pool (§5) |
| Discovery dump empty | APISIX can't reach Consul (`discovery.consul.servers`), or name mismatch |

---

## 7. Tradeoffs and limitations (read before copying this anywhere serious)

- **Self-registration couples the app to the registry.** Mitigated by never throwing
  on registration failure, but a crash before deregister still leaves an orphan
  until the health check prunes it.
- **`-dev` Consul is single-node and in-memory.** No HA, no persistence, no ACLs, no
  TLS. Production Consul is a different operational story.
- **Registered IPs are ephemeral.** A recreated container gets a new `172.x` IP and
  re-registers — fine because re-registration is automatic, but it means the
  registry's contents are only meaningful while instances are alive.
- **No mTLS / no ACL token.** `discovery.consul.servers` here is plain HTTP with an
  empty token. Real deployments should authenticate to Consul.
- **APISIX polls, it doesn't subscribe.** The dump showed `fetch_interval: 3` —
  there's a few-seconds window between a health change and APISIX acting on it.
  Acceptable here; worth knowing.
- **This only changed the gateway path.** Service-to-service calls (`order → listing`)
  still use a direct address from `LISTING_SERVICE_ADDRESS` env, *not* discovery. So
  the registry is currently consumed only by APISIX. Migrating internal calls to
  discovery too would be a natural next step — and would inherit the same IP-vs-name
  lesson.

---

## 8. What I'd tell someone picking this up

1. **The name is the contract.** `service_name` in the gateway config, `Name` in the
   registration, and the key in the discovery dump are the same string. Start
   debugging there.
2. **Discovery wants addresses, not names.** If a consumer dials at the balancer
   layer, it needs an IP. Don't reach for a DNS resolver — reach for the registry.
3. **`passing` in Consul is necessary, not sufficient.** It tells you the instance is
   reachable; it tells you nothing about whether the *address APISIX received* is
   dialable. Always check the discovery dump, not just Consul health.
4. **Graceful shutdown is part of discovery.** Registration without reliable
   deregistration accumulates ghosts. Wire your signal handlers, and lean on
   IP-based health checks to clean up the rest.

---

### File map

| Concern | File |
| --- | --- |
| Consul agent + service env (`CONSUL_HTTP_ADDR`, `CONSUL_SERVICE_NAME`) | `docker-compose.yml` |
| Self-registration + health check + IP detection | `src/consul.ts` |
| Registration call sites | `src/listing-service.ts`, `src/order-service.ts` |
| APISIX → Consul wiring | `apisix/config.yaml` (`discovery.consul`) |
| Upstream discovery binding | `apisix/declarative.yaml` (`discovery_type`, `service_name`) |
