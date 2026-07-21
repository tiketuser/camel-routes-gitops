# Camel K GitOps Bridge — Architecture & Operations Guide

> Purpose of this document: a complete, self-contained explanation of this system —
> what it is, why every piece exists, how data flows through it, how to operate it
> day-to-day, and the reasoning behind the non-obvious design decisions (especially
> "why Redis and not a simple in-memory throttle"). Written to be handed to someone
> building a slide deck or onboarding a new engineer.

---

## 1. What this system is, in one paragraph

This is a **GitOps-managed message bridge platform** built on **Apache Camel K**
running on Kubernetes. Each "route" is a small, independent bridge that reads
messages from one place (Kafka topic or an HTTPS endpoint) and writes them to
another (Kafka topic or an HTTPS endpoint), enforcing a **distributed rate limit**
on the way through. Adding a new bridge is a one-file pull request — no code, no
manual `kubectl apply`. **Argo CD** watches the repo, renders a shared **Helm
chart**, and applies the resulting Kubernetes `Integration` custom resource; the
**Camel K operator** builds and runs it.

Two repositories exist:
- **`camel-routes-gitops`** (this repo) — the GitOps source of truth. Everything
  that changes route-to-route lives here as data (YAML), not code.
- **`camel-k-project-avieli`** — the platform/infrastructure repo: cluster
  bootstrap, monitoring stack, certificates, and the earlier hand-rolled version
  of the bridge (kept for reference / air-gap tooling).

---

## 2. Why it's built this way (the design motivations)

| Decision | Why |
|---|---|
| **One Integration CR per route**, not one giant multi-route Integration | Independent lifecycle — deploying/breaking route A cannot affect route B. Argo CD can report per-route health. Blast radius of a bad change is one file. |
| **Helm chart + data-only route files** | Adding a route is "write 15 lines of YAML," not "write a Java class." No PR review needs to touch code to add a bridge. |
| **Type inferred from `source:`/`sink:` shape**, no `routeType` enum | An enum has to be kept in sync with the actual config by hand and can drift out of sync (e.g. `routeType: kafka-kafka` but someone typo'd an `http:` block). Inferring the type from which key (`kafka:`/`http:`) is actually nested removes an entire class of "type says X but config says Y" bugs. |
| **GitOps (Argo CD) instead of a CI pipeline that runs `kubectl apply`** | Git becomes the single audit log and rollback mechanism. Self-heal means manual cluster drift (someone `kubectl edit`-ing a resource) gets silently reverted back to what's in Git — the cluster can never quietly diverge from the repo for long. |
| **Redis-backed distributed rate limiter, not in-process throttling** | See §6 — this is the single most important non-obvious design choice in the system. |
| **mTLS everywhere on the HTTP edges** | The HTTP ingress (`platform-http`) and the egress call to `echo-server` both require client certs, not just server TLS. This isn't a public API — it's a bridge between trusted internal systems, so both directions authenticate. |
| **Kafka SASL/SCRAM per side, independently configurable** | A route can bridge two *different* Kafka clusters (e.g. on-prem → cloud) with different credentials on each side — not just move data within one cluster. |

---

## 3. Repository layout

```
camel-routes-gitops/
├── routes/*.yaml                    # ONE FILE PER ROUTE — the only thing you touch day-to-day
├── charts/camel-route/
│   ├── values.yaml                  # chart-wide defaults (brokers, SASL, Redis host, TLS secrets...)
│   ├── templates/
│   │   ├── _helpers.tpl             # all the logic: kind inference, Kafka param merging, route YAML generation
│   │   └── integration.yaml         # the templated Integration CR
│   └── files/RateLimit.java         # the ONE Java file in the whole system (see §6)
├── argocd/
│   ├── root-app.yaml                # bootstrap: the only thing ever applied by hand
│   ├── project.yaml                 # AppProject — locks this repo to the camel-k namespace + Integration kind only
│   └── appset.yaml                  # ApplicationSet — one child Application per routes/*.yaml file
├── bootstrap/                       # one-time cluster bootstrap docs/secrets (Argo CD install, repo creds)
└── airgap-bundle/                   # packages the whole control plane for offline install
```

---

## 4. The GitOps mechanism (how a file becomes a running pod)

This is an **"app-of-apps"** pattern with three layers:

```
argocd/root-app.yaml  (applied ONCE, by hand)
        │  Argo CD syncs everything under argocd/, including itself
        ▼
┌────────────────────────────┬──────────────────────────────┐
│ argocd/project.yaml        │ argocd/appset.yaml           │
│ AppProject "camel-routes"  │ ApplicationSet "camel-routes"│
│ — locks scope to:          │ — git "files" generator      │
│   • this repo only         │   scans routes/*.yaml         │
│   • camel-k namespace only │ — for each file, generates    │
│   • Integration kind only  │   one child Application       │
└────────────────────────────┴──────────────┬───────────────┘
                                             ▼
                          Application "route-<name>"  (one per route file)
                             source: charts/camel-route (the Helm chart)
                             values: routes/<name>.yaml
                                             ▼
                          helm template charts/camel-route -f routes/<name>.yaml
                                             ▼
                             Integration CR "<name>" applied to camel-k namespace
                                             ▼
                          Camel K operator sees the CR → builds a container image
                          (via the in-cluster Maven mirror + registry) → runs it
                          as a normal Kubernetes Deployment
```

**To add a route:** write `routes/orders-kk.yaml`, push to `main`. Argo CD's
ApplicationSet controller polls the repo (default every 30s here, tuned down from
the default 3 minutes — see §9), notices the new file matching `routes/*.yaml`,
and materializes a brand new child `Application` object for it automatically —
**no one has to touch `appset.yaml`**. That Application then syncs the Helm chart
with the new file as its values, Argo CD applies the resulting `Integration`, and
the Camel K operator takes it from there.

**To change a route:** edit the file, push. `selfHeal: true` + `automated: {prune:
true, selfHeal: true}` on every generated Application means Argo re-syncs
automatically within one poll cycle, and if anyone manually edits the live
resource, Argo reverts it back to match Git on the next reconciliation.

**To delete a route:** delete the file, push. The ApplicationSet generator no
longer produces that child Application, Argo prunes it, and a **background
cascade finalizer** (`resources-finalizer.argocd.argoproj.io/background`) deletes
the underlying Integration and its Deployment/Pod. (Note: *foreground* cascade
deletion was tried and deadlocks — the Camel K operator keeps recreating the
Integration's Deployment while foreground GC is trying to tear the tree down from
the top; background cascade avoids that race.)

---

## 5. Anatomy of a route file

Every file in `routes/` is a Helm **values file** for the `camel-route` chart.
Nothing else — no imperative logic lives here, only data.

```yaml
name: orders-kk                    # Integration name (must be a valid k8s name)

source:
  kafka:                           # exactly one of kafka:/http: — the OTHER one is the sink's job
    topic: orders-in
    groupId: orders-group          # optional, defaults to "<name>-group"
    autoOffsetReset: earliest      # optional, defaults to "earliest"
    # brokers / securityProtocol / saslMechanism / credentialsSecret:
    #   each side can override the chart-wide kafka: defaults independently —
    #   this is how one route can bridge two different Kafka clusters

sink:
  kafka:
    topic: orders-out
    brokers: kafka-eu.otherteam.svc:9092      # this side's OWN cluster, different from source
    securityProtocol: SASL_SSL
    saslMechanism: SCRAM-SHA-512
    credentialsSecret: orders-kafka-creds     # must already exist in the camel-k namespace
    userKey: orders.user                      # only needed if this secret coexists with another
    passwordKey: orders.password               #   kafka side's secret — keys must be unique per pod

rateLimit:
  enabled: true
  key: orders                      # Redis bucket key — routes sharing a key share ONE global limit
  rate: 10.0                       # sustained tokens/sec
  burst: 20                        # bucket capacity (allows short bursts above the sustained rate)
  # mode: block|reject             # optional override — see §6 for the default-by-source-type rule
```

**There is no `routeType` field.** The chart looks at which key — `kafka:` or
`http:` — is nested under `source:` and under `sink:`, and derives the route's
"shape" from that pair (`camel-route.sourceKind` / `camel-route.sinkKind` /
`camel-route.type` in `_helpers.tpl`). Any of the four combinations works:
kafka→kafka, http→kafka, kafka→http, http→http. The derived shape (e.g.
`kafka-kafka`) is written back onto the Integration as the `camel-route/type`
label, purely for observability (`kubectl get integration -L camel-route/type`) —
it has zero effect on behavior.

A side's `kafka:` block is **merged over** the chart-wide `kafka:` defaults in
`values.yaml` (`mergeOverwrite` in `_helpers.tpl`), so you only specify what's
*different* from the default cluster. Preview any route's actual rendered
Kubernetes YAML before pushing: `helm template charts/camel-route -f
routes/<name>.yaml`.

---

## 6. Rate limiting — and why Redis, not in-process throttling

### The mechanism

Every route with `rateLimit.enabled: true` gets one extra step injected into its
Camel route: a call to a bean named `rateLimit`, defined once in
`files/RateLimit.java` — **the only Java file in the entire system**. Everything
else is declarative YAML.

The limiter is a classic **token bucket**, but the bucket's state (`tokens`,
`ts` — last-updated timestamp) lives as a Redis hash, and the refill/consume
logic runs as a single **atomic Lua script** executed server-side in Redis via
`EVAL`:

```lua
-- KEYS[1] = "ratelimit:<bucket>"; ARGV = rate, capacity, requested(=1)
now = Redis's own clock (TIME command)          -- not the caller's clock
tokens, ts = HMGET key 'tokens' 'ts'             -- read current state (or initialize full)
tokens = min(capacity, tokens + (now - ts) * rate)  -- refill continuously since last check
if tokens >= requested: tokens -= requested; allow = 1 else allow = 0
HSET key tokens ts                                -- write back the new state
EXPIRE key (idle buckets clean themselves up)
return allow
```

Two call styles, chosen automatically by the source type (overridable via
`rateLimit.mode`):

- **`http(...)` — reject mode**, default for HTTP-sourced routes. Over the
  limit → sets `429 Too Many Requests` + `Retry-After: 1`, stops the route. The
  caller decides whether to retry. Makes sense for HTTP: there's a live client
  waiting on a socket; you can't make it wait forever.
- **`block(...)` — backpressure mode**, default for Kafka-sourced routes. Over
  the limit → the consumer thread just `Thread.sleep(50)`s and retries in a
  loop until a token frees up. Nothing is dropped or rejected — the Kafka
  consumer simply stops polling for a while, which is exactly Kafka-native
  backpressure (messages queue up safely in the topic, not in the bridge).

`rateLimit.key` (the bucket name) is deliberately decoupled from the route name:
**two different routes can share one bucket** by giving them the same `key`,
producing one shared global SLA across both — e.g. an org-wide "no more than
50/s into this downstream system" limit enforced across three different source
routes that all feed it.

### Why Redis and not a simple in-process/in-memory throttle (e.g. Guava
RateLimiter, resilience4j, bucket4j-local)

This is the crux of the design and worth stating precisely, because "just use a
library rate limiter" is the obvious-looking wrong answer here:

1. **The limit has to be correct across multiple replicas.** An `Integration`
   isn't guaranteed to be a single pod forever — `replicas: N` is a one-line
   config change away, and Camel K/Kubernetes can and will run more than one pod
   for a route (rolling updates alone momentarily run 2). An in-memory token
   bucket is **per-JVM**. With 3 replicas each independently enforcing "10/s,"
   the *actual* aggregate throughput hitting the downstream system is up to
   30/s — the SLA silently triples with every replica added. A shared **Redis**
   bucket is the *only* value in the whole system every replica agrees on, so
   the limit is enforced on the **aggregate**, correctly, regardless of how many
   pods are running it.

2. **The limit has to be shared *across different routes*, not just replicas of
   one route.** `rateLimit.key` lets unrelated routes share a downstream budget
   (see above). That's structurally impossible with any in-process solution —
   there is no "process" that spans two separate Integrations/Deployments.
   Only an external, shared store makes a cross-route bucket possible.

3. **Correctness under restarts.** Camel K rebuilds and restarts pods routinely
   (new image, config change, crash, node drain). An in-memory bucket resets to
   "full" on every restart — a client that was legitimately rate-limited a
   second ago gets a completely fresh burst allowance for free just because the
   pod bounced, which is a real gap an attacker or a buggy retrying client can
   exploit. Redis state survives the bridge pod's restart untouched.

4. **A real, if secondary, concern: throttling (delay/queue) vs. rate
   *limiting* (admit/reject) are different problems, and "throttle" alone
   doesn't fit the HTTP side.** A naive throttle (e.g. a fixed-rate semaphore
   or a leaky-bucket queue that just delays excess requests) still needs
   *some* shared counter to know it's over budget in a multi-replica world —
   you can't avoid the distributed-state problem just by calling it a
   "throttle" instead of a "rate limiter." Once you accept you need shared
   state to throttle correctly across replicas, Redis (or something
   equivalent — a distributed cache) stops being optional.

5. **Cost is genuinely tiny.** One `EVAL` per message is a single round-trip to
   an in-memory data store doing O(1) hash reads/writes — sub-millisecond,
   negligible next to a Kafka round-trip or an HTTPS call. The "Redis is
   overkill for a rate limiter" instinct doesn't hold once you actually need
   correctness across replicas/routes/restarts; the alternative isn't "no
   infrastructure," it's "wrong answer under scale-out."

In short: **the moment a rate limit needs to be correct across more than one
process, it stops being a local-throttle problem and becomes a
distributed-consensus-on-a-counter problem** — and Redis with an atomic Lua
script is the simplest correct tool for that job, not a heavyweight one.

---

## 7. TLS / mTLS model

- **HTTP-sourced routes** terminate TLS in the pod itself on port `8443`
  (`quarkus.http.ssl-port`), using the `tls.secret` (server cert). Plain HTTP is
  explicitly disabled (`quarkus.http.insecure-requests=disabled`) — there is no
  unencrypted listener at all.
- **Client auth is required**, not optional: `quarkus.http.ssl.client-auth=REQUIRED`,
  trusting only certs signed by the CA in `tls.clientAuth.secret`
  (`bridge-client-ca`). A caller without a valid client cert never completes the
  TLS handshake — the request never even reaches route logic.
- **HTTP-sinked routes** (calling out to e.g. `echo-server`) go the other
  direction: the bridge presents its *own* client identity
  (`echoTls.secret` → `echoClientSSL` bean in `RateLimit.java`) and validates the
  destination's server cert against a pinned truststore. So the bridge
  authenticates itself outbound just as strictly as it demands inbound callers
  authenticate themselves.
- **Kafka sides** use SASL/SCRAM (not mTLS) — `SASL_PLAINTEXT` +
  `SCRAM-SHA-256` by default, credentials mounted from a Kubernetes secret
  (`credentialsSecret`) as Camel config, referenced via `{{kafka.user}}` /
  `{{kafka.password}}` placeholders resolved by Camel K from the secret's keys
  at runtime (never baked into the image or the Integration spec in plaintext).

---

## 8. What the Camel K operator actually builds and runs

The rendered `Integration` CR (see `templates/integration.yaml`) declares:
- **`dependencies:`** — Maven coordinates added only if needed: `camel:kafka` if
  either side touches Kafka, `camel:platform-http` if the source is HTTP,
  `camel:http` if the sink is HTTP, `mvn:redis.clients:jedis:5.2.0` if rate
  limiting or an HTTP sink needs the plugin class.
- **`traits.mount.configs`** — one secret mount per **distinct** Kafka
  credentials secret across both sides (deduplicated — a same-cluster route
  mounts one secret once, not twice).
- **`traits.mount.resources`** — TLS material mounted at fixed paths
  (`/etc/tls`, `/etc/tls-ca`, `/etc/echo-tls`) only when actually needed.
- **`sources:`** — always the templated route YAML (`camel-route.routeYaml`
  from `_helpers.tpl`); `RateLimit.java` is included only when the route
  actually needs the bean (rate limiting enabled, or an HTTP sink needing the
  `echoClientSSL` context).

The operator watches this CR, resolves dependencies against the in-cluster
**Maven mirror**, builds a container image against the in-cluster **registry**,
and runs the result as an ordinary Kubernetes `Deployment` — Argo CD never sees
or manages that Deployment directly; it only owns the `Integration` CR. Argo's
health status for the CR comes from a **custom Lua health check** registered in
`argocd-cm` at bootstrap: `Running` phase → Healthy, `Error` → Degraded,
anything else → Progressing. This is what makes `argocd app get route-x` (or the
UI) actually reflect whether the operator got the route running, not just
whether the YAML was applied.

---

## 9. Local dev / bootstrap topology

- **k3d** (k3s-in-Docker) single-node cluster, backed by **Colima** as the local
  Docker runtime on macOS.
- **Argo CD** installed via the upstream install manifest into its own
  namespace; reachable at `https://localhost:30503` via a NodePort published
  through k3d's load-balancer container.
- Argo is tuned for fast local iteration (not the multi-minute defaults):
  `ARGOCD_APPLICATIONSET_CONTROLLER_REQUEUE_AFTER=30s`,
  `timeout.reconciliation: 30s` (in `argocd-cm`),
  `ARGOCD_REPO_SERVER_REVISION_CACHE_EXPIRATION=30s` — push-to-live in roughly
  10–40 seconds. GitHub webhooks can't reach a local cluster (no inbound route
  from the internet), so this polling cadence *is* the sync trigger; a
  production deployment reachable from GitHub would add a push webhook instead
  and could rely on near-instant sync.
- An alternative **fully offline** path exists: `bootstrap/gitea.yaml` runs an
  in-cluster Gitea as the git source instead of GitHub, for the air-gapped
  variant (packaged wholesale by `airgap-bundle/`).
- Supporting infra the routes depend on but that Argo does **not** manage
  (provisioned separately, see the main repo's bootstrap docs): the Camel K
  operator + registry + Maven mirror, a Kafka broker + its SCRAM credentials
  secret, Redis, the various TLS secrets, and the `echo-server` test sink.

---

## 10. Quick reference — day 2 operations

| Task | How |
|---|---|
| Add a route | Write `routes/<name>.yaml`, push to `main`. |
| Change a route | Edit the file, push — self-heal picks it up automatically. |
| Delete a route | Delete the file, push — pruned + cascade-deleted automatically. |
| Preview a route's rendered manifest | `helm template charts/camel-route -f routes/<name>.yaml` |
| Share a rate limit across routes | Give them the same `rateLimit.key`. |
| Bridge two different Kafka clusters | Set different `brokers`/`credentialsSecret`/etc. under `source.kafka` vs `sink.kafka` — no chart change needed. |
| Check if the operator actually got a route running | `kubectl get integration <name> -n camel-k` (phase `Running`) or `argocd app get route-<name>` (health `Healthy`). |
| See a route's inferred type | `kubectl get integration <name> -n camel-k -L camel-route/type` |
