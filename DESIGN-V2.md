# Design v2 — Kamelet/Pipe GitOps with External Prometheus + Elasticsearch

> Status: **design** (agreed 2026-07-18). Supersedes the Helm-chart approach in
> ARCHITECTURE.md for route authoring; the Argo CD app-of-apps bootstrap and the
> Redis rate-limiter rationale carry over unchanged.
>
> Decisions locked with the user:
> 1. Routes are authored as **Pipe CRs referencing custom Kamelets** — no Helm,
>    no `_helpers.tpl`, no templating layer between Git and the cluster.
> 2. **Elasticsearch is external** to the cluster (like Prometheus); an
>    in-cluster Filebeat DaemonSet pushes logs out. No Loki.
> 3. One design that runs on **k3d now** and maps 1:1 onto the **air-gapped
>    production** topology later.

---

## 1. The shape of the system

```
                Git repo (single source of truth)
                        │  Argo CD (remote / mgmt side)
                        ▼
   ┌────────────────────────────────────────────────────┐
   │ cluster (k3d now, air-gapped later)                │
   │                                                    │
   │  kamelets/*  ──►  Kamelet CRs   (the vocabulary)   │
   │  routes/*    ──►  Pipe CRs      (one file = route) │
   │  platform/*  ──►  trait defaults, RBAC             │
   │  logging/*   ──►  Filebeat DaemonSet               │
   │                                                    │
   │  Camel K operator: Pipe → Integration → image →    │
   │  Deployment (metrics on :8080/q/metrics, JSON logs)│
   └───────────┬───────────────────────────┬────────────┘
        :6443 inbound                 :9200 outbound
   (Prometheus scrapes pods        (Filebeat pushes logs
    via kube-API proxy)             to Elasticsearch)
```

Two firewall openings total, both terminating at things you already run
outside the cluster:

| Direction | Port | Used by |
|---|---|---|
| mgmt → cluster API | 6443 | Argo CD sync **and** Prometheus scraping (same opening) |
| cluster → Elasticsearch | 9200 | Filebeat log shipping (the one *new* opening) |

---

## 2. Repo layout

```
camel-routes-gitops/
├── argocd/
│   ├── root-app.yaml            # app-of-apps bootstrap (applied once by hand — unchanged)
│   ├── project.yaml             # AppProject: this repo → camel-k ns; kinds: Kamelet, Pipe,
│   │                            #   IntegrationPlatform/Profile, RBAC, DaemonSet(logging ns)
│   └── apps/
│       ├── kamelets.yaml        # Application → kamelets/   (sync-wave 0)
│       ├── platform.yaml        # Application → platform/   (sync-wave 0)
│       ├── logging.yaml         # Application → logging/    (sync-wave 0)
│       └── routes.yaml          # Application → routes/     (sync-wave 1)
├── kamelets/                    # the vocabulary — written once, reviewed hard
│   ├── kafka-scram-source.kamelet.yaml
│   ├── kafka-scram-sink.kamelet.yaml
│   ├── mtls-http-source.kamelet.yaml    # platform-http, TLS+client-auth (inbound leg)
│   ├── mtls-http-sink.kamelet.yaml      # camel:http + client keystore (outbound leg)
│   └── rate-limit-action.kamelet.yaml   # wraps the Redis token bucket
├── routes/                      # ONE Pipe CR per route — the only day-to-day surface
│   └── orders-kk.yaml
├── platform/
│   ├── trait-defaults.yaml      # IntegrationPlatform/Profile: prometheus, json logging, health
│   └── prometheus-scraper-rbac.yaml  # SA + ClusterRole for external Prometheus (pods/proxy)
├── logging/
│   └── filebeat.yaml            # DaemonSet + ConfigMap (ES endpoint/API-key from a Secret)
└── external/                    # NOT applied by Argo — config you paste into external systems
    ├── prometheus-scrape.yaml   # scrape_configs for the external Prometheus
    ├── prometheus-alerts.yaml   # starter alert rules
    └── README.md
```

**Argo wiring change vs v1:** the ApplicationSet + per-route child Application
machinery goes away. Because routes are now plain CRs (no per-file Helm values),
a single Application with `directory: {recurse: true}` over `routes/` gives
add/change/delete-a-file semantics with `automated: {prune: true, selfHeal:
true}` — and per-route health is still visible per resource inside the app via
the custom health checks (§7). Fewer controllers, same UX. (If per-route Argo
Applications are ever wanted back, an ApplicationSet with a git files generator
works on plain manifests too — but it's not needed for correctness.)

Sync waves order kamelets/platform before routes so a brand-new cluster never
syncs a Pipe whose Kamelet doesn't exist yet.

---

## 3. Route authoring — a Pipe is the whole route

```yaml
# routes/orders-kk.yaml — complete file
apiVersion: camel.apache.org/v1
kind: Pipe
metadata:
  name: orders-kk
  namespace: camel-k
spec:
  source:
    ref: {apiVersion: camel.apache.org/v1, kind: Kamelet, name: kafka-scram-source}
    properties:
      topic: orders-in
      # brokers / secret overrides only when this side differs from defaults
  steps:
    - ref: {apiVersion: camel.apache.org/v1, kind: Kamelet, name: rate-limit-action}
      properties: {key: orders, rate: 10, burst: 20}   # mode defaults by source kind
  sink:
    ref: {apiVersion: camel.apache.org/v1, kind: Kamelet, name: kafka-scram-sink}
    properties:
      topic: orders-out
```

- No `routeType`, no inference logic: the route's shape *is* which Kamelets it
  references. All four combinations (kafka/http × kafka/http) fall out of
  composition, not a template switch.
- Two-different-Kafka-clusters routes: each side's Kamelet takes optional
  `brokers`/`credentialsSecret`/SASL overrides as properties, defaulted in the
  Kamelet definition — same capability as the old chart-wide merge, without the
  merge code.
- Shared rate-limit budgets: unchanged — same `key` on two Pipes = one Redis
  bucket = one aggregate SLA.
- The operator turns each Pipe into an Integration, so everything downstream
  (build via in-cluster Maven mirror + registry, one Deployment per route,
  independent blast radius) is identical to today.

## 4. The rate limiter becomes a versioned artifact, not an inline file

`RateLimit.java` stops being `.Files.Get`-injected source and becomes a tiny
jar, e.g. `mvn:io.avieli.camel:ratelimit-plugin:1.0.0`, published once to the
in-cluster **Maven mirror** (in the air-gap, it ships inside the bundle like
any other artifact). The Kamelet declares it:

```yaml
# kamelets/rate-limit-action.kamelet.yaml (essence)
apiVersion: camel.apache.org/v1
kind: Kamelet
metadata:
  name: rate-limit-action
  labels: {camel.apache.org/kamelet.type: action}
spec:
  definition:
    required: [key]
    properties:
      key:   {type: string}
      rate:  {type: number,  default: 10}
      burst: {type: integer, default: 20}
      mode:  {type: string,  default: block}   # block | reject
  dependencies:
    - mvn:io.avieli.camel:ratelimit-plugin:1.0.0
    - camel:kamelet
  template:
    beans:
      - name: rateLimit
        type: "#class:io.avieli.camel.RateLimit"
    from:
      uri: kamelet:source
      steps:
        - bean: {ref: rateLimit, method: "check('{{key}}', {{rate}}, {{burst}}, '{{mode}}')"}
```

Wins: the Java code is versioned/releasable independently of routes, appears in
no route file, and upgrading the limiter is bumping one Maven coordinate in one
Kamelet. The Redis token-bucket semantics (atomic Lua, block vs reject,
cross-route shared buckets) are unchanged — the "why Redis" rationale in
ARCHITECTURE.md §6 stands as-is.

## 5. mTLS model (unchanged semantics, new packaging)

Per the proven setup: **`platform-http` for the inbound leg, `camel:http` for
the outbound leg**; `quarkus.http.ssl.client-auth=REQUIRED` is a **build-time
property** so it lives in trait defaults / Kamelet-declared build properties,
not per-route config. `mtls-http-source` encapsulates the 8443 listener +
required client auth; `mtls-http-sink` encapsulates presenting the client
keystore + pinned truststore. TLS secrets stay pre-provisioned platform
resources mounted via the mount trait.

---

## 6. Built-in monitoring — zero per-route configuration

Trait defaults are set **once**, centrally, in `platform/trait-defaults.yaml`
(`IntegrationPlatform.spec.traits`, or an `IntegrationProfile` on Camel K 2.x),
so every route gets observability without its author ever mentioning it:

```yaml
traits:
  prometheus: {enabled: true, podMonitor: false}  # /q/metrics on :8080; no Prom operator in-cluster
  logging:    {json: true}                        # structured logs for Elasticsearch
  health:     {enabled: true}                     # Camel health → liveness/readiness probes
```

### 6a. External Prometheus scrapes pods through the kube-API proxy

Prometheus lives outside the cluster with no pod-network access — so it
discovers and scrapes **through the API server**, using the same :6443 opening
Argo already has. `platform/prometheus-scraper-rbac.yaml` creates a
`prometheus-scraper` ServiceAccount whose token can list pods and GET
`pods/proxy` in `camel-k` — nothing else.

```yaml
# external/prometheus-scrape.yaml — goes in the EXTERNAL Prometheus config
scrape_configs:
  - job_name: camel-routes
    scheme: https
    tls_config:    {ca_file: /etc/prometheus/k8s-ca.crt}     # k3d: insecure_skip_verify ok
    authorization: {credentials_file: /etc/prometheus/k8s-token}
    kubernetes_sd_configs:
      - role: pod
        api_server: https://<CLUSTER_API>:6443
        namespaces: {names: [camel-k]}
        tls_config:    {ca_file: /etc/prometheus/k8s-ca.crt}
        authorization: {credentials_file: /etc/prometheus/k8s-token}
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_camel_apache_org_integration]
        regex: (.+)
        action: keep                                  # only Camel K route pods
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: __metrics_path__
        replacement: /api/v1/namespaces/camel-k/pods/${1}:8080/proxy/q/metrics
      - target_label: __address__
        replacement: <CLUSTER_API>:6443               # scrape THROUGH the apiserver
      - source_labels: [__meta_kubernetes_pod_label_camel_apache_org_integration]
        target_label: route                           # every series labeled by route
```

On k3d, `<CLUSTER_API>` is the localhost port k3d publishes for the API server
— identical config, different host. New routes are picked up by discovery
automatically; **adding a route requires zero Prometheus changes**.

### 6b. Metrics contract + starter alerts

Every route exposes Micrometer/Camel metrics: `camel_exchanges_total`,
`camel_exchanges_failed_total`, `camel_exchange_processing_time_seconds_*`,
plus JVM/process metrics — all carrying the `route` label from relabeling.
`external/prometheus-alerts.yaml` ships starter rules:

- **RouteDown** — `up{job="camel-routes"} == 0` (per pod) for 2m.
- **RouteVanished** — a previously-seen `route` label absent 5m (deleted vs died).
- **RouteFailureRatio** — failed/total exchange rate > 5% for 5m.
- **RouteThrottledHard** — sustained rate-limit rejections (reject mode 429s).

## 7. Argo health — Pipe-aware

Custom Lua health checks in `argocd-cm` (bootstrap, alongside the existing
Integration one): `Pipe` phase `Ready` → Healthy, `Error` → Degraded, else
Progressing. Argo's UI/CLI then answers "is this route actually running"
per resource inside the `routes` Application.

---

## 8. Logging — Filebeat → external Elasticsearch (no Loki)

`logging/filebeat.yaml`: a Filebeat DaemonSet (Elastic-native shipper, ECS
fields out of the box), GitOps-managed like everything else. Because the
logging trait emits JSON, no fragile multiline/regex parsing exists anywhere.

```yaml
# essence of the Filebeat config
filebeat.autodiscover:
  providers:
    - type: kubernetes
      templates:
        - condition:                       # ship ONLY Camel route pods (+ operator if desired)
            has_fields: ['kubernetes.labels.camel_apache_org/integration']
          config:
            - type: container
              paths: ['/var/log/containers/*-${data.kubernetes.container.id}.log']
              processors:
                - decode_json_fields:
                    fields: [message]
                    target: ''
                    overwrite_keys: true   # Quarkus JSON → top-level ECS-ish fields
output.elasticsearch:
  hosts: ['https://<ES_HOST>:9200']
  api_key: '${ES_API_KEY}'                 # from Secret `es-credentials` (pre-provisioned)
  index: 'camel-routes-%{+yyyy.MM.dd}'     # or an ES data stream + ILM on the ES side
```

- Per-route log views in Kibana come free from the
  `kubernetes.labels.camel_apache_org/integration` field — filter by route,
  correlate with the `route` label on metrics.
- Retention/ILM is owned on the Elasticsearch side, not in-cluster.
- The ES endpoint + API key live in a pre-provisioned Secret (same category as
  Kafka SCRAM creds — platform runbook, not Git).
- Air-gap note: the Filebeat image ships in the registry bundle; the only
  network requirement is the cluster → ES :9200 opening.

---

## 9. What Argo manages vs. what stays on the platform runbook

| Argo-managed (this repo) | Pre-provisioned (bootstrap/air-gap runbook) |
|---|---|
| Kamelets, Pipes (routes) | Camel K operator, registry, Maven mirror (+ ratelimit jar) |
| Trait defaults, scraper RBAC | Kafka + SCRAM secret, Redis, TLS secrets, ES API-key secret |
| Filebeat DaemonSet | External Prometheus + Elasticsearch/Kibana themselves |

## 10. Day-2 operations

| Task | How |
|---|---|
| Add / change / delete a route | Add / edit / delete one Pipe file in `routes/`, push. |
| See if a route is really running | `argocd app get routes` (per-resource health) or `kubectl get pipe -n camel-k`. |
| Metrics for a new route | Automatic — discovery + relabeling, no Prometheus edit. |
| Logs for a route | Kibana filter on the integration label — automatic for new routes. |
| Change SASL/TLS/limiter behavior globally | Edit one Kamelet or trait-defaults file — every route rebuilds on next sync. |
| Upgrade the rate limiter | Publish new jar to the mirror, bump the version in `rate-limit-action.kamelet.yaml`. |
| Share a rate limit across routes | Same `rateLimit` `key` property on both Pipes. |

## 11. Migration & validation plan

1. Build + publish the `ratelimit-plugin` jar to the in-cluster Maven mirror
   (k3d first; add to air-gap bundle).
2. Write the five Kamelets; validate each with a throwaway Pipe via
   `kubectl apply` before any Argo involvement.
3. Restructure the repo per §2; keep the Helm chart on a branch until parity.
4. Port the three existing routes to Pipes; diff behavior (rate-limit test
   script, mTLS test) against the chart-deployed versions.
5. Swap Argo: remove ApplicationSet, add the four `argocd/apps/*` Applications;
   add the Pipe health check to `argocd-cm`.
6. Stand up external scraping on the k3d API port + Filebeat → your ES;
   confirm a brand-new route shows up in Prometheus and Kibana with **zero**
   config outside Git.
7. Production mapping: same repo; change the cluster endpoint in Argo, the
   `<CLUSTER_API>` in the Prometheus config, and open cluster → ES :9200.

## 12. Known risks / open items

- **Camel placeholder escaping** moves from Helm to Kamelet templates — Kamelet
  `{{property}}` substitution is native (no Helm double-escaping problem), but
  Camel secret placeholders like `{{kafka.user}}` inside Kamelet templates need
  the documented `{{?...}}`/raw forms verified during step 2 of the migration.
- **Build-time properties in Kamelets**: `client-auth=REQUIRED` must land as a
  build property; verify whether it's cleanest via trait defaults (global) or
  per-Kamelet `camel.apache.org/…` build-property annotations on the Pipe.
- **API-proxy scrape throughput** is fine for tens of routes; at hundreds,
  revisit (e.g. a single authenticated metrics ingress instead of pod proxy).
- **Pipe expressiveness ceiling**: Pipes are linear source→steps→sink. Today's
  routes fit exactly; if a future route needs branching/multicast, that one
  route can drop down to a plain Integration CR in the same `routes/` dir —
  Argo doesn't care about the kind.
