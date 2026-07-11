# camel-routes-gitops

GitOps repo for Camel K bridge routes. **Deploying a route = pushing one small YAML file
to `routes/`.** Argo CD auto-discovers it (ApplicationSet git-files generator), renders the
`charts/camel-route` Helm chart with it, and applies the resulting `Integration` CR; the
in-cluster Camel K operator builds and runs it. Deleting the file prunes the route.

## Layout

| Path | What |
|---|---|
| `charts/camel-route/` | Helm chart: one templated `Integration` per route, switching on `routeType`. Embeds `files/RateLimit.java` (Redis token-bucket limiter) when `rateLimit.enabled`. |
| `routes/*.yaml` | One values file per route — this is the only thing you touch day-to-day. |
| `argocd/root-app.yaml` | **Root "father" Application (app-of-apps)** — applied once by hand; from then on it GitOps-manages everything in `argocd/`, including itself. |
| `argocd/project.yaml` | AppProject `camel-routes` — locked to this repo, the `camel-k` namespace, and the `Integration` kind. |
| `argocd/appset.yaml` | ApplicationSet — git files generator over `routes/*.yaml`; the generated `route-*` Applications are the "sons": automated sync, prune, self-heal, retry with backoff, cascade-delete finalizers. |

## Add a route

Create `routes/<name>.yaml` and push:

```yaml
name: orders-kk
routeType: kafka-kafka        # kafka-kafka | https-kafka | https-https | mq-mq | https-mq | mq-https | mq-drain
source:
  topic: orders-in            # https-*: use httpPath: /ingest/orders instead; mq-*: use queue: DEV.X instead
sink:
  topic: orders-out           # https-https/mq-https: use url: http://svc/path instead; mq-*: use queue: DEV.X instead
rateLimit:
  enabled: true
  key: orders                 # routes sharing a key share one global limit
  rate: 10.0                  # sustained msg/s
  burst: 20
```

Defaults (brokers, SASL, Redis host, TLS secret, prometheus traits) live in
`charts/camel-route/values.yaml`. **Every route is its own Argo Application with its own
Integration**, so a route isn't limited to overriding just its topic/queue — it can point
at a completely different broker/queue manager with its own credentials by adding a
`kafka:`/`mq:` block to that route's file:

```yaml
name: orders-kk
routeType: kafka-kafka
source:
  topic: orders-in
sink:
  topic: orders-out
kafka:                                    # this route's own broker, not the shared default
  brokers: kafka-eu.otherteam.svc:9092
  securityProtocol: SASL_SSL
  saslMechanism: SCRAM-SHA-512
  credentialsSecret: orders-kafka-creds   # must exist in camel-k ns with kafka.user/kafka.password
```

```yaml
name: billing-mm
routeType: mq-mq
source:
  queue: BILL.SOURCE
sink:
  queue: BILL.SINK
mq:                                       # this route's own queue manager
  host: mq-billing.otherteam.svc
  port: 1414
  channel: BILLING.SVRCONN
  qmgr: QMBILL
  credentialsSecret: billing-mq-creds     # must exist in camel-k ns with mq.user/mq.password
```

Omit `kafka:`/`mq:` to fall back to the chart-wide default in `values.yaml`.
`rateLimit.mode` defaults by source: kafka/mq → `block` (backpressure), http → `reject` (429).

- **Change a route**: edit its file, push — self-heal syncs it.
- **Delete a route**: delete the file, push — prune removes the Integration.
- **Preview locally**: `helm template charts/camel-route -f routes/<name>.yaml`

## Bootstrap (local dev cluster, GitHub as the git source)

Source of truth: **https://github.com/TheRozom/camel-routes-gitops** (private).

Prerequisites on the cluster (not Argo-managed — see the main repo's air-gap runbook):
Camel K operator + registry + Maven mirror, Kafka + `kafka-scram-credentials` secret,
Redis (`redis` ns), `ratelimit-tls` secret, echo-server.

1. **Argo CD**: `kubectl create ns argocd && kubectl apply -n argocd --server-side -f
   https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml`
2. **Repo access** (private repo): create the repository credential secret — commands in
   `bootstrap/github-repo-secret.example.yaml` (token created imperatively, never committed).
3. **`kubectl apply -f argocd/root-app.yaml`** — the only manual apply, ever. The root
   app (father) then syncs `argocd/` from GitHub: the AppProject, the ApplicationSet, and
   itself; the ApplicationSet generates one `route-*` Application (son) per file in `routes/`.
4. Fast sync (~10-40s push→live): Argo is tuned to 30s reconciliation:
   `ARGOCD_APPLICATIONSET_CONTROLLER_REQUEUE_AFTER=30s` (appset controller env),
   `timeout.reconciliation: 30s` (argocd-cm), and
   `ARGOCD_REPO_SERVER_REVISION_CACHE_EXPIRATION=30s` (repo-server env).
   GitHub webhooks can't reach a local cluster (no inbound route), so polling is the
   trigger; when Argo runs somewhere GitHub can reach, add a push webhook to
   `https://<argocd-host>/api/webhook` for instant sync.
5. UI: exposed on NodePort → `https://localhost:30503` (admin /
   `kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d`).

### Alternative: fully in-cluster git (no internet)

`bootstrap/gitea.yaml` runs Gitea inside the cluster (push via `localhost:30501`, Argo
fetches via `gitea-http.git.svc.cluster.local:3000`) — useful for the air-gapped variant.
Caveats we hit, kept for the record: Gitea's `ROOT_URL` must match Argo's repoURL or
webhook payloads are ignored; Argo CD 3.4's Gitea webhook parser rejects Gitea 1.22–1.24
push payloads (`created_at` string vs int64), so polling is the trigger there too; and the
ApplicationSet controller's webhook handler fails silently if it starts before argocd-server
generates `server.secretkey` — restart it if the log shows `failed to create webhook handler`.

### Production / air-gapped variant

Run Argo CD on the management side with firewall openings: Argo → cluster API (:6443) and
Argo → git server. Register the cluster with a scoped ServiceAccount (Integration CRUD in
`camel-k` only) via a declarative cluster Secret, change `destination.server` in
`argocd/appset.yaml` + `project.yaml` to that cluster, and point `repoURL` at the real git
server.

### Integration health check (applied at bootstrap)

Route apps gate their health on the operator actually running the route (Running →
Healthy, Error → Degraded, else Progressing) via this `argocd-cm` entry:

```yaml
resource.customizations.health.camel.apache.org_Integration: |
  hs = {}
  if obj.status ~= nil and obj.status.phase == "Running" then
    hs.status = "Healthy"; hs.message = "Integration running"
  elseif obj.status ~= nil and obj.status.phase == "Error" then
    hs.status = "Degraded"; hs.message = "Integration in error"
  else
    hs.status = "Progressing"; hs.message = "Waiting for operator"
  end
  return hs
```
