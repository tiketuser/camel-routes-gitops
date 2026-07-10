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
routeType: kafka-kafka        # kafka-kafka | https-kafka | https-https
source:
  topic: orders-in            # https-*: use httpPath: /ingest/orders instead
sink:
  topic: orders-out           # https-https: use url: http://svc/path instead
rateLimit:
  enabled: true
  key: orders                 # routes sharing a key share one global limit
  rate: 10.0                  # sustained msg/s
  burst: 20
```

Defaults (brokers, SASL, Redis host, TLS secret, prometheus traits) live in
`charts/camel-route/values.yaml`. Override any of them per route.
`rateLimit.mode` defaults by source: kafka → `block` (backpressure), http → `reject` (429).

- **Change a route**: edit its file, push — self-heal syncs it.
- **Delete a route**: delete the file, push — prune removes the Integration.
- **Preview locally**: `helm template charts/camel-route -f routes/<name>.yaml`

## Bootstrap (local dev cluster, everything in Docker/k3s)

Prerequisites on the cluster (not Argo-managed — see the main repo's air-gap runbook):
Camel K operator + registry + Maven mirror, Kafka + `kafka-scram-credentials` secret,
Redis (`redis` ns), `ratelimit-tls` secret, echo-server.

1. **Gitea** (in-cluster git server): `kubectl apply -f bootstrap/gitea.yaml`, then push this
   repo to `http://localhost:30501/gitops/camel-routes-gitops.git` (user `gitops`).
2. **Argo CD**: `kubectl create ns argocd && kubectl apply -n argocd -f
   https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml`
3. Register the repo (public repos need nothing; ours is created public).
4. **`kubectl apply -f argocd/root-app.yaml`** — the only manual apply, ever. The root
   app (father) then syncs `argocd/` from git: the AppProject, the ApplicationSet, and
   itself; the ApplicationSet generates one `route-*` Application (son) per file in `routes/`.
5. Fast sync (~10s push→Application): Argo is tuned to 30s reconciliation:
   `ARGOCD_APPLICATIONSET_CONTROLLER_REQUEUE_AFTER=30s` (appset controller env),
   `timeout.reconciliation: 30s` (argocd-cm), and
   `ARGOCD_REPO_SERVER_REVISION_CACHE_EXPIRATION=30s` (repo-server env).
   Gitea push webhooks to
   `http://argocd-applicationset-controller.argocd.svc.cluster.local:7000/api/webhook` and
   `http://argocd-server.argocd.svc.cluster.local/api/webhook` are also registered
   (Gitea needs `GITEA__webhook__ALLOWED_HOST_LIST=private,loopback` and a ROOT_URL that
   matches the Argo repoURL), **but** Argo CD 3.4's Gitea webhook parser currently rejects
   Gitea 1.22–1.24 payloads (`cannot unmarshal string into ... created_at of type int64`),
   so polling is the effective trigger until that upstream bug is fixed. Note also: the
   ApplicationSet controller must start *after* argocd-server has generated
   `server.secretkey`, or its webhook handler silently fails — restart it if the startup
   log shows `failed to create webhook handler`.
6. UI: exposed on NodePort → `https://localhost:30503` (admin /
   `kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d`).

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
