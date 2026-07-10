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
| `argocd/project.yaml` | AppProject `camel-routes` — locked to this repo, the `camel-k` namespace, and the `Integration` kind. |
| `argocd/appset.yaml` | ApplicationSet — git files generator over `routes/*.yaml`, automated sync with prune + self-heal. |

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
4. `kubectl apply -f argocd/project.yaml -f argocd/appset.yaml`
5. UI: `kubectl port-forward svc/argocd-server -n argocd 8090:443` →
   `https://localhost:8090` (admin / `kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d`).

### Production / air-gapped variant

Run Argo CD on the management side with firewall openings: Argo → cluster API (:6443) and
Argo → git server. Register the cluster with a scoped ServiceAccount (Integration CRUD in
`camel-k` only) via a declarative cluster Secret, change `destination.server` in
`argocd/appset.yaml` + `project.yaml` to that cluster, and point `repoURL` at the real git
server.

### Optional: Integration health check

By default Argo reports Integration CRs Healthy as soon as they apply. To gate health on
the operator actually running the route, add to `argocd-cm`:

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
