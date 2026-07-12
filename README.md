# camel-routes-gitops

GitOps repo for Camel K bridge routes. **Deploying a route = pushing one small YAML file
to `routes/`.** Argo CD auto-discovers it (ApplicationSet git-files generator), renders the
`charts/camel-route` Helm chart with it, and applies the resulting `Integration` CR; the
in-cluster Camel K operator builds and runs it. Deleting the file prunes the route.

## Layout

| Path | What |
|---|---|
| `charts/camel-route/` | Helm chart: one templated `Integration` per route. The route type is inferred from which kind key (`kafka:` / `http:`) sits under `source:` and `sink:` — no `routeType` field. Embeds `files/RateLimit.java` (Redis token-bucket limiter) when `rateLimit.enabled`. |
| `routes/*.yaml` | One values file per route — this is the only thing you touch day-to-day. |
| `argocd/root-app.yaml` | **Root "father" Application (app-of-apps)** — applied once by hand; from then on it GitOps-manages everything in `argocd/`, including itself. |
| `argocd/project.yaml` | AppProject `camel-routes` — locked to this repo, the `camel-k` namespace, and the `Integration` kind. |
| `argocd/appset.yaml` | ApplicationSet — git files generator over `routes/*.yaml`; the generated `route-*` Applications are the "sons": automated sync, prune, self-heal, retry with backoff, cascade-delete finalizers. |

## Add a route

Create `routes/<name>.yaml` and push:

```yaml
name: orders-kk
source:
  kafka:                      # or http: {path: /ingest/orders}
    topic: orders-in
sink:
  kafka:                      # or http: {url: svc.ns.svc.cluster.local/path}
    topic: orders-out
rateLimit:
  enabled: true
  key: orders                 # routes sharing a key share one global limit
  rate: 10.0                  # sustained msg/s
  burst: 20
```

There is no `routeType` field — the type is inferred from which kind key (`kafka:` or
`http:`) each of `source:`/`sink:` nests, and any combination works (kafka→kafka,
https→kafka, https→https, kafka→https). The chart emits the derived type as the
`camel-route/type` label on the Integration.

Defaults (brokers, SASL, Redis host, TLS secret, prometheus traits) live in
`charts/camel-route/values.yaml`. A side's `kafka:` block is merged over those defaults,
so **source and sink are configured independently** — a route can bridge two different
clusters with different credentials:

```yaml
name: orders-kk
source:
  kafka:
    topic: orders-in                        # brokers/SASL inherited from values.yaml
sink:
  kafka:
    topic: orders-out
    brokers: kafka-eu.otherteam.svc:9092    # this side's own cluster
    securityProtocol: SASL_SSL
    saslMechanism: SCRAM-SHA-512
    credentialsSecret: orders-kafka-creds   # must exist in camel-k ns; when it coexists
    userKey: orders.user                    #   with another side's secret, its keys must
    passwordKey: orders.password            #   be uniquely named — point these at them
```

To override the defaults for both sides at once, set a top-level `kafka:` block in the
route file (same shape as in `values.yaml`).
`rateLimit.mode` defaults by source: kafka → `block` (backpressure), http → `reject` (429).

> **IBM MQ support removed for now**: the `mq:` source/sink kind (formerly the `mq-mq` /
> `https-mq` / `mq-https` / `mq-drain` routeTypes), the `mqConnectionFactory` bean, and
> the MQ route files have been pulled out.
> The MQ Advanced for Developers image is amd64/s390x/ppc64le-only and its glibc build
> requires x86-64-v3 CPU features that QEMU emulation on Apple Silicon doesn't provide, so
> it can't run on this local cluster. Re-add it if running on an amd64 host or once a
> working emulation path is found.

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

`airgap-bundle/` packages this whole control-plane bootstrap (Argo CD + in-cluster Gitea,
images, CLIs, and this repo's own content as an initial commit) as a one-command
`install.sh` for an air-gapped k3s host — see `airgap-bundle/AIRGAP-DEPLOY.md`. It assumes
the Camel K runtime itself is already deployed via the main repo's own air-gap bundle.

**Download ready-made:** the complete transfer bundle (`camel-routes-gitops-airgap-bundle.zip`,
~360 MB — Argo CD/Dex/Redis/Gitea images, `kubectl`/`helm`, vendored Argo CD manifests, and
this repo's content) is attached to the
[v0.2.0 release](https://github.com/TheRozom/camel-routes-gitops/releases/tag/v0.2.0):

```bash
gh release download v0.2.0 --repo TheRozom/camel-routes-gitops
```

Or regenerate it yourself on any online machine: `airgap-bundle/regenerate-bundle.sh`.

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
