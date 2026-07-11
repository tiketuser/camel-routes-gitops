# Air-Gapped GitOps Control Plane Deployment

Bootstraps Argo CD (and, by default, an in-cluster Gitea) on an air-gapped k3s cluster
so it can deploy Camel K routes from this repo. **One script, one config file, no
internet required.**

This bundle only covers the GitOps control plane. It assumes the Camel K runtime
(operator, registry, Maven mirror, Kafka, Redis, rate-limit plugin/routes) is already
running — deploy that first from the **camel-k-project-avieli** repo's own
`airgap-bundle/` (see its `AIRGAP-DEPLOY.md`).

## 1. Bundle contents

```
airgap-bundle/
├── AIRGAP-DEPLOY.md            ← this guide
├── install.sh                  ← the one-click script
├── config.env.example          ← copy to config.env and edit before running
├── images/
│   ├── all-images-amd64.tar    ← Argo CD, Dex, Redis, Gitea images, linux/amd64
│   └── image-list-amd64.txt
├── cli/linux-amd64/            ← kubectl, helm (static binaries; optional if already installed)
├── manifests/
│   ├── argocd-install.yaml     ← Argo CD v3.4.5, vendored upstream install manifest
│   ├── root-app.yaml.tmpl      ← templated root Application (${REPO_URL}, ${REVISION})
│   └── appset.yaml.tmpl        ← templated ApplicationSet
└── repo-snapshot/               ← copy of this repo's charts/, routes/, argocd/project.yaml
                                    — pushed into Gitea as the initial commit
```

## 2. Transfer and configure

```bash
# Transfer camel-routes-gitops-airgap-bundle.zip to the server, then:
unzip camel-routes-gitops-airgap-bundle.zip && cd airgap-bundle
cp config.env.example config.env
$EDITOR config.env   # at minimum, set GITEA_ADMIN_PASSWORD
```

`GIT_BACKEND` in `config.env` picks the git source:

- **`gitea`** (default) — install.sh deploys an in-cluster Gitea, creates the admin
  user + repo, and pushes `repo-snapshot/` into it (with `root-app.yaml`/`appset.yaml`
  rendered to point back at that same Gitea repo). Fully offline.
- **`external`** — skips Gitea; Argo CD is pointed straight at `REPO_URL` in
  `config.env`, a git server you already run. You're responsible for that repo already
  containing matching content (its own `argocd/appset.yaml` must point at itself).

## 3. Run

```bash
./install.sh
```

Steps: import images → install CLIs → (gitea backend) deploy Gitea, create admin
user + repo, render and push the repo snapshot → install Argo CD → apply the root
Application. After that one apply, Argo CD self-manages everything under `argocd/`
(including itself) via the app-of-apps pattern, and the ApplicationSet turns each
`routes/*.yaml` file into a running `Integration`.

Re-running `install.sh` is safe: image import, CLI install, Gitea/Argo CD manifest
apply, and the repo-snapshot push (`git push -f`) are all idempotent.

## 4. Verify

```bash
kubectl get application camel-routes-root -n argocd
kubectl get applicationset camel-routes -n argocd
kubectl get application -n argocd -l app.kubernetes.io/part-of=camel-routes
kubectl get integration -n camel-k
```

Argo CD UI: `kubectl port-forward -n argocd svc/argocd-server 8443:443`, then
`https://localhost:8443` (`admin` / `kubectl -n argocd get secret
argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d`).

## 5. Add or change a route afterward

`GIT_BACKEND=gitea`: clone from the NodePort (`http://gitea-http... ` in-cluster, or
`http://<NODE_IP>:<GITEA_NODEPORT>/...` from outside), edit `routes/*.yaml`, push — the
same self-heal/prune flow described in the main README applies, just against Gitea
instead of GitHub.

## 6. Regenerating this bundle

On any online machine, from the repo root: `airgap-bundle/regenerate-bundle.sh`. It
pulls + saves the images in `images/image-list-amd64.txt`, downloads `kubectl`/`helm`,
refreshes `repo-snapshot/` from the current working tree, and zips the result.
