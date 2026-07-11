#!/usr/bin/env bash
# One-click bootstrap of the GitOps control plane (Argo CD + optionally in-cluster
# Gitea) on an air-gapped k3s cluster, from this bundle.
#
# Prerequisites (NOT done by this script — see the main repo's airgap-bundle first):
# Camel K operator, registry, Maven mirror, Kafka, Redis, the rate-limit plugin/routes.
# This script only bootstraps the GitOps control plane that deploys routes onto that
# runtime.
#
# Usage: ./install.sh [path-to-config.env]   (defaults to ./config.env)
set -euo pipefail

BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${1:-$BUNDLE_DIR/config.env}"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Config file not found: $CONFIG_FILE" >&2
  echo "Copy config.env.example to config.env and edit it first." >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$CONFIG_FILE"

: "${GIT_BACKEND:?set GIT_BACKEND=gitea|external in config.env}"
: "${REVISION:=main}"

case "$GIT_BACKEND" in
  gitea)
    : "${GITEA_ADMIN_USER:=gitops}"
    : "${GITEA_ADMIN_PASSWORD:?set GITEA_ADMIN_PASSWORD in config.env}"
    : "${GITEA_REPO_OWNER:=$GITEA_ADMIN_USER}"
    : "${GITEA_REPO_NAME:=camel-routes-gitops}"
    : "${GITEA_NODEPORT:=30501}"
    REPO_URL="http://gitea-http.git.svc.cluster.local:3000/${GITEA_REPO_OWNER}/${GITEA_REPO_NAME}.git"
    ;;
  external)
    : "${REPO_URL:?set REPO_URL in config.env when GIT_BACKEND=external}"
    ;;
  *)
    echo "Unknown GIT_BACKEND=$GIT_BACKEND (expected gitea|external)" >&2
    exit 1
    ;;
esac

echo "==> [1/6] Importing container images"
if [[ -f "$BUNDLE_DIR/images/all-images-amd64.tar" ]]; then
  sudo k3s ctr images import "$BUNDLE_DIR/images/all-images-amd64.tar"
else
  echo "    no images tar found at images/all-images-amd64.tar, skipping (assuming already imported)"
fi

echo "==> [2/6] Installing CLIs"
for bin in kubectl helm; do
  if ! command -v "$bin" >/dev/null 2>&1 && [[ -f "$BUNDLE_DIR/cli/linux-amd64/$bin" ]]; then
    sudo install -m 0755 "$BUNDLE_DIR/cli/linux-amd64/$bin" /usr/local/bin/
  fi
done

if [[ "$GIT_BACKEND" == "gitea" ]]; then
  echo "==> [3/6] Deploying in-cluster Gitea"
  kubectl apply -f "$BUNDLE_DIR/../bootstrap/gitea.yaml"
  kubectl rollout status deployment/gitea -n git --timeout=180s

  echo "    creating Gitea admin user (ignored if it already exists)"
  GITEA_POD=$(kubectl get pod -n git -l app=gitea -o jsonpath='{.items[0].metadata.name}')
  kubectl exec -n git "$GITEA_POD" -- gitea admin user create \
    --username "$GITEA_ADMIN_USER" --password "$GITEA_ADMIN_PASSWORD" \
    --email "${GITEA_ADMIN_USER}@local" --admin --must-change-password=false \
    || echo "    (user create failed/skipped — likely already exists)"

  echo "    creating Gitea repo ${GITEA_REPO_OWNER}/${GITEA_REPO_NAME} (ignored if it already exists)"
  curl -sf -u "${GITEA_ADMIN_USER}:${GITEA_ADMIN_PASSWORD}" \
    -X POST "http://localhost:${GITEA_NODEPORT}/api/v1/user/repos" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"${GITEA_REPO_NAME}\",\"private\":false}" \
    || echo "    (repo create failed/skipped — likely already exists)"

  echo "==> [4/6] Rendering root-app/appset for this git backend and pushing the repo snapshot into Gitea"
  TMP_GIT=$(mktemp -d)
  trap 'rm -rf "$TMP_GIT"' EXIT
  cp -r "$BUNDLE_DIR/repo-snapshot/." "$TMP_GIT/"
  mkdir -p "$TMP_GIT/argocd"
  REPO_URL="$REPO_URL" REVISION="$REVISION" envsubst '${REPO_URL} ${REVISION}' \
    < "$BUNDLE_DIR/manifests/root-app.yaml.tmpl" > "$TMP_GIT/argocd/root-app.yaml"
  REPO_URL="$REPO_URL" REVISION="$REVISION" envsubst '${REPO_URL} ${REVISION}' \
    < "$BUNDLE_DIR/manifests/appset.yaml.tmpl" > "$TMP_GIT/argocd/appset.yaml"

  git -C "$TMP_GIT" init -q -b "$REVISION"
  git -C "$TMP_GIT" add -A
  git -C "$TMP_GIT" -c user.email="${GITEA_ADMIN_USER}@local" -c user.name="$GITEA_ADMIN_USER" \
    commit -q -m "air-gap bootstrap snapshot"
  git -C "$TMP_GIT" push -f \
    "http://${GITEA_ADMIN_USER}:${GITEA_ADMIN_PASSWORD}@localhost:${GITEA_NODEPORT}/${GITEA_REPO_OWNER}/${GITEA_REPO_NAME}.git" \
    "$REVISION"
  RENDERED_ROOT_APP="$TMP_GIT/argocd/root-app.yaml"
else
  echo "==> [3-4/6] GIT_BACKEND=external — skipping Gitea, using $REPO_URL directly"
  echo "    (make sure that repo's argocd/appset.yaml already points at the same REPO_URL/REVISION)"
  TMP_GIT=$(mktemp -d)
  trap 'rm -rf "$TMP_GIT"' EXIT
  REPO_URL="$REPO_URL" REVISION="$REVISION" envsubst '${REPO_URL} ${REVISION}' \
    < "$BUNDLE_DIR/manifests/root-app.yaml.tmpl" > "$TMP_GIT/root-app.yaml"
  RENDERED_ROOT_APP="$TMP_GIT/root-app.yaml"
fi

echo "==> [5/6] Installing Argo CD"
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -n argocd --server-side -f "$BUNDLE_DIR/manifests/argocd-install.yaml"
kubectl rollout status deployment/argocd-server -n argocd --timeout=300s

echo "==> [6/6] Applying root Application (one-time manual apply — self-manages from here)"
kubectl apply -f "$RENDERED_ROOT_APP"

echo
echo "Done. Argo CD UI: kubectl port-forward -n argocd svc/argocd-server 8443:443"
echo "Admin password: kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d"
