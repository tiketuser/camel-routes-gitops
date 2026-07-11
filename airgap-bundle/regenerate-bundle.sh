#!/usr/bin/env bash
# Run on an online machine with docker + internet, from the repo root, to (re)build
# camel-routes-gitops-airgap-bundle.zip. Not part of the air-gapped runtime path itself.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUNDLE_DIR="$REPO_ROOT/airgap-bundle"
cd "$REPO_ROOT"

echo "==> Pulling + saving container images (linux/amd64)"
mkdir -p "$BUNDLE_DIR/images"
while read -r image; do
  [[ -z "$image" ]] && continue
  docker pull --platform linux/amd64 "$image"
done < "$BUNDLE_DIR/images/image-list-amd64.txt"
docker save --platform linux/amd64 -o "$BUNDLE_DIR/images/all-images-amd64.tar" \
  $(tr '\n' ' ' < "$BUNDLE_DIR/images/image-list-amd64.txt")

echo "==> Downloading kubectl + helm (linux/amd64)"
mkdir -p "$BUNDLE_DIR/cli/linux-amd64"
KUBECTL_VERSION="${KUBECTL_VERSION:-v1.31.5}"
HELM_VERSION="${HELM_VERSION:-v3.16.4}"
curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
  -o "$BUNDLE_DIR/cli/linux-amd64/kubectl"
curl -fsSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz" \
  | tar xz -C "$BUNDLE_DIR/cli/linux-amd64" --strip-components=1 linux-amd64/helm
chmod +x "$BUNDLE_DIR/cli/linux-amd64/kubectl" "$BUNDLE_DIR/cli/linux-amd64/helm"

echo "==> Refreshing repo-snapshot/ from the current working tree"
rm -rf "$BUNDLE_DIR/repo-snapshot"
mkdir -p "$BUNDLE_DIR/repo-snapshot/argocd"
cp -r "$REPO_ROOT/charts" "$BUNDLE_DIR/repo-snapshot/"
cp -r "$REPO_ROOT/routes" "$BUNDLE_DIR/repo-snapshot/"
cp "$REPO_ROOT/argocd/project.yaml" "$BUNDLE_DIR/repo-snapshot/argocd/"
cp "$REPO_ROOT/README.md" "$BUNDLE_DIR/repo-snapshot/"

echo "==> Re-vendoring argocd-install.yaml (edit ARGOCD_VERSION here to bump)"
ARGOCD_VERSION="${ARGOCD_VERSION:-v3.4.5}"
curl -fsSL "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml" \
  -o "$BUNDLE_DIR/manifests/argocd-install.yaml"

echo "==> Zipping bundle"
cd "$REPO_ROOT"
zip -r camel-routes-gitops-airgap-bundle.zip airgap-bundle \
  -x "*.DS_Store" -x "airgap-bundle/config.env"

echo "Done: $REPO_ROOT/camel-routes-gitops-airgap-bundle.zip"
