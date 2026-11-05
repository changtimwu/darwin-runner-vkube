#!/usr/bin/env bash
# Run with an admin kubeconfig after `kubectl apply -f deploy/rbac.yaml`.
# Writes darwin-vk.kubeconfig (SA token) and cluster-ca.crt for --client-ca.
set -euo pipefail
OUT=${1:-darwin-vk.kubeconfig}
SERVER=$(kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.server}')
kubectl -n kube-system wait --for=jsonpath='{.data.token}' secret/darwin-vk-token --timeout=30s >/dev/null
TOKEN=$(kubectl -n kube-system get secret darwin-vk-token -o jsonpath='{.data.token}' | base64 -d)
kubectl -n kube-system get secret darwin-vk-token -o jsonpath='{.data.ca\.crt}' | base64 -d > cluster-ca.crt

KUBECONFIG=$OUT kubectl config set-cluster cluster --server="$SERVER" --certificate-authority=cluster-ca.crt --embed-certs=true >/dev/null
KUBECONFIG=$OUT kubectl config set-credentials darwin-vk --token="$TOKEN" >/dev/null
KUBECONFIG=$OUT kubectl config set-context default --cluster=cluster --user=darwin-vk >/dev/null
KUBECONFIG=$OUT kubectl config use-context default >/dev/null
chmod 600 "$OUT"
echo "wrote $OUT and cluster-ca.crt"
