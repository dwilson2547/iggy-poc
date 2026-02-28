# Helm Chart — Apache Iggy Server

This folder contains a Helm chart for deploying the [Apache Iggy](https://iggy.apache.org) message streaming server on Kubernetes, along with ready-made values files for common environments.

---

## Directory Layout

```text
helm/
├── iggy/                          # The Helm chart
│   ├── Chart.yaml
│   ├── values.yaml                # Default values (ClusterIP, 10 Gi, moderate resources)
│   └── templates/
│       ├── _helpers.tpl
│       ├── NOTES.txt
│       ├── deployment.yaml
│       ├── ingress.yaml
│       ├── persistentvolumeclaim.yaml
│       ├── secret.yaml
│       ├── service.yaml
│       └── serviceaccount.yaml
└── examples/
    ├── values-dev.yaml            # Local / CI — minimal resources, small volume
    └── values-production.yaml    # Cloud production — LoadBalancer, large volume, anti-affinity
```

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Kubernetes | 1.23+ | Any distribution (EKS, GKE, AKS, kind, k3d, minikube…) |
| Helm | 3.10+ | `helm version` |
| kubectl | Matching cluster | `kubectl version` |

---

## Quick Start

### 1. Install with default values

```bash
helm install iggy ./helm/iggy \
  --namespace iggy \
  --create-namespace
```

### 2. Port-forward and verify

```bash
# HTTP REST API
kubectl port-forward svc/iggy 3000:3000 -n iggy

# In another terminal — get a token and list streams
TOKEN=$(curl -s -X POST http://localhost:3000/users/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"iggy","password":"iggy"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s -H "Authorization: Bearer $TOKEN" http://localhost:3000/streams
# → []
```

---

## Configuration

### Default values (`values.yaml`)

| Key | Default | Description |
|-----|---------|-------------|
| `image.repository` | `apache/iggy` | Docker image repository |
| `image.tag` | *(chart appVersion)* | Image tag; defaults to `0.6.0` |
| `replicaCount` | `1` | Number of pods (iggy is single-node) |
| `service.type` | `ClusterIP` | Kubernetes Service type |
| `service.httpPort` | `3000` | HTTP REST API port |
| `service.tcpPort` | `8090` | TCP binary protocol port |
| `iggy.rootUsername` | `iggy` | Root user username |
| `iggy.rootPassword` | `iggy` | Root user password |
| `persistence.enabled` | `true` | Enable PersistentVolumeClaim |
| `persistence.size` | `10Gi` | PVC size |
| `resources.requests.cpu` | `100m` | CPU request |
| `resources.requests.memory` | `128Mi` | Memory request |
| `resources.limits.cpu` | `500m` | CPU limit |
| `resources.limits.memory` | `512Mi` | Memory limit |

For a complete list of options, see [`iggy/values.yaml`](iggy/values.yaml).

---

## Example Configurations

### Development / local cluster

Minimal resources (50 m CPU / 64 Mi), small 1 Gi volume, `ClusterIP` service.

```bash
helm install iggy ./helm/iggy \
  -f ./helm/examples/values-dev.yaml \
  --namespace iggy \
  --create-namespace
```

### Production (cloud)

`LoadBalancer` service, 50 Gi SSD volume, generous resource limits, pod anti-affinity, and credentials stored in an existing Kubernetes Secret.

**Step 1 — Create the credentials Secret:**

```bash
kubectl create secret generic iggy-credentials \
  --from-literal=username=<YOUR_USERNAME> \
  --from-literal=password=<YOUR_STRONG_PASSWORD> \
  --namespace iggy
```

**Step 2 — Install the chart:**

```bash
helm install iggy ./helm/iggy \
  -f ./helm/examples/values-production.yaml \
  --namespace iggy \
  --create-namespace
```

> **Note:** Edit [`examples/values-production.yaml`](examples/values-production.yaml) before deploying — update `persistence.storageClass` to match your cluster's available StorageClass (`kubectl get storageclass`).

---

## Upgrading

```bash
helm upgrade iggy ./helm/iggy \
  -f ./helm/examples/values-production.yaml \
  --namespace iggy
```

To upgrade the iggy server version, pass `--set image.tag=<new-tag>`:

```bash
helm upgrade iggy ./helm/iggy --set image.tag=0.7.0 -n iggy
```

---

## Uninstalling

```bash
helm uninstall iggy -n iggy

# To also remove persistent data:
kubectl delete pvc iggy-data -n iggy
```

---

## Connecting the SDK clients

Once port-forwarding is active (or the LoadBalancer IP is assigned), point each SDK client at the correct address:

| Client | Protocol | Default address |
|--------|----------|-----------------|
| Python, Rust, TypeScript | TCP binary | `localhost:8090` |
| Java (HTTP) | HTTP REST | `http://localhost:3000` |

These match the defaults already configured in the `clients/` examples in this repository.

---

## Troubleshooting

**Pod is stuck in `Pending`**
```bash
kubectl describe pod -l app.kubernetes.io/name=iggy -n iggy
```
Common cause: no PersistentVolume available. Use `persistence.enabled=false` for a quick test, or ensure a default StorageClass is configured.

**Pod is stuck in `CrashLoopBackOff`**
```bash
kubectl logs -l app.kubernetes.io/name=iggy -n iggy --previous
```
Check that `securityContext.seccompUnconfined=true` (the default) — iggy uses `io_uring` which requires Seccomp to be set to `Unconfined`.

**Cannot connect on TCP port 8090**
`kubectl port-forward` only supports TCP, so port-forwarding the TCP binary port works fine:
```bash
kubectl port-forward svc/iggy 8090:8090 -n iggy
```
