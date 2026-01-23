# Part 2: Environment Setup — From Local Development to Production

**Mastering Spark on Kubernetes Series**

*Set up a complete Spark-on-Kubernetes environment from scratch. We'll build both a local KinD cluster for development and a production-ready GKE cluster with autoscaling, spot instances, and proper networking.*

---

## What You'll Accomplish

By the end of this guide, you'll have:

- ✅ A local 4-node Kubernetes cluster using KinD (free, runs on your laptop)
- ✅ A production GKE cluster with separate node pools for drivers and executors
- ✅ Spot/Preemptible instances configured for 70-90% cost savings
- ✅ Proper networking and storage for Spark workloads
- ✅ Cluster autoscaling that scales to zero when idle
- ✅ All prerequisites installed and verified

**Reading Time:** 45 minutes  
**Hands-On Time:** 60-90 minutes  
**Prerequisites:** A computer with Docker, basic terminal knowledge

---

## Table of Contents

1. [Prerequisites: Tools You'll Need](#prerequisites)
2. [Understanding Cluster Requirements for Spark](#cluster-requirements)
3. [Local Development: KinD Setup](#kind-setup)
4. [Production: GKE Cluster Setup](#gke-setup)
5. [Storage Configuration](#storage-configuration)
6. [Networking Fundamentals](#networking)
7. [Installing Essential Tools](#essential-tools)
8. [Verification & Testing](#verification)
9. [Cost Optimization Strategies](#cost-optimization)
10. [Troubleshooting Common Issues](#troubleshooting)

---

## Prerequisites: Tools You'll Need {#prerequisites}

Before we begin, let's install all the tools you'll need. I'll provide commands for both macOS and Linux.

### 1. Docker Desktop

Docker is required for both KinD (which runs K8s in Docker containers) and building Spark images.

**macOS:**
```bash
# Install via Homebrew
brew install --cask docker

# Start Docker Desktop
open /Applications/Docker.app

# Verify installation
docker --version
# Expected: Docker version 24.x.x or higher
```

**Linux (Ubuntu/Debian):**
```bash
# Install Docker
curl -fsSL https://get.docker.com | sh

# Add your user to docker group (avoids needing sudo)
sudo usermod -aG docker $USER

# Apply group changes (or log out and back in)
newgrp docker

# Start Docker
sudo systemctl start docker
sudo systemctl enable docker

# Verify
docker --version
```

**Resource Allocation (Important!):**

For Spark workloads, allocate sufficient resources to Docker:

1. Open Docker Desktop → Settings → Resources
2. Set:
   - **CPUs:** At least 4 (8 recommended)
   - **Memory:** At least 8 GB (16 GB recommended)
   - **Disk:** At least 50 GB

### 2. kubectl (Kubernetes CLI)

**macOS:**
```bash
brew install kubectl

# Verify
kubectl version --client
```

**Linux:**
```bash
# Download latest stable
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"

# Install
chmod +x kubectl
sudo mv kubectl /usr/local/bin/

# Verify
kubectl version --client
```

### 3. KinD (Kubernetes in Docker)

**macOS:**
```bash
brew install kind

# Verify
kind version
# Expected: kind v0.20.0 or higher
```

**Linux:**
```bash
# Download
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-linux-amd64

# Install
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind

# Verify
kind version
```

### 4. Helm (Kubernetes Package Manager)

We'll use Helm to install the Spark Operator and monitoring tools.

**macOS:**
```bash
brew install helm

# Verify
helm version
```

**Linux:**
```bash
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# Verify
helm version
```

### 5. Google Cloud SDK (For GKE)

Skip this if you're only using KinD for now.

**macOS:**
```bash
brew install --cask google-cloud-sdk

# Initialize
gcloud init

# Login
gcloud auth login

# Set project
gcloud config set project YOUR_PROJECT_ID
```

**Linux:**
```bash
# Download and install
curl https://sdk.cloud.google.com | bash

# Restart shell
exec -l $SHELL

# Initialize
gcloud init
```

### 6. jq (JSON Processor)

Useful for parsing kubectl output.

```bash
# macOS
brew install jq

# Linux
sudo apt-get install jq
```

### Verification Script

Run this script to verify all tools are installed:

```bash
#!/bin/bash
# verify-prerequisites.sh

echo "=== Checking Prerequisites ==="

check_command() {
    if command -v $1 &> /dev/null; then
        echo "✅ $1: $($1 --version 2>&1 | head -1)"
    else
        echo "❌ $1: NOT INSTALLED"
    fi
}

check_command docker
check_command kubectl
check_command kind
check_command helm
check_command gcloud
check_command jq

echo ""
echo "=== Docker Resources ==="
docker info --format '{{.MemTotal}}' | awk '{print "Memory: " $1/1024/1024/1024 " GB"}'
docker info --format '{{.NCPU}}' | awk '{print "CPUs: " $1}'

echo ""
echo "=== Ready to proceed! ==="
```

Save and run:
```bash
chmod +x verify-prerequisites.sh
./verify-prerequisites.sh
```

---

## Understanding Cluster Requirements for Spark {#cluster-requirements}

Before creating clusters, let's understand what Spark needs from Kubernetes.

### Spark Pod Types

| Pod Type | Role | Resource Profile | Reliability Requirement |
|----------|------|------------------|------------------------|
| **Driver** | Coordinates job, holds SparkContext | Medium CPU, High Memory | High (job fails if driver dies) |
| **Executor** | Runs tasks, processes data | High CPU, High Memory | Medium (can be replaced) |

### Node Pool Strategy

For production, we separate workloads into different node pools:

```
┌─────────────────────────────────────────────────────────────┐
│                     GKE Cluster                              │
├─────────────────┬─────────────────┬─────────────────────────┤
│  System Pool    │  Driver Pool    │    Executor Pool        │
│  (1-3 nodes)    │  (1-5 nodes)    │    (0-20 nodes)         │
├─────────────────┼─────────────────┼─────────────────────────┤
│ • kube-system   │ • Spark Driver  │ • Spark Executors       │
│ • monitoring    │ • Always-on     │ • Scale to zero         │
│ • ingress       │ • On-Demand     │ • Spot/Preemptible      │
│ • logging       │                 │ • Auto-scaling          │
├─────────────────┼─────────────────┼─────────────────────────┤
│ e2-medium       │ e2-standard-4   │ e2-standard-8           │
│ 2 vCPU, 4GB     │ 4 vCPU, 16GB    │ 8 vCPU, 32GB            │
└─────────────────┴─────────────────┴─────────────────────────┘
```

### Resource Calculations

**How many executors can fit on a node?**

```
Example: e2-standard-8 (8 vCPU, 32GB RAM)

Reserved for K8s system:     ~1 vCPU, ~1GB RAM
Available for Spark:         7 vCPU, 31GB RAM

Executor config:
  spark.executor.cores = 2
  spark.executor.memory = 8g
  spark.executor.memoryOverhead = 2g (25% of memory)
  Total per executor: 2 vCPU, 10GB RAM

Executors per node: floor(7/2) = 3 (limited by CPU)
                    floor(31/10) = 3 (limited by memory)
                    
Result: 3 executors per e2-standard-8 node
```

---

## Local Development: KinD Setup {#kind-setup}

KinD (Kubernetes in Docker) creates a fully functional Kubernetes cluster using Docker containers as nodes. It's perfect for:

- 💻 Local development and testing
- 🧪 CI/CD pipelines
- 📚 Learning without cloud costs

### Cluster Configuration

Create a file called `kind-spark-cluster.yaml`:

```yaml
# kind-spark-cluster.yaml
# Multi-node KinD cluster optimized for Spark workloads

kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: spark-cluster

# Networking configuration
networking:
  # Use Calico for Network Policies (optional but recommended)
  disableDefaultCNI: false
  # Pod subnet
  podSubnet: "10.244.0.0/16"
  # Service subnet  
  serviceSubnet: "10.96.0.0/12"

nodes:
  # ===== Control Plane Node =====
  - role: control-plane
    kubeadmConfigPatches:
      - |
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "node-type=control-plane"
    # Port mappings for accessing services
    extraPortMappings:
      # Spark UI (NodePort 30040 -> localhost:4040)
      - containerPort: 30040
        hostPort: 4040
        protocol: TCP
      # Spark History Server (NodePort 30180 -> localhost:18080)
      - containerPort: 30180
        hostPort: 18080
        protocol: TCP
      # Kubernetes Dashboard (optional)
      - containerPort: 30443
        hostPort: 8443
        protocol: TCP

  # ===== Worker Node 1 =====
  - role: worker
    kubeadmConfigPatches:
      - |
        kind: JoinConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "node-type=worker,spark-role=any"
    # Mount local directory for shuffle data
    extraMounts:
      - hostPath: /tmp/spark-local-dir-1
        containerPath: /data/spark
        readOnly: false
        selinuxRelabel: false
        propagation: None

  # ===== Worker Node 2 =====
  - role: worker
    kubeadmConfigPatches:
      - |
        kind: JoinConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "node-type=worker,spark-role=any"
    extraMounts:
      - hostPath: /tmp/spark-local-dir-2
        containerPath: /data/spark
        readOnly: false
        selinuxRelabel: false
        propagation: None

  # ===== Worker Node 3 =====
  - role: worker
    kubeadmConfigPatches:
      - |
        kind: JoinConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "node-type=worker,spark-role=any"
    extraMounts:
      - hostPath: /tmp/spark-local-dir-3
        containerPath: /data/spark
        readOnly: false
        selinuxRelabel: false
        propagation: None

  # ===== Worker Node 4 (Optional - for larger workloads) =====
  - role: worker
    kubeadmConfigPatches:
      - |
        kind: JoinConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "node-type=worker,spark-role=any"
    extraMounts:
      - hostPath: /tmp/spark-local-dir-4
        containerPath: /data/spark
        readOnly: false
        selinuxRelabel: false
        propagation: None
```

### Create the Cluster

```bash
# Create local directories for shuffle data
mkdir -p /tmp/spark-local-dir-{1,2,3,4}

# Create the cluster (takes 2-4 minutes)
kind create cluster --config kind-spark-cluster.yaml

# Expected output:
# Creating cluster "spark-cluster" ...
#  ✓ Ensuring node image (kindest/node:v1.27.3) 🖼
#  ✓ Preparing nodes 📦 📦 📦 📦 📦
#  ✓ Writing configuration 📜
#  ✓ Starting control-plane 🕹️
#  ✓ Installing CNI 🔌
#  ✓ Installing StorageClass 💾
#  ✓ Joining worker nodes 🚜
# Set kubectl context to "kind-spark-cluster"
```

### Verify the Cluster

```bash
# Check cluster info
kubectl cluster-info --context kind-spark-cluster

# List nodes
kubectl get nodes -o wide

# Expected output:
# NAME                          STATUS   ROLES           AGE   VERSION   INTERNAL-IP   ...
# spark-cluster-control-plane   Ready    control-plane   2m    v1.27.3   172.18.0.5    ...
# spark-cluster-worker          Ready    <none>          90s   v1.27.3   172.18.0.2    ...
# spark-cluster-worker2         Ready    <none>          90s   v1.27.3   172.18.0.3    ...
# spark-cluster-worker3         Ready    <none>          90s   v1.27.3   172.18.0.4    ...
# spark-cluster-worker4         Ready    <none>          90s   v1.27.3   172.18.0.6    ...

# Check node labels
kubectl get nodes --show-labels | grep spark-role
```

### Create Spark Namespace

```bash
# Create namespace for Spark jobs
kubectl create namespace spark-jobs

# Set as default namespace
kubectl config set-context --current --namespace=spark-jobs

# Verify
kubectl config view --minify | grep namespace
```

### Resource Limits for KinD

KinD nodes share your laptop's resources. Let's set resource quotas to prevent runaway jobs:

```yaml
# kind-resource-quota.yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: spark-quota
  namespace: spark-jobs
spec:
  hard:
    requests.cpu: "8"        # Total CPU requests
    requests.memory: "16Gi"  # Total memory requests
    limits.cpu: "16"         # Total CPU limits
    limits.memory: "32Gi"    # Total memory limits
    pods: "20"               # Maximum pods
---
apiVersion: v1
kind: LimitRange
metadata:
  name: spark-limits
  namespace: spark-jobs
spec:
  limits:
    - type: Container
      default:
        cpu: "2"
        memory: "4Gi"
      defaultRequest:
        cpu: "500m"
        memory: "1Gi"
      max:
        cpu: "4"
        memory: "8Gi"
```

Apply:
```bash
kubectl apply -f kind-resource-quota.yaml
```

### Useful KinD Commands

```bash
# List clusters
kind get clusters

# Delete cluster
kind delete cluster --name spark-cluster

# Get kubeconfig
kind get kubeconfig --name spark-cluster

# Load a Docker image into KinD (important for local images!)
kind load docker-image my-spark-image:latest --name spark-cluster

# Export logs for debugging
kind export logs ./kind-logs --name spark-cluster
```

---

## Production: GKE Cluster Setup {#gke-setup}

For production workloads, GKE provides:

- 🔄 Automatic Kubernetes upgrades
- 📈 Cluster autoscaling (including scale to zero)
- 🔐 Workload Identity for secure cloud access
- 💰 Spot VMs for 60-90% cost savings
- 📊 Integrated monitoring with Cloud Monitoring

### Enable Required APIs

```bash
# Set your project
export PROJECT_ID="your-gcp-project-id"
gcloud config set project $PROJECT_ID

# Enable required APIs
gcloud services enable container.googleapis.com
gcloud services enable compute.googleapis.com
gcloud services enable iam.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
```

### Create the GKE Cluster

```bash
# Set variables
export CLUSTER_NAME="spark-production"
export REGION="us-central1"
export ZONE="us-central1-a"

# Create cluster with Workload Identity enabled
gcloud container clusters create $CLUSTER_NAME \
  --region $REGION \
  --release-channel regular \
  --num-nodes 1 \
  --machine-type e2-medium \
  --disk-size 50GB \
  --disk-type pd-standard \
  --enable-autoscaling \
  --min-nodes 1 \
  --max-nodes 3 \
  --workload-pool=${PROJECT_ID}.svc.id.goog \
  --enable-ip-alias \
  --enable-network-policy \
  --enable-autorepair \
  --enable-autoupgrade \
  --node-labels=node-type=system \
  --addons=HttpLoadBalancing,HorizontalPodAutoscaling \
  --logging=SYSTEM,WORKLOAD \
  --monitoring=SYSTEM

# This takes 5-10 minutes
```

### Add Driver Node Pool (On-Demand)

Drivers need reliable nodes that won't be preempted:

```bash
gcloud container node-pools create driver-pool \
  --cluster $CLUSTER_NAME \
  --region $REGION \
  --machine-type e2-standard-4 \
  --disk-size 100GB \
  --disk-type pd-ssd \
  --num-nodes 0 \
  --enable-autoscaling \
  --min-nodes 0 \
  --max-nodes 5 \
  --node-labels=node-type=driver,spark-role=driver \
  --node-taints=spark-role=driver:NoSchedule \
  --metadata=disable-legacy-endpoints=true
```

### Add Executor Node Pool (Spot Instances)

Executors can use Spot VMs for massive cost savings:

```bash
gcloud container node-pools create executor-pool \
  --cluster $CLUSTER_NAME \
  --region $REGION \
  --machine-type e2-standard-8 \
  --disk-size 200GB \
  --disk-type pd-ssd \
  --local-ssd-count 1 \
  --num-nodes 0 \
  --enable-autoscaling \
  --min-nodes 0 \
  --max-nodes 20 \
  --spot \
  --node-labels=node-type=executor,spark-role=executor \
  --node-taints=spark-role=executor:NoSchedule \
  --metadata=disable-legacy-endpoints=true
```

### Get Cluster Credentials

```bash
gcloud container clusters get-credentials $CLUSTER_NAME --region $REGION

# Verify connection
kubectl get nodes
```

### Create Namespaces

```bash
# Spark jobs namespace
kubectl create namespace spark-jobs

# Monitoring namespace
kubectl create namespace monitoring

# Set default
kubectl config set-context --current --namespace=spark-jobs
```

### Cluster Details Summary

After creation, your cluster looks like:

```
┌─────────────────────────────────────────────────────────────────┐
│                 GKE Cluster: spark-production                    │
├───────────────────────────────────────────────────────────────────┤
│ Region: us-central1                                               │
│ Workload Identity: Enabled                                        │
│ Network Policy: Enabled                                           │
├─────────────────┬─────────────────┬───────────────────────────────┤
│  default-pool   │  driver-pool    │     executor-pool             │
│  (System)       │  (Drivers)      │     (Executors)               │
├─────────────────┼─────────────────┼───────────────────────────────┤
│ Machine:        │ Machine:        │ Machine:                      │
│ e2-medium       │ e2-standard-4   │ e2-standard-8                 │
│ 2 vCPU, 4GB     │ 4 vCPU, 16GB    │ 8 vCPU, 32GB + Local SSD     │
├─────────────────┼─────────────────┼───────────────────────────────┤
│ Scaling:        │ Scaling:        │ Scaling:                      │
│ 1-3 nodes       │ 0-5 nodes       │ 0-20 nodes                    │
├─────────────────┼─────────────────┼───────────────────────────────┤
│ VM Type:        │ VM Type:        │ VM Type:                      │
│ On-Demand       │ On-Demand       │ Spot (70-90% cheaper)         │
├─────────────────┼─────────────────┼───────────────────────────────┤
│ Taints:         │ Taints:         │ Taints:                       │
│ None            │ spark-role=     │ spark-role=                   │
│                 │ driver:NoSched  │ executor:NoSched              │
└─────────────────┴─────────────────┴───────────────────────────────┘
```

### Verify Node Pools

```bash
# List node pools
gcloud container node-pools list --cluster $CLUSTER_NAME --region $REGION

# Check nodes and labels
kubectl get nodes --show-labels | grep -E "node-type|spark-role"

# Check taints
kubectl get nodes -o custom-columns=NAME:.metadata.name,TAINTS:.spec.taints
```

---

## Storage Configuration {#storage-configuration}

Spark uses local storage for shuffle data. Let's configure storage classes.

### KinD: Using Host Paths

KinD already mounts `/tmp/spark-local-dir-*` to `/data/spark` in each worker. No additional setup needed.

### GKE: Local SSD Storage Class

For executor nodes with local SSDs:

```yaml
# gke-local-ssd-storage.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: local-ssd
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer
---
# Note: Local SSDs on GKE are automatically attached at /mnt/disks/ssd0
# You'll reference this path in your Spark configurations
```

### GKE: Standard Persistent Disk (For History Server)

```yaml
# gke-pd-storage.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: spark-pd-ssd
provisioner: pd.csi.storage.gke.io
parameters:
  type: pd-ssd
  replication-type: regional-pd
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
```

Apply:
```bash
kubectl apply -f gke-pd-storage.yaml
```

---

## Networking Fundamentals {#networking}

### How Spark Pods Communicate

```
┌─────────────────────────────────────────────────────────────┐
│                        Spark Job                             │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│   ┌─────────────┐                                             │
│   │   Driver    │◄─── Port 7078 (Back-refs from executors)    │
│   │   Pod       │◄─── Port 7079 (Block Manager)               │
│   │             │◄─── Port 4040 (Spark UI)                    │
│   └──────┬──────┘                                             │
│          │                                                    │
│          │ Task assignment / Results                          │
│          ▼                                                    │
│   ┌──────────────────────────────────────────────────────┐   │
│   │ Headless Service (spark-driver-svc)                   │   │
│   │ • Allows executors to find driver by DNS              │   │
│   └──────────────────────────────────────────────────────┘   │
│          │                                                    │
│          ▼                                                    │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│   │  Executor   │    │  Executor   │    │  Executor   │     │
│   │  Pod 1      │    │  Pod 2      │    │  Pod 3      │     │
│   │             │◄──►│             │◄──►│             │     │
│   └─────────────┘    └─────────────┘    └─────────────┘     │
│         ▲                  ▲                  ▲              │
│         └──────────────────┴──────────────────┘              │
│              Shuffle data exchange (direct pod-to-pod)        │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### Ports Used by Spark

| Port | Purpose | Configurable Via |
|------|---------|-----------------|
| 7077 | Spark Master (Standalone only) | N/A for K8s |
| 7078 | Driver RPC endpoint | `spark.driver.port` |
| 7079 | Driver Block Manager | `spark.driver.blockManager.port` |
| 4040 | Spark UI | `spark.ui.port` |
| Random | Executor ports | `spark.blockManager.port` |

### DNS Resolution

Spark on K8s uses Kubernetes DNS for service discovery:

- Driver creates a headless service
- Executors find driver via: `<driver-pod-name>.<namespace>.svc.cluster.local`

---

## Installing Essential Tools {#essential-tools}

### 1. Metrics Server (Required for HPA)

```bash
# KinD
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

# Patch for KinD (disable TLS verification)
kubectl patch -n kube-system deployment metrics-server \
  --type='json' \
  -p='[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kubelet-insecure-tls"}]'

# GKE: Metrics server is pre-installed
```

### 2. Spark Operator

```bash
# Add Helm repo
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

# Install (KinD Optimized)
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set webhook.enable=false \
  --set sparkJobNamespace=spark-jobs

# Verify
kubectl get pods -n spark-operator
```

> **Note:** We disable the webhook for KinD to avoid certificate management issues. We also explicitly set `sparkJobNamespace=spark-jobs` so the operator watches our namespace.

### 3. (Optional) Kubernetes Dashboard

For visual cluster management:

```bash
# Install dashboard
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml

# Create admin user
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: admin-user
  namespace: kubernetes-dashboard
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: admin-user
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
- kind: ServiceAccount
  name: admin-user
  namespace: kubernetes-dashboard
EOF

# Get token
kubectl -n kubernetes-dashboard create token admin-user

# Port forward
kubectl port-forward -n kubernetes-dashboard svc/kubernetes-dashboard 8443:443

# Access: https://localhost:8443
```

---

## Verification & Testing {#verification}

Let's verify everything works with a simple Spark job.

### Quick Verification Script

```bash
#!/bin/bash
# verify-cluster.sh

echo "=== Cluster Verification ==="
echo ""

echo "1. Nodes:"
kubectl get nodes -o wide
echo ""

echo "2. Node Labels:"
kubectl get nodes -o custom-columns=NAME:.metadata.name,LABELS:.metadata.labels
echo ""

echo "3. Namespaces:"
kubectl get namespaces
echo ""

echo "4. Spark Operator:"
kubectl get pods -n spark-operator
echo ""

echo "5. Storage Classes:"
kubectl get storageclass
echo ""

echo "6. Resource Quota (spark-jobs):"
kubectl get resourcequota -n spark-jobs
echo ""

echo "=== Verification Complete ==="
```

### Test with a Simple Pod

```bash
# Test pod to verify networking and resources
kubectl run test-spark --rm -it --restart=Never \
  --image=apache/spark:3.5.0 \
  --command -- /bin/bash -c "echo 'Spark $(spark-submit --version 2>&1 | head -1)'"
```

### Test SparkPi Job

```yaml
# test-sparkpi.yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-pi-test
  namespace: spark-jobs
spec:
  type: Scala
  mode: cluster
  image: "apache/spark:3.5.0"
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar"
  arguments:
    - "1000"
  sparkVersion: "3.5.0"
  restartPolicy:
    type: Never
  driver:
    cores: 1
    memory: "512m"
    serviceAccount: spark
  executor:
    cores: 1
    instances: 2
    memory: "512m"
```

First, create the ServiceAccount (we'll cover RBAC in detail in Part 4):

```bash
# Quick RBAC setup for testing
kubectl create serviceaccount spark -n spark-jobs

kubectl create role spark-role -n spark-jobs \
  --verb=get,list,watch,create,delete,patch \
  --resource=pods,services,configmaps

kubectl create rolebinding spark-rolebinding -n spark-jobs \
  --role=spark-role \
  --serviceaccount=spark-jobs:spark
```

Run the test:

```bash
kubectl apply -f test-sparkpi.yaml

# Watch the job
kubectl get sparkapplication -n spark-jobs -w

# View logs
kubectl logs spark-pi-test-driver -n spark-jobs -f

# After completion, check result
kubectl logs spark-pi-test-driver -n spark-jobs | grep "Pi is roughly"

# Cleanup
kubectl delete sparkapplication spark-pi-test -n spark-jobs
```

---

## Cost Optimization Strategies {#cost-optimization}

### Spot Instance Savings

| VM Type | On-Demand ($/hr) | Spot ($/hr) | Savings |
|---------|-----------------|-------------|---------|
| e2-standard-4 | $0.134 | $0.040 | 70% |
| e2-standard-8 | $0.268 | $0.080 | 70% |
| n2-standard-8 | $0.389 | $0.117 | 70% |
| c2-standard-8 | $0.418 | $0.125 | 70% |

### Scale to Zero

Our executor pool is configured to scale to 0 when idle:

```bash
# Verify autoscaling config
gcloud container node-pools describe executor-pool \
  --cluster $CLUSTER_NAME \
  --region $REGION \
  --format="yaml(autoscaling)"
```

### Cluster Autoscaler Optimization

```bash
# Update autoscaler profile for faster scale-up
gcloud container clusters update $CLUSTER_NAME \
  --region $REGION \
  --autoscaling-profile optimize-utilization
```

### Cost Monitoring

```bash
# View GKE costs in console
echo "https://console.cloud.google.com/kubernetes/clusters/details/${REGION}/${CLUSTER_NAME}/details?project=${PROJECT_ID}"
```

---

## Troubleshooting Common Issues {#troubleshooting}

### Issue 1: KinD Cluster Won't Start

**Symptom:** `ERROR: failed to create cluster`

**Solutions:**
```bash
# Check Docker is running
docker ps

# Ensure enough resources (8GB+ RAM)
docker info | grep "Total Memory"

# Delete existing cluster and retry
kind delete cluster --name spark-cluster
kind create cluster --config kind-spark-cluster.yaml
```

### Issue 2: Pods Stuck in Pending

**Symptom:** `kubectl get pods` shows Pending status

**Solutions:**
```bash
# Check events
kubectl describe pod <pod-name> -n spark-jobs

# Common causes:
# 1. Insufficient resources - check requests/limits
# 2. Node selector not matching - check labels
# 3. Taints not tolerated - check tolerations

# Check available resources
kubectl describe nodes | grep -A 10 "Allocated resources"
```

### Issue 3: ImagePullBackOff

**Symptom:** Pod can't pull Docker image

**Solutions:**
```bash
# For KinD - load image locally
kind load docker-image my-image:tag --name spark-cluster

# For GKE - check image path
# Should be: gcr.io/PROJECT_ID/image:tag

# Check for imagePullSecrets
kubectl get serviceaccount spark -n spark-jobs -o yaml
```

### Issue 4: Executor Pods OOMKilled

**Symptom:** Executors restart with OOMKilled reason

**Solutions:**
```bash
# Increase memory overhead
# spark.executor.memoryOverhead = 0.25 * spark.executor.memory

# Example for 8g executor:
# spark.executor.memory = 8g
# spark.executor.memoryOverhead = 2g
# Total container memory = 10g
```

---

## Summary

You now have:

✅ **Local Environment (KinD):**
- 4-node cluster
- Port mappings for Spark UI
- Local storage for shuffle data
- Resource quotas configured

✅ **Production Environment (GKE):**
- 3 node pools (System, Driver, Executor)
- Spot instances for executors (70% savings)
- Autoscaling (including scale to zero)
- Workload Identity enabled
- Network Policy enabled

✅ **Tools Installed:**
- kubectl, Helm, KinD
- Spark Operator
- Metrics Server

---

## What's Next

In **Part 3: Building Production Spark Images**, we'll:
- Create custom Docker images with all dependencies
- Add cloud storage connectors (GCS, S3)
- Include Delta Lake and monitoring agents
- Optimize image size with multi-stage builds

---

## Quick Reference Commands

```bash
# === KinD ===
kind create cluster --config kind-spark-cluster.yaml
kind delete cluster --name spark-cluster
kind load docker-image IMAGE --name spark-cluster

# === GKE ===
gcloud container clusters get-credentials $CLUSTER_NAME --region $REGION
gcloud container node-pools list --cluster $CLUSTER_NAME --region $REGION

# === General ===
kubectl get nodes -o wide
kubectl get pods -n spark-jobs -w
kubectl logs -n spark-jobs <pod-name> -f
kubectl describe pod -n spark-jobs <pod-name>
```

---

*Next: [Part 3: Building Production Spark Images →](link-to-part-3)*

*Previous: [Part 1: Architecture Deep Dive ←](link-to-part-1)*
