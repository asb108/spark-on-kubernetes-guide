# Part 4: Security Deep Dive — RBAC, Secrets & Network Policies

**Mastering Spark on Kubernetes Series**

*Your Spark jobs process your company's most sensitive data. Before running anything in production, you need to understand Kubernetes security from the ground up. This guide takes you from zero to production-ready security.*

---

## What You'll Learn

By the end of this guide, you'll understand:

- ✅ **Why** Kubernetes security exists and what problems it solves
- ✅ **How** authentication and authorization work in Kubernetes
- ✅ **What** RBAC is and how to configure it correctly
- ✅ **How** to set up keyless cloud access with GCP Workload Identity
- ✅ **How** to isolate pods with Network Policies
- ✅ **How** to manage secrets securely
- ✅ **How** to harden your containers

**Reading Time:** 60 minutes  
**Hands-On Time:** 45 minutes  
**Prerequisites:** [Part 3: Building Production Images](./part-03-docker-images.md)  
**Visual Reference:** [View Diagrams & Tables (Interactive)](./diagrams-part4.html)

---

## Table of Contents

1. [The Security Problem](#security-problem)
2. [Kubernetes Security Model Explained](#security-model)
3. [Authentication: Who Are You?](#authentication)
4. [Authorization: What Can You Do?](#authorization)
5. [RBAC Deep Dive](#rbac-deep-dive)
6. [ServiceAccounts Explained](#service-accounts)
7. [Building Spark's RBAC Step-by-Step](#spark-rbac)
8. [GCP Workload Identity](#gcp-workload-identity)
9. [Network Policies](#network-policies)
10. [Secrets Management](#secrets-management)
11. [Pod Security](#pod-security)
12. [Complete Security Setup](#complete-setup)

---

## The Security Problem {#security-problem}

Before diving into solutions, let's understand what we're protecting against.

### What Makes Spark Jobs High-Value Targets?

```
┌────────────────────────────────────────────────────────────────┐
│                     A Typical Spark Job                        │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│   INPUT                      PROCESS                  OUTPUT   │
│  ┌──────────┐             ┌──────────────┐         ┌─────────┐ │
│  │ Customer │────────────▶│  Your Spark  │────────▶│ Results │ │
│  │   Data   │             │     Code     │         │         │ │
│  │ (PII)    │             │              │         │ (S3/GCS)│ │
│  └──────────┘             └──────────────┘         └─────────┘ │
│                                  │                             │
│                                  ▼                             │
│                           ┌──────────────┐                     │
│                           │ Cloud Creds  │                     │
│                           │ DB Passwords │                     │
│                           │ API Keys     │                     │
│                           └──────────────┘                     │
│                                                                 │
└────────────────────────────────────────────────────────────────┘

If an attacker compromises this pod, they get:
• Access to sensitive customer data
• Cloud credentials to access more resources
• Network access to other services
• Potential to pivot to other systems
```

### Real-World Attack Scenarios

| Attack Vector | What Happens | Impact |
|--------------|--------------|--------|
| **Overprivileged Pod** | Pod can create any resource | Attacker deploys crypto miner |
| **Static Cloud Keys** | Keys never rotate | Leaked key = permanent access |
| **No Network Isolation** | Pod can reach any endpoint | Data exfiltration to attacker's server |
| **Secrets in Env Vars** | Exposed in logs/process list | Credentials leaked |
| **Root Container** | Container escape possible | Full node compromise |

### The Defense Strategy

```
                        Defense in Depth
                        
┌─────────────────────────────────────────────────────────────┐
│  Layer 1: Authentication                                     │
│  "Prove who you are before you can do anything"             │
├─────────────────────────────────────────────────────────────┤
│  Layer 2: Authorization (RBAC)                              │
│  "You can only do what you're explicitly allowed to do"     │
├─────────────────────────────────────────────────────────────┤
│  Layer 3: Network Policies                                  │
│  "You can only talk to approved endpoints"                  │
├─────────────────────────────────────────────────────────────┤
│  Layer 4: Secrets Management                                │
│  "Sensitive data is encrypted and access is audited"        │
├─────────────────────────────────────────────────────────────┤
│  Layer 5: Pod Security                                      │
│  "Even if compromised, blast radius is limited"             │
└─────────────────────────────────────────────────────────────┘
```

---

## Kubernetes Security Model Explained {#security-model}

Kubernetes has a two-step security model:

1. **Authentication**: Verify who is making the request
2. **Authorization**: Decide if they're allowed to do it

```
                    API Request Flow
                    
    ┌─────────┐         ┌─────────────┐         ┌─────────────┐
    │ kubectl │────────▶│ API Server  │────────▶│   etcd      │
    │  / Pod  │         │             │         │  (storage)  │
    └─────────┘         └─────────────┘         └─────────────┘
                              │
                              ▼
                    ┌─────────────────────────────────────┐
                    │                                     │
                    │  1. AUTHENTICATION                  │
                    │     "Who is this?"                  │
                    │     • Client certificate?           │
                    │     • ServiceAccount token?         │
                    │     • OIDC token?                   │
                    │                                     │
                    ├─────────────────────────────────────┤
                    │                                     │
                    │  2. AUTHORIZATION                   │
                    │     "Can they do this?"             │
                    │     • RBAC: Check roles/bindings    │
                    │     • If allowed → Continue         │
                    │     • If denied → 403 Forbidden     │
                    │                                     │
                    ├─────────────────────────────────────┤
                    │                                     │
                    │  3. ADMISSION CONTROL               │
                    │     "Should we modify/reject?"      │
                    │     • Mutating webhooks             │
                    │     • Validating webhooks           │
                    │                                     │
                    └─────────────────────────────────────┘
```

---

## Authentication: Who Are You? {#authentication}

In Kubernetes, there are two types of identities:

### 1. Human Users

Humans (you, me, CI systems) authenticate using:
- **Client Certificates** (kubeconfig)
- **OIDC Tokens** (Google, Azure AD)
- **Token Files**

When you run `kubectl get pods`, your kubeconfig contains credentials that prove who you are.

### 2. Service Accounts (Pods)

Pods don't have kubeconfig files. They use **ServiceAccounts**.

```yaml
# Every pod gets a token mounted automatically
$ kubectl exec my-pod -- cat /var/run/secrets/kubernetes.io/serviceaccount/token
eyJhbGciOiJSUzI1NiIsImtpZCI6...  # This is a JWT token
```

This token is a **JWT (JSON Web Token)** that contains:

```json
{
  "iss": "kubernetes/serviceaccount",
  "sub": "system:serviceaccount:spark-jobs:spark",
  "namespace": "spark-jobs",
  "serviceaccount.name": "spark"
}
```

The API server validates this token and knows: *"This is the 'spark' ServiceAccount from the 'spark-jobs' namespace."*

---

## Authorization: What Can You Do? {#authorization}

After authentication, Kubernetes checks: **"Is this identity allowed to perform this action?"**

Kubernetes supports multiple authorization modes, but **RBAC** is the standard.

### RBAC = Role-Based Access Control

The core idea: **Define roles with specific permissions, then assign roles to identities.**

Think of it like a company:
- **Role**: "Software Engineer" can read/write code, run tests
- **Role**: "Accountant" can view/edit financial records
- **Binding**: "Alice" is assigned the "Software Engineer" role

In Kubernetes terms:

| Real World | Kubernetes |
|------------|------------|
| Job Title | Role / ClusterRole |
| Responsibilities | Permissions (verbs on resources) |
| Employee | ServiceAccount / User |
| Assigning someone to a job | RoleBinding / ClusterRoleBinding |

---

## RBAC Deep Dive {#rbac-deep-dive}

Let's understand each RBAC component in detail.

### Component 1: Role (or ClusterRole)

A **Role** defines **what actions** are allowed on **what resources**.

```yaml
# This Role allows reading pods in the spark-jobs namespace
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: pod-reader          # Name of this role
  namespace: spark-jobs     # Only applies to this namespace
rules:
  - apiGroups: [""]         # "" = core API group (pods, services, etc.)
    resources: ["pods"]     # What can be accessed
    verbs: ["get", "list"]  # What actions are allowed
```

**Understanding the fields:**

| Field | What It Means | Examples |
|-------|---------------|----------|
| `apiGroups` | API group of the resource | `""` (core), `"apps"`, `"batch"` |
| `resources` | Type of Kubernetes objects | `"pods"`, `"services"`, `"deployments"` |
| `verbs` | Actions you can perform | `"get"`, `"create"`, `"delete"` |

**All available verbs:**

| Verb | HTTP Equivalent | Description |
|------|-----------------|-------------|
| `get` | GET /resource/{name} | Read a single resource |
| `list` | GET /resource | Read all resources of this type |
| `watch` | GET /resource?watch=true | Stream changes in real-time |
| `create` | POST /resource | Create a new resource |
| `update` | PUT /resource/{name} | Replace entire resource |
| `patch` | PATCH /resource/{name} | Modify specific fields |
| `delete` | DELETE /resource/{name} | Delete a resource |
| `deletecollection` | DELETE /resource | Delete multiple resources |

### Component 2: RoleBinding

A **RoleBinding** connects a Role to a Subject (who gets the permissions).

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: read-pods-binding
  namespace: spark-jobs
subjects:                          # WHO gets the permissions
  - kind: ServiceAccount
    name: spark                    # The ServiceAccount name
    namespace: spark-jobs          # Must specify namespace for SAs
roleRef:                           # WHICH Role to assign
  kind: Role
  name: pod-reader                 # The Role defined above
  apiGroup: rbac.authorization.k8s.io
```

### Role vs ClusterRole

| Feature | Role | ClusterRole |
|---------|------|-------------|
| Scope | Single namespace | Entire cluster |
| Use with | RoleBinding | ClusterRoleBinding or RoleBinding |
| Use case | Namespace-scoped workloads | Cluster-wide resources or reusable roles |

**Example: ClusterRole for cluster-wide resources**

```yaml
# Nodes are cluster-scoped (not in any namespace)
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: node-viewer   # No namespace field!
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list"]
```

### Visualizing RBAC

```
┌─────────────────────────────────────────────────────────────────┐
│                        RBAC in Action                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  WHO                     CONNECTS                   WHAT         │
│  ────                    ────────                   ────         │
│                                                                  │
│  ┌──────────────┐       ┌──────────────┐       ┌──────────────┐ │
│  │ServiceAccount│──────▶│ RoleBinding  │──────▶│    Role      │ │
│  │              │       │              │       │              │ │
│  │ name: spark  │       │ subjects:    │       │ rules:       │ │
│  │ ns: spark-   │       │   - spark    │       │   pods:      │ │
│  │     jobs     │       │ roleRef:     │       │     create   │ │
│  │              │       │   spark-role │       │     delete   │ │
│  └──────────────┘       └──────────────┘       └──────────────┘ │
│         │                                              │         │
│         │              RESULT                          │         │
│         │              ──────                          │         │
│         ▼                                              ▼         │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ "The 'spark' ServiceAccount can create and delete pods     ││
│  │  in the 'spark-jobs' namespace"                           ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## ServiceAccounts Explained {#service-accounts}

A **ServiceAccount** is the identity that pods use to talk to the Kubernetes API.

### The Default Problem

Every namespace has a `default` ServiceAccount:

```bash
$ kubectl get serviceaccounts -n spark-jobs
NAME      SECRETS   AGE
default   1         5d
```

**Problem**: If you don't specify a ServiceAccount, pods use `default`. This leads to:
1. All pods sharing the same identity (no isolation)
2. Overly-permissive permissions (or none at all)
3. No audit trail of which workload did what

### Creating a Dedicated ServiceAccount

```yaml
# spark-serviceaccount.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark                    # Choose a descriptive name
  namespace: spark-jobs
  labels:
    app: spark
    component: security
```

Apply it:

```bash
kubectl apply -f spark-serviceaccount.yaml
```

### Using Your ServiceAccount in Pods

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: spark-driver
  namespace: spark-jobs
spec:
  serviceAccountName: spark     # <-- USE YOUR SA, NOT default
  containers:
    - name: driver
      image: spark:3.5.0
```

### What Happens Inside the Pod

When a pod starts with `serviceAccountName: spark`:

1. Kubernetes mounts a token at `/var/run/secrets/kubernetes.io/serviceaccount/`
2. The token proves the pod's identity to the API server
3. Any API calls from the pod use this identity

```bash
# Inside the pod
$ ls /var/run/secrets/kubernetes.io/serviceaccount/
ca.crt     # Cluster CA certificate
namespace  # Current namespace name  
token      # JWT token for authentication

$ cat namespace
spark-jobs

$ cat token | cut -d. -f2 | base64 -d | jq .
{
  "iss": "https://kubernetes.default.svc.cluster.local",
  "sub": "system:serviceaccount:spark-jobs:spark",
  ...
}
```

---

## Building Spark's RBAC Step-by-Step {#spark-rbac}

Now let's build a complete, minimal RBAC setup for Spark.

### Step 1: Understand What Spark Needs

Spark on Kubernetes does the following:

| Action | Why | Resource | Verbs Needed |
|--------|-----|----------|--------------|
| Create executor pods | Run distributed tasks | pods | create |
| Monitor executors | Track progress | pods | get, list, watch |
| Clean up executors | Remove finished pods | pods | delete |
| Create headless service | Enable driver discovery | services | create, get, delete |
| Create ConfigMaps | Pass config to executors | configmaps | create, get, delete |

### Step 2: Create the Namespace

```yaml
# 01-namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: spark-jobs
  labels:
    app.kubernetes.io/name: spark
```

```bash
kubectl apply -f 01-namespace.yaml
```

### Step 3: Create the ServiceAccount

```yaml
# 02-serviceaccount.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark-jobs
```

```bash
kubectl apply -f 02-serviceaccount.yaml
```

### Step 4: Create the Role with Minimal Permissions

```yaml
# 03-role.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-role
  namespace: spark-jobs
rules:
  # Permission 1: Manage executor pods
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "watch", "delete"]
  
  # Permission 2: Create headless service for driver
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["create", "get", "delete"]
  
  # Permission 3: Configuration for executors
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["create", "get", "delete"]
```

```bash
kubectl apply -f 03-role.yaml
```

### Step 5: Create the RoleBinding

```yaml
# 04-rolebinding.yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-role-binding
  namespace: spark-jobs
subjects:
  - kind: ServiceAccount
    name: spark
    namespace: spark-jobs
roleRef:
  kind: Role
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
```

```bash
kubectl apply -f 04-rolebinding.yaml
```

### Step 6: Verify Your Setup

```bash
# Check if spark can create pods
kubectl auth can-i create pods \
  --as=system:serviceaccount:spark-jobs:spark \
  -n spark-jobs
# yes

# Check if spark can delete nodes (should be no!)
kubectl auth can-i delete nodes \
  --as=system:serviceaccount:spark-jobs:spark
# no

# Check if spark can do things in other namespaces
kubectl auth can-i create pods \
  --as=system:serviceaccount:spark-jobs:spark \
  -n default
# no
```

### All-in-One YAML

For convenience, here's everything in one file:

```yaml
# spark-rbac-complete.yaml
---
apiVersion: v1
kind: Namespace
metadata:
  name: spark-jobs
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark-jobs
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-role
  namespace: spark-jobs
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["create", "get", "delete"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["create", "get", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-role-binding
  namespace: spark-jobs
subjects:
  - kind: ServiceAccount
    name: spark
    namespace: spark-jobs
roleRef:
  kind: Role
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
```

---

## GCP Workload Identity {#gcp-workload-identity}

Now that Spark can talk to Kubernetes, how does it access **cloud resources** like GCS buckets?

### The Problem with Service Account Keys

Traditional approach (BAD):

```bash
# Export a JSON key (NEVER do this in production!)
gcloud iam service-accounts keys create key.json \
  --iam-account=spark-sa@project.iam.gserviceaccount.com

# Create K8s secret from key
kubectl create secret generic gcp-key --from-file=key.json

# Mount in pod and set GOOGLE_APPLICATION_CREDENTIALS
```

**Why this is dangerous:**
- Keys never expire (until manually rotated)
- If leaked, attacker has permanent access
- Hard to track which pod used the key
- Rotation requires redeploying all pods

### The Solution: Workload Identity

**Workload Identity** federates Kubernetes ServiceAccounts to GCP IAM. No keys involved!

```
┌──────────────────────────────────────────────────────────────────┐
│                     How Workload Identity Works                   │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                        GKE Cluster                           │ │
│  │                                                              │ │
│  │   ┌──────────────┐          ┌─────────────────────────────┐ │ │
│  │   │  Spark Pod   │          │  GKE Metadata Server        │ │ │
│  │   │              │   1. Pod │  (169.254.169.254)          │ │ │
│  │   │  K8s SA:     │──────────│                             │ │ │
│  │   │    spark     │  requests│  Intercepts requests and    │ │ │
│  │   │              │  token   │  exchanges K8s token for    │ │ │
│  │   └──────────────┘          │  GCP access token           │ │ │
│  │                             └──────────────┬──────────────┘ │ │
│  └────────────────────────────────────────────│────────────────┘ │
│                                               │                   │
│                                    2. Validate│federation        │
│                                               ▼                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                     Google Cloud IAM                         │ │
│  │                                                              │ │
│  │   Workload Identity Pool:                                   │ │
│  │     PROJECT_ID.svc.id.goog                                  │ │
│  │                                                              │ │
│  │   Maps: spark-jobs/spark  ──▶  spark-sa@project.iam...     │ │
│  │                                                              │ │
│  │   3. Returns GCP access token with spark-sa's permissions   │ │
│  │                                                              │ │
│  └──────────────────────────────────────────────────────│──────┘ │
│                                                         │        │
│                                              4. Pod uses│token   │
│                                                         ▼        │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              Google Cloud Storage / BigQuery / etc.          │ │
│  │                                                              │ │
│  │   Validates token, grants access based on IAM roles         │ │
│  │                                                              │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

### Step-by-Step Setup

**Prerequisites:**
- GKE cluster with Workload Identity enabled
- `gcloud` CLI authenticated

**Step 1: Enable Workload Identity on your cluster**

```bash
# Set variables
PROJECT_ID="your-gcp-project"
CLUSTER_NAME="spark-cluster"
REGION="us-central1"

# Enable Workload Identity (if not already)
gcloud container clusters update ${CLUSTER_NAME} \
  --region=${REGION} \
  --workload-pool=${PROJECT_ID}.svc.id.goog
```

**Step 2: Create a GCP Service Account**

```bash
# Create the GCP SA that will access cloud resources
GCP_SA_NAME="spark-gcs-sa"

gcloud iam service-accounts create ${GCP_SA_NAME} \
  --display-name="Spark GCS Access Service Account"
```

**Step 3: Grant IAM permissions to the GCP SA**

```bash
# Grant permission to read from GCS
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${GCP_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

# Optional: Add more roles as needed
# --role="roles/bigquery.dataEditor"
```

**Step 4: Link K8s ServiceAccount to GCP ServiceAccount**

This is the key step—it creates the federation:

```bash
# Allow K8s SA to impersonate GCP SA
gcloud iam service-accounts add-iam-policy-binding \
  ${GCP_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[spark-jobs/spark]"
```

This says: *"The K8s ServiceAccount `spark` in namespace `spark-jobs` is allowed to act as the GCP SA `spark-gcs-sa`."*

**Step 5: Annotate the K8s ServiceAccount**

```bash
kubectl annotate serviceaccount spark \
  --namespace=spark-jobs \
  iam.gke.io/gcp-service-account=${GCP_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com
```

Or update the YAML:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark-jobs
  annotations:
    iam.gke.io/gcp-service-account: spark-gcs-sa@your-project.iam.gserviceaccount.com
```

**Step 6: Verify it works**

```bash
# Run a test pod
kubectl run gcloud-test \
  --image=google/cloud-sdk:slim \
  --serviceaccount=spark \
  --namespace=spark-jobs \
  --rm -it --restart=Never \
  -- gcloud auth list

# Expected output:
#          ACTIVE  ACCOUNT
#          *       spark-gcs-sa@your-project.iam.gserviceaccount.com

# Test GCS access
kubectl run gcs-test \
  --image=google/cloud-sdk:slim \
  --serviceaccount=spark \
  --namespace=spark-jobs \
  --rm -it --restart=Never \
  -- gsutil ls gs://your-bucket/
```

### Using in SparkApplication

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: gcs-reader
  namespace: spark-jobs
spec:
  type: Python
  mode: cluster
  image: spark-prod:3.5.0
  mainApplicationFile: gs://my-bucket/jobs/process.py  # Direct GCS access!
  sparkVersion: "3.5.0"
  driver:
    serviceAccount: spark  # Uses Workload Identity
    cores: 1
    memory: "2g"
  executor:
    serviceAccount: spark  # Executors also need it
    instances: 2
    cores: 2
    memory: "4g"
```

> **Note on Other Cloud Providers:** AWS (IRSA) and Azure (Managed Identity) have similar concepts but different implementations. These will be covered in a future appendix once we've fully tested them.

---

## Network Policies {#network-policies}

Network Policies are Kubernetes' built-in firewall for pods.

### The Default Problem

By default, **any pod can talk to any other pod** in the cluster:

```
Without Network Policies:

┌──────────────────────────────────────────────────────────────┐
│                         Cluster                               │
│                                                               │
│   ┌─────────┐       ┌─────────┐       ┌─────────┐           │
│   │  Spark  │◀─────▶│  MySQL  │◀─────▶│  Redis  │           │
│   │  Driver │       │   DB    │       │  Cache  │           │
│   └─────────┘       └─────────┘       └─────────┘           │
│        │                                    │                │
│        │         ANY POD CAN REACH          │                │
│        ▼         ANY OTHER POD!             ▼                │
│   ┌─────────┐                         ┌─────────┐           │
│   │  Spark  │                         │ Attacker│           │
│   │Executor │                         │   Pod   │           │
│   └─────────┘                         └─────────┘           │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

If an attacker compromises any pod, they can:
- Connect to your databases
- Reach internal APIs
- Exfiltrate data

### How Network Policies Work

Network Policies use **labels** to select which pods the rules apply to.

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: my-policy
  namespace: spark-jobs
spec:
  podSelector:          # Which pods this policy applies to
    matchLabels:
      app: spark
  policyTypes:
    - Ingress           # Control incoming traffic
    - Egress            # Control outgoing traffic
  ingress:              # Rules for incoming traffic
    - from: [...]
  egress:               # Rules for outgoing traffic
    - to: [...]
```

### Step 1: Default Deny Everything

Start with zero trust—block all traffic, then allow only what's needed:

```yaml
# deny-all.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: spark-jobs
spec:
  podSelector: {}       # Empty = applies to ALL pods in namespace
  policyTypes:
    - Ingress
    - Egress
```

After applying this, **no pod in `spark-jobs` can send or receive any traffic**.

### Step 2: Allow DNS (Required!)

Pods need DNS to resolve service names:

```yaml
# allow-dns.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-dns
  namespace: spark-jobs
spec:
  podSelector: {}       # All pods
  policyTypes:
    - Egress
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
          podSelector:
            matchLabels:
              k8s-app: kube-dns
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
```

### Step 3: Allow Spark Internal Communication

Spark driver and executors need to talk to each other:

```yaml
# spark-internal.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-internal
  namespace: spark-jobs
spec:
  podSelector:
    matchLabels:
      app: spark        # Applies to pods with app=spark
  policyTypes:
    - Ingress
    - Egress
  ingress:
    # Allow traffic from other Spark pods
    - from:
        - podSelector:
            matchLabels:
              app: spark
      ports:
        - protocol: TCP
          port: 7078    # Block manager port
        - protocol: TCP
          port: 7079    # Block manager port
        - protocol: TCP
          port: 4040    # Spark UI
  egress:
    # Allow traffic to other Spark pods
    - to:
        - podSelector:
            matchLabels:
              app: spark
      ports:
        - protocol: TCP
          port: 7078
        - protocol: TCP
          port: 7079
```

### Step 4: Allow External Cloud Access

Spark needs to reach cloud storage:

```yaml
# allow-cloud-egress.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-cloud-egress
  namespace: spark-jobs
spec:
  podSelector:
    matchLabels:
      app: spark
  policyTypes:
    - Egress
  egress:
    # Allow HTTPS to external endpoints (GCS, etc.)
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - 10.0.0.0/8       # Block internal networks
              - 172.16.0.0/12    # to prevent lateral movement
              - 192.168.0.0/16
      ports:
        - protocol: TCP
          port: 443
```

### Visual: After Network Policies

```
With Network Policies:

┌──────────────────────────────────────────────────────────────┐
│                         Cluster                               │
│                                                               │
│   ┌─────────┐       ┌─────────┐       ┌─────────┐           │
│   │  Spark  │◀═════▶│  Spark  │       │  MySQL  │           │
│   │  Driver │ALLOWED│Executor │       │   DB    │           │
│   └─────────┘       └─────────┘       └────┬────┘           │
│        │                                    │                │
│        │ ALLOWED                   BLOCKED  │                │
│        │ (:443)                  ──────────┘                │
│        ▼                                                     │
│   ┌─────────────────┐          ┌─────────┐                  │
│   │   Google Cloud  │          │ Attacker│                  │
│   │    Storage      │          │   Pod   │───X BLOCKED      │
│   └─────────────────┘          └─────────┘                  │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

---

## Secrets Management {#secrets-management}

### Option 1: Kubernetes Secrets (Basic)

**Creating a secret:**

```bash
# From literal values
kubectl create secret generic spark-secrets \
  --namespace=spark-jobs \
  --from-literal=database-password=supersecret123

# From a file
kubectl create secret generic spark-secrets \
  --namespace=spark-jobs \
  --from-file=credentials.json
```

**Using in pods:**

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
    - name: spark-driver
      image: spark:3.5.0
      # Option A: As environment variable
      env:
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: spark-secrets
              key: database-password
      # Option B: As mounted file (PREFERRED)
      volumeMounts:
        - name: secrets
          mountPath: /etc/secrets
          readOnly: true
  volumes:
    - name: secrets
      secret:
        secretName: spark-secrets
```

**IMPORTANT: Always mount as files, not env vars!**

Why? Environment variables can leak through:
- Process lists (`ps aux`)
- Error logs
- Core dumps
- Child processes

### Option 2: External Secrets (Production)

For production, use an external secrets manager:

```yaml
# External Secrets Operator syncs from cloud secret managers
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: spark-secrets
  namespace: spark-jobs
spec:
  refreshInterval: 1h              # Auto-refresh every hour
  secretStoreRef:
    kind: ClusterSecretStore
    name: gcp-secret-manager       # or aws-secrets-manager
  target:
    name: spark-secrets            # K8s Secret to create
  data:
    - secretKey: database-password
      remoteRef:
        key: projects/my-project/secrets/spark-db-password
        version: latest
```

---

## Pod Security {#pod-security}

The last layer: harden the containers themselves.

### Key Security Settings

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: secure-spark-driver
spec:
  securityContext:
    runAsNonRoot: true         # Refuse to start as root
    runAsUser: 185             # Spark user UID
    runAsGroup: 185
    fsGroup: 185
    seccompProfile:
      type: RuntimeDefault     # Apply default seccomp profile
  containers:
    - name: driver
      image: spark:3.5.0
      securityContext:
        allowPrivilegeEscalation: false   # Can't become root later
        readOnlyRootFilesystem: true      # Can't write to container
        capabilities:
          drop:
            - ALL              # Remove all Linux capabilities
      # Need writable directories? Use emptyDir
      volumeMounts:
        - name: tmp
          mountPath: /tmp
        - name: spark-local
          mountPath: /opt/spark/work-dir
  volumes:
    - name: tmp
      emptyDir: {}
    - name: spark-local
      emptyDir: {}
```

### What Each Setting Does

| Setting | Protection |
|---------|------------|
| `runAsNonRoot` | Blocks container from running as root (UID 0) |
| `readOnlyRootFilesystem` | Prevents writing to container filesystem |
| `allowPrivilegeEscalation` | Blocks gaining more privileges after start |
| `capabilities.drop: ALL` | Removes all Linux capabilities (like raw sockets) |
| `seccompProfile` | Restricts available system calls |

---

## Complete Security Setup {#complete-setup}

Here's everything combined:

```yaml
# complete-secure-spark-rbac.yaml
---
# 1. Namespace with security labels
apiVersion: v1
kind: Namespace
metadata:
  name: spark-jobs
  labels:
    pod-security.kubernetes.io/enforce: baseline
---
# 2. ServiceAccount with Workload Identity
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark-jobs
  annotations:
    # Uncomment and set for GCP:
    # iam.gke.io/gcp-service-account: spark-sa@PROJECT.iam.gserviceaccount.com
---
# 3. Minimal Role
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-role
  namespace: spark-jobs
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["create", "get", "delete"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["create", "get", "delete"]
---
# 4. RoleBinding
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-role-binding
  namespace: spark-jobs
subjects:
  - kind: ServiceAccount
    name: spark
    namespace: spark-jobs
roleRef:
  kind: Role
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
---
# 5. Default Deny Network Policy
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny
  namespace: spark-jobs
spec:
  podSelector: {}
  policyTypes:
    - Ingress
    - Egress
---
# 6. Allow DNS
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-dns
  namespace: spark-jobs
spec:
  podSelector: {}
  policyTypes:
    - Egress
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
      ports:
        - protocol: UDP
          port: 53
---
# 7. Allow Spark Internal + Cloud Egress
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-network
  namespace: spark-jobs
spec:
  podSelector:
    matchLabels:
      app: spark
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: spark
  egress:
    - to:
        - podSelector:
            matchLabels:
              app: spark
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - 10.0.0.0/8
              - 172.16.0.0/12
              - 192.168.0.0/16
      ports:
        - protocol: TCP
          port: 443
```

---

## Summary

You've learned:

✅ **Why security matters** — Spark processes sensitive data  
✅ **How K8s security works** — Authentication → Authorization  
✅ **RBAC from scratch** — Roles, Bindings, ServiceAccounts  
✅ **Least privilege** — Only grant what's needed  
✅ **Workload Identity** — Keyless cloud access on GCP  
✅ **Network Policies** — Pod-level firewalls  
✅ **Secrets management** — Safe ways to handle credentials  
✅ **Pod security** — Container hardening  

---

## What's Next

In **Part 5: spark-submit Mastery**, we'll cover:
- Complete spark-submit configuration reference
- Client vs Cluster mode deep dive
- Dynamic allocation on Kubernetes
- Pod templates for advanced customization
- Debugging common issues

---

*Next: [Part 5: spark-submit Mastery →](./part-05-spark-submit.md)*

*Previous: [Part 3: Building Production Images ←](./part-03-docker-images.md)*
