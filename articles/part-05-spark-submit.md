# Part 5: spark-submit Mastery — From First Principles

**Mastering Spark on Kubernetes Series**

*Before running your first Spark job on Kubernetes, you need to understand spark-submit deeply. This guide starts from the very basics and builds up to production-ready configurations.*

---

## What You'll Learn

By the end of this guide, you'll understand:

- ✅ What spark-submit is and why it exists
- ✅ How Spark applications are structured
- ✅ How spark-submit communicates with Kubernetes
- ✅ Client vs Cluster mode — conceptually, not just practically
- ✅ Every configuration option explained from first principles
- ✅ How Spark calculates memory and CPU requirements
- ✅ Dynamic allocation — how and why executors scale
- ✅ Complete debugging methodology

**Reading Time:** 60 minutes  
**Hands-On Time:** 45 minutes  
**Prerequisites:** [Part 4: Security Deep Dive](./article-04-security.md)  
**Visual Reference:** [View Diagrams & Tables (Interactive)](./diagrams-part5.html)

---

## Table of Contents

1. [What is spark-submit?](#what-is-spark-submit)
2. [Anatomy of a Spark Application](#spark-application-anatomy)
3. [How spark-submit Talks to Kubernetes](#spark-submit-kubernetes)
4. [Your First Spark Job](#first-job)
5. [Deployment Modes Explained](#deployment-modes)
6. [Configuration Deep Dive](#configuration-deep-dive)
7. [Memory Management from First Principles](#memory-management)
8. [CPU and Parallelism](#cpu-parallelism)
9. [Node Selection and Scheduling](#node-selection)
10. [Dynamic Allocation](#dynamic-allocation)
11. [Pod Templates](#pod-templates)
12. [PySpark Considerations](#pyspark)
13. [Debugging Methodology](#debugging)

---

## What is spark-submit? {#what-is-spark-submit}

### The Problem It Solves

Imagine you've written a Spark application — maybe it reads data from storage, transforms it, and writes results somewhere. Your code might look like this:

```python
# my_etl_job.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyETL").getOrCreate()
df = spark.read.parquet("gs://bucket/input")
result = df.filter(df.amount > 100).groupBy("category").count()
result.write.parquet("gs://bucket/output")
```

But here's the question: **How do you actually run this?**

Your code needs:
- A cluster of machines to run on
- Enough memory and CPU
- Access to your data storage
- A way to distribute the work across machines

**spark-submit is the bridge** between your code and the execution environment.

### What spark-submit Actually Does

Think of spark-submit as a **deployment tool**. It:

1. **Packages your application** — Bundles your code, configs, and dependencies
2. **Connects to your cluster** — YARN, Kubernetes, standalone, or local
3. **Requests resources** — Memory, CPU, number of executors
4. **Starts your driver** — The brain of your application
5. **Manages the lifecycle** — Starts executors, monitors progress, cleans up

```
┌────────────────────────────────────────────────────────────────────────┐
│                    What spark-submit Does                               │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   YOU HAVE:                           YOU NEED:                         │
│   ─────────                           ─────────                         │
│                                                                         │
│   • Your code (my_job.py)             • A running driver process        │
│   • Configuration (--conf)            • Multiple executor processes     │
│   • Cluster address (--master)        • Coordination between them       │
│   • Resource requirements             • Access to your data             │
│                                                                         │
│                        spark-submit                                     │
│                        ─────────────                                    │
│                             │                                           │
│                             │ Translates your request into              │
│                             │ cluster-specific deployment               │
│                             ▼                                           │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │                    YOUR RUNNING APPLICATION                      │  │
│   │                                                                  │  │
│   │   ┌──────────────────┐                                          │  │
│   │   │     DRIVER       │  ← Your SparkSession lives here          │  │
│   │   │  • SparkContext  │                                          │  │
│   │   │  • DAG Scheduler │                                          │  │
│   │   │  • Task Scheduler│                                          │  │
│   │   └────────┬─────────┘                                          │  │
│   │            │                                                     │  │
│   │    ┌───────┼───────┐                                            │  │
│   │    ▼       ▼       ▼                                            │  │
│   │  ┌────┐  ┌────┐  ┌────┐                                         │  │
│   │  │Exec│  │Exec│  │Exec│  ← Actual data processing happens here  │  │
│   │  │ 1  │  │ 2  │  │ 3  │                                         │  │
│   │  └────┘  └────┘  └────┘                                         │  │
│   │                                                                  │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Anatomy of a Spark Application {#spark-application-anatomy}

Before we dive into spark-submit flags, let's understand what a Spark application consists of.

### The Driver: The Brain

The **driver** is a single JVM process that:

1. **Creates SparkSession/SparkContext** — The entry point to your cluster
2. **Analyzes your code** — Builds a DAG (Directed Acyclic Graph) of operations
3. **Optimizes execution** — Catalyst optimizer rewrites your queries
4. **Schedules tasks** — Breaks work into small tasks and assigns them
5. **Collects results** — Gathers output from executors

**Where does it run?**
- In **cluster mode**: Inside the Kubernetes cluster (as a pod)
- In **client mode**: On your local machine

### The Executors: The Workers

**Executors** are JVM processes (one per pod in Kubernetes) that:

1. **Receive tasks** from the driver
2. **Execute those tasks** on data partitions
3. **Cache data** in memory if asked
4. **Return results** to the driver

**Key characteristics:**
- Each executor can run multiple tasks in parallel (based on cores)
- They're started after the driver begins
- They persist for the entire application lifetime (unless dynamic allocation)

### The Cluster Manager: The Orchestrator

This is where **Kubernetes** comes in. The cluster manager:

1. **Receives resource requests** from spark-submit
2. **Allocates containers/pods** for driver and executors
3. **Monitors health** and handles failures
4. **Deallocates resources** when the job completes

```
┌────────────────────────────────────────────────────────────────────────┐
│                 Spark Application Components                            │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│                        ┌─────────────────────────────────┐             │
│                        │      CLUSTER MANAGER            │             │
│                        │        (Kubernetes)             │             │
│                        │                                 │             │
│                        │  • Receives pod requests        │             │
│                        │  • Schedules pods to nodes      │             │
│                        │  • Manages pod lifecycle        │             │
│                        └────────────────┬────────────────┘             │
│                                         │                               │
│                        Resource allocation & monitoring                 │
│                                         │                               │
│                                         ▼                               │
│   ┌──────────────────────────────────────────────────────────────────┐ │
│   │                      YOUR SPARK APPLICATION                       │ │
│   │                                                                   │ │
│   │   DRIVER POD                      EXECUTOR PODS                   │ │
│   │   ──────────                      ─────────────                   │ │
│   │                                                                   │ │
│   │   ┌──────────────────┐            ┌────────────────┐             │ │
│   │   │ SparkSession     │            │  Executor 1    │             │ │
│   │   │                  │   tasks    │  ┌──────────┐  │             │ │
│   │   │ DAGScheduler     │ ─────────▶ │  │ Task 1   │  │             │ │
│   │   │                  │            │  │ Task 2   │  │             │ │
│   │   │ TaskScheduler    │  results   │  │ Task 3   │  │             │ │
│   │   │                  │ ◀───────── │  │ Task 4   │  │             │ │
│   │   │ Web UI (:4040)   │            │  └──────────┘  │             │ │
│   │   └──────────────────┘            └────────────────┘             │ │
│   │                                                                   │ │
│   │                                   ┌────────────────┐             │ │
│   │                                   │  Executor 2    │             │ │
│   │                                   │  ┌──────────┐  │             │ │
│   │                                   │  │ Task 5   │  │             │ │
│   │                                   │  │ Task 6   │  │             │ │
│   │                                   │  │ ...      │  │             │ │
│   │                                   │  └──────────┘  │             │ │
│   │                                   └────────────────┘             │ │
│   │                                                                   │ │
│   └──────────────────────────────────────────────────────────────────┘ │
│                                                                         │
└────────────────────────────────────────────────────────────────────────┘
```

---

## How spark-submit Talks to Kubernetes {#spark-submit-kubernetes}

Now let's understand the specific mechanics of how spark-submit works with Kubernetes.

### Step-by-Step Execution Flow

When you run `spark-submit --master k8s://...`, here's exactly what happens:

#### Step 1: Parse Configuration

spark-submit reads:
- Command-line flags (`--conf`, `--master`, `--deploy-mode`)
- spark-defaults.conf file (if present)
- Environment variables (SPARK_HOME, etc.)

It builds a complete configuration object with all settings.

#### Step 2: Connect to Kubernetes

spark-submit uses the **fabric8 Kubernetes client** (a Java library) to:
- Read your kubeconfig (~/.kube/config)
- Authenticate to the API server
- Verify it can create pods in the target namespace

```bash
# The --master flag points to your Kubernetes API server
--master k8s://https://192.168.1.100:6443

# spark-submit extracts:
#   Protocol: k8s (means use Kubernetes backend)
#   API URL:  https://192.168.1.100:6443
```

#### Step 3: Build Driver Pod Specification

spark-submit constructs a complete Pod specification:

```yaml
# This is what spark-submit builds internally
apiVersion: v1
kind: Pod
metadata:
  name: my-app-driver
  namespace: spark-jobs
  labels:
    spark-role: driver
    spark-app-name: my-app
spec:
  serviceAccountName: spark
  containers:
  - name: spark-kubernetes-driver
    image: myrepo/spark:3.5.0
    resources:
      requests:
        memory: "4Gi"    # driver.memory + overhead
        cpu: "2"
      limits:
        memory: "4Gi"
        cpu: "2"
    env:
    - name: SPARK_DRIVER_MEMORY
      value: "4g"
    # ... many more env vars from your --conf flags
```

#### Step 4: Submit to Kubernetes API

spark-submit sends a POST request to create the driver pod:

```
POST /api/v1/namespaces/spark-jobs/pods

Body: <the pod spec from step 3>
```

At this point, **spark-submit's job is mostly done** (in cluster mode).

#### Step 5: Driver Starts and Takes Over

The driver pod is scheduled to a node and starts running. The driver:

1. Initializes SparkContext
2. Reads your application code
3. Creates executor pod requests
4. Submits these to Kubernetes API (using its own ServiceAccount)

```
┌──────────────────────────────────────────────────────────────────────┐
│              Who Creates What in Kubernetes Mode                      │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│   spark-submit                    Driver Pod                          │
│   ─────────────                   ──────────                          │
│                                                                       │
│   Creates:                        Creates:                            │
│   • Driver pod                    • Executor pods (N of them)         │
│                                   • Headless service (for executors   │
│                                     to find driver)                   │
│                                                                       │
│   That's why the ServiceAccount needs permissions for:                │
│   • pods (create, get, delete)                                       │
│   • services (create, delete)                                        │
│   • configmaps (create, delete) — for executor configs               │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

#### Step 6: Executors Register

Each executor pod:
1. Starts and finds the driver (via headless service)
2. Sends a heartbeat registration
3. Receives task assignments
4. Begins processing

#### Step 7: Job Execution

Tasks flow from driver to executors, results flow back.

#### Step 8: Cleanup

When the application finishes:
1. Driver signals executors to shut down
2. Executor pods are deleted
3. Driver pod exits (and is optionally deleted)

---

## Your First Spark Job {#first-job}

Let's run a simple job and understand each part.

### Prerequisites Check

```bash
# 1. Verify kubectl can reach your cluster
kubectl cluster-info
# Output: Kubernetes control plane is running at https://...

# 2. Verify you have a namespace for Spark
kubectl get namespace spark-jobs
# If not: kubectl create namespace spark-jobs

# 3. Verify the ServiceAccount exists (from Part 4)
kubectl get serviceaccount spark -n spark-jobs

# 4. Verify RBAC is set up correctly
kubectl auth can-i create pods \
  --as=system:serviceaccount:spark-jobs:spark \
  -n spark-jobs
# Output: yes
```

### The Minimal spark-submit Command

```bash
spark-submit \
  --master k8s://https://$(kubectl config view -o jsonpath='{.clusters[0].cluster.server}') \
  --deploy-mode cluster \
  --name spark-pi \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.kubernetes.container.image=apache/spark:3.5.0 \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar 100
```

### Understanding Each Flag

Let me explain each part in detail:

#### `--master k8s://https://...`

This tells Spark to use Kubernetes as the cluster manager.

```
--master k8s://https://192.168.1.100:6443
         ↑↑↑ ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
         │   └── Kubernetes API server URL
         │
         └── Protocol: "k8s" = Kubernetes mode
             Other options: yarn, spark://, mesos://, local[*]
```

**Getting your API server URL:**
```bash
# Method 1: From kubectl config
kubectl config view -o jsonpath='{.clusters[0].cluster.server}'

# Method 2: From cluster-info
kubectl cluster-info | grep "control plane"
```

#### `--deploy-mode cluster`

This controls **where the driver runs**.

| Mode | Where Driver Runs | When to Use |
|------|-------------------|-------------|
| `cluster` | Inside Kubernetes (as a pod) | Production, CI/CD, scheduled jobs |
| `client` | On your local machine | Development, debugging |

We'll explore this deeply in the next section.

#### `--name spark-pi`

The application name. This becomes:
- Part of the pod names (e.g., `spark-pi-12345-driver`)
- Visible in Spark UI
- Appears in Kubernetes labels

#### `--class org.apache.spark.examples.SparkPi`

For **JVM applications** (Scala/Java), this is the main class.

```scala
// This is what the SparkPi example looks like:
package org.apache.spark.examples

object SparkPi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark Pi").getOrCreate()
    // ... calculation ...
    println(s"Pi is roughly $pi")
  }
}
```

For **Python**, you don't use `--class`, you specify the script path instead.

#### `--conf spark.kubernetes.container.image=...`

The Docker image that will be used for driver and executor pods.

**Important**: This image must:
- Have Spark installed in `/opt/spark`
- Have your application code (or access to it)
- Have all dependencies (JARs, Python packages)

#### `--conf spark.kubernetes.namespace=spark-jobs`

Which Kubernetes namespace to create pods in.

**Why namespaces matter:**
- Resource quotas apply per-namespace
- RBAC permissions are scoped to namespaces
- Easier to manage multiple teams/projects

#### `--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark`

Which Kubernetes ServiceAccount the driver pod should use.

**Why this matters:**
- The driver needs to create executor pods
- It needs `create`, `delete`, `get` permissions on pods
- This was set up in Part 4 (Security)

#### `local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar`

The path to your application JAR.

```
local:///opt/spark/examples/jars/spark-examples.jar
↑↑↑↑↑  ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
│      └── Path INSIDE the container
│
└── "local" means the file is inside the container
    Other options:
    • gs://bucket/path.jar (Google Cloud Storage)
    • s3a://bucket/path.jar (AWS S3)
    • hdfs://namenode/path.jar (HDFS)
```

#### `100`

Application argument. Passed to your main class/script.

---

## Deployment Modes Explained {#deployment-modes}

This is one of the most important concepts to understand deeply.

### What "Mode" Really Means

The **deploy mode** determines where the SparkSession/SparkContext actually runs.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        CLUSTER MODE                                     │
│                    (--deploy-mode cluster)                              │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  YOUR LAPTOP                      KUBERNETES CLUSTER                    │
│  ───────────                      ──────────────────                    │
│                                                                         │
│  ┌──────────────────┐            ┌─────────────────────────────────┐   │
│  │   spark-submit   │            │                                 │   │
│  │                  │            │  ┌────────────────────────────┐ │   │
│  │   1. Build spec  │  CREATE ──▶│  │      DRIVER POD            │ │   │
│  │   2. Submit pod  │            │  │                            │ │   │
│  │   3. Exit ✓      │            │  │  SparkContext lives here   │ │   │
│  │                  │            │  │  Your code runs here       │ │   │
│  └──────────────────┘            │  │  Collects results here     │ │   │
│                                  │  └──────────────┬─────────────┘ │   │
│  You can close your laptop!      │                 │               │   │
│  The job continues running.      │     ┌───────────┼───────────┐   │   │
│                                  │     ▼           ▼           ▼   │   │
│                                  │  ┌──────┐   ┌──────┐   ┌──────┐ │   │
│                                  │  │Exec 1│   │Exec 2│   │Exec 3│ │   │
│                                  │  └──────┘   └──────┘   └──────┘ │   │
│                                  │                                 │   │
│                                  └─────────────────────────────────┘   │
│                                                                         │
└────────────────────────────────────────────────────────────────────────┘


┌────────────────────────────────────────────────────────────────────────┐
│                         CLIENT MODE                                     │
│                     (--deploy-mode client)                              │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  YOUR LAPTOP                      KUBERNETES CLUSTER                    │
│  ───────────                      ──────────────────                    │
│                                                                         │
│  ┌──────────────────┐            ┌─────────────────────────────────┐   │
│  │   spark-submit   │            │                                 │   │
│  │                  │            │  No driver pod!                 │   │
│  │   DRIVER RUNS    │            │                                 │   │
│  │   HERE!          │            │  Only executor pods:            │   │
│  │                  │   CREATE ──▶│                                 │   │
│  │  SparkContext    │            │  ┌──────┐   ┌──────┐   ┌──────┐ │   │
│  │  lives here      │ ◀──────────│  │Exec 1│   │Exec 2│   │Exec 3│ │   │
│  │                  │  CONNECT   │  │      │   │      │   │      │ │   │
│  │  Results come    │            │  │  ──────────────────────────│ │   │
│  │  back here       │            │  │      MUST BE ABLE TO        │ │   │
│  │                  │            │  │   REACH YOUR LAPTOP!        │ │   │
│  └──────────────────┘            │  └──────────────────────────────┘│   │
│                                  │                                 │   │
│  You CANNOT close your laptop!   └─────────────────────────────────┘   │
│  Job dies if you disconnect.                                           │
│                                                                         │
└────────────────────────────────────────────────────────────────────────┘
```

### Client Mode: The Network Problem

In client mode, executors need to connect back to your driver (your laptop).

**This often fails because:**

1. Your laptop is behind a NAT/firewall
2. Executors can't resolve your hostname
3. Corporate VPN blocks incoming connections

**Error you'll see:**
```
WARN TaskSchedulerImpl: Initial job has not accepted any resources; 
check your cluster UI to ensure that workers are registered and have 
sufficient resources

ExecutorLostFailure: Unable to connect to driver at 192.168.1.100:45678
```

**When client mode works:**
- Running spark-submit from inside the cluster (in a pod)
- Using a VPN that makes your laptop routable
- Local development with port-forwarding

### When to Use Each Mode

| Scenario | Recommended Mode | Reason |
|----------|-----------------|--------|
| **Production batch jobs** | Cluster | Job survives if submitting machine goes down |
| **CI/CD pipelines** | Cluster | Jenkins/GitHub Actions can exit after submit |
| **Airflow/scheduler** | Cluster | Scheduler doesn't wait for job completion |
| **Interactive development** | Client | See logs immediately in terminal |
| **spark-shell / pyspark REPL** | Client | Interactive sessions require local driver |
| **Debugging** | Client | Can attach debugger to local JVM |

---

## Configuration Deep Dive {#configuration-deep-dive}

Let's understand every important configuration from first principles.

### How Configuration Works in Spark

Spark configuration comes from multiple sources, in this priority order (highest first):

1. **Programmatic**: `spark.conf.set("key", "value")` in your code
2. **spark-submit flags**: `--conf key=value`
3. **spark-defaults.conf**: File in `$SPARK_HOME/conf/`
4. **Environment variables**: `SPARK_*` variables
5. **Built-in defaults**: Hardcoded in Spark source

```bash
# Example: These are equivalent for the --master flag
spark-submit --master k8s://...
spark-submit --conf spark.master=k8s://...
```

### Connection Configuration

These control how Spark connects to Kubernetes:

```bash
# Required: Kubernetes API server URL
--conf spark.master=k8s://https://192.168.1.100:6443

# Required: Namespace to create pods in
--conf spark.kubernetes.namespace=spark-jobs

# Required: ServiceAccount with RBAC permissions
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark

# Optional: Different SA for executors (usually same as driver)
--conf spark.kubernetes.authenticate.executor.serviceAccountName=spark
```

**What happens if these are wrong:**

| Misconfiguration | Error You'll See |
|-----------------|------------------|
| Wrong master URL | `Connection refused` or `Unable to connect` |
| Wrong namespace | `Namespace not found` |
| Missing ServiceAccount | `ServiceAccount not found` |
| Wrong RBAC | `Forbidden: pods is forbidden` |

### Container Image Configuration

```bash
# Required: Docker image for all pods
--conf spark.kubernetes.container.image=gcr.io/my-project/spark:3.5.0

# Optional: Different images for driver/executor
--conf spark.kubernetes.driver.container.image=gcr.io/my-project/spark-driver:3.5.0
--conf spark.kubernetes.executor.container.image=gcr.io/my-project/spark-executor:3.5.0

# Optional: When to pull the image
--conf spark.kubernetes.container.image.pullPolicy=IfNotPresent
# Options: Always, IfNotPresent, Never

# Optional: Registry authentication (if private registry)
--conf spark.kubernetes.container.image.pullSecrets=my-registry-secret
```

**Pull Policy explanation:**

| Policy | When to Use |
|--------|-------------|
| `Always` | Every time - for `:latest` tags or frequent changes |
| `IfNotPresent` | Only if not cached - for versioned tags (recommended) |
| `Never` | Never pull - for pre-loaded images on nodes |

---

## Memory Management from First Principles {#memory-management}

Understanding memory is crucial for avoiding OOMKilled errors.

### The Memory Equation

When you set `spark.executor.memory=8g`, you're setting the **JVM heap size**.

But the Kubernetes pod needs **more than just heap memory**.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    EXECUTOR POD TOTAL MEMORY                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   K8s Memory Request = spark.executor.memory + spark.executor.memoryOverhead
│                      = JVM Heap              + Off-Heap Memory           │
│                                                                          │
│   ┌────────────────────────────────────────────────────────────────────┐│
│   │                    JVM HEAP: 8GB                                   ││
│   │              (spark.executor.memory=8g)                            ││
│   │                                                                    ││
│   │   ┌────────────────────────┐  ┌────────────────────────────────┐  ││
│   │   │   STORAGE MEMORY       │  │      EXECUTION MEMORY          │  ││
│   │   │       (~4GB)           │  │          (~4GB)                │  ││
│   │   │                        │  │                                │  ││
│   │   │ • Cached RDDs/DataFrames│ │ • Shuffles                     │  ││
│   │   │ • Broadcast variables   │ │ • Joins                        │  ││
│   │   │ • Unrolled data        │  │ • Sorts                        │  ││
│   │   │                        │  │ • Aggregations                 │  ││
│   │   └────────────────────────┘  └────────────────────────────────┘  ││
│   │                                                                    ││
│   │   Note: These can borrow from each other (unified memory)         ││
│   └────────────────────────────────────────────────────────────────────┘│
│                                                                          │
│   ┌────────────────────────────────────────────────────────────────────┐│
│   │                    OFF-HEAP / OVERHEAD: 2GB                        ││
│   │              (spark.executor.memoryOverhead=2g)                    ││
│   │                                                                    ││
│   │   • JVM metaspace (class metadata)                                ││
│   │   • JVM stack memory (per-thread)                                 ││
│   │   • Native libraries (compression, encryption)                    ││
│   │   • Python processes (if PySpark)     ← This is BIG for PySpark   ││
│   │   • Off-heap storage (if enabled)                                 ││
│   │   • Direct byte buffers                                           ││
│   └────────────────────────────────────────────────────────────────────┘│
│                                                                          │
│   TOTAL K8s REQUEST = 8GB + 2GB = 10GB                                  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### The Default Overhead Calculation

If you don't set `memoryOverhead`, Spark calculates it:

```
memoryOverhead = max(384MB, 0.10 × executor.memory)

Examples:
  executor.memory = 1g  → overhead = max(384MB, 100MB) = 384MB
  executor.memory = 4g  → overhead = max(384MB, 400MB) = 400MB
  executor.memory = 8g  → overhead = max(384MB, 800MB) = 800MB
  executor.memory = 16g → overhead = max(384MB, 1.6GB) = 1.6GB
```

### Why PySpark Needs More Overhead

When you run PySpark, each executor runs:
- A JVM process (the executor)
- A Python daemon process
- Python worker processes (one per task)

```
┌────────────────────────────────────────────────────────────────────┐
│                    PYSPARK EXECUTOR POD                             │
├────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌─────────────────────────────────────────────────────────────┐  │
│   │                JVM EXECUTOR PROCESS                          │  │
│   │                                                              │  │
│   │   • Receives tasks from driver                              │  │
│   │   • Reads data from storage                                 │  │
│   │   • Sends data to Python via socket                         │  │
│   │   • Collects results from Python                            │  │
│   │                                                              │  │
│   │   Memory: spark.executor.memory                             │  │
│   └─────────────────────────────────────────────────────────────┘  │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────────┐  │
│   │              PYTHON WORKER PROCESSES                         │  │
│   │                                                              │  │
│   │   • Run your Python UDFs                                    │  │
│   │   • pandas operations                                       │  │
│   │   • ML model inference                                      │  │
│   │                                                              │  │
│   │   Memory: Counted in memoryOverhead!                        │  │
│   │                                                              │  │
│   │   This is why PySpark often needs 20-30% overhead           │  │
│   └─────────────────────────────────────────────────────────────┘  │
│                                                                     │
└────────────────────────────────────────────────────────────────────┘
```

**PySpark memory recommendations:**

| Workload Type | Recommended Overhead |
|---------------|---------------------|
| Pure Spark SQL | 10% (default) |
| PySpark with UDFs | 20-30% |
| pandas UDFs | 30-40% |
| ML with large models | 40-50% |

### Complete Memory Configuration

```bash
spark-submit \
  # Driver memory
  --conf spark.driver.memory=4g \
  --conf spark.driver.memoryOverhead=1g \
  
  # Executor memory
  --conf spark.executor.memory=8g \
  --conf spark.executor.memoryOverhead=2g \
  
  # Optional: Unified memory fraction (default 0.6 = 60%)
  # This is the fraction of heap for storage + execution
  --conf spark.memory.fraction=0.6 \
  
  # Optional: Storage vs Execution split within fraction
  # (default 0.5 means 50/50 split, can borrow from each other)
  --conf spark.memory.storageFraction=0.5 \
  ...
```

---

## CPU and Parallelism {#cpu-parallelism}

### Understanding Cores in Spark

The word "cores" means different things in different contexts:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CORES: What Does It Mean?                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   spark.executor.cores = 4                                              │
│   ──────────────────────                                                │
│                                                                          │
│   This is NOT about physical CPU cores!                                 │
│   This is the number of CONCURRENT TASKS per executor.                  │
│                                                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    EXECUTOR (4 cores)                             │  │
│   │                                                                   │  │
│   │   ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │  │
│   │   │  Task 1  │ │  Task 2  │ │  Task 3  │ │  Task 4  │           │  │
│   │   │  Slot 1  │ │  Slot 2  │ │  Slot 3  │ │  Slot 4  │           │  │
│   │   └──────────┘ └──────────┘ └──────────┘ └──────────┘           │  │
│   │                                                                   │  │
│   │   These 4 tasks run in parallel within this executor             │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│   spark.kubernetes.executor.request.cores = 4                           │
│   ─────────────────────────────────────────────                         │
│                                                                          │
│   This IS about Kubernetes CPU resources!                               │
│   This is the CPU request for the pod.                                  │
│                                                                          │
│   Kubernetes will guarantee this much CPU capacity.                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Best Practice: Match Them

```bash
# For predictable performance, match Spark cores to K8s CPU request
--conf spark.executor.cores=4 \
--conf spark.kubernetes.executor.request.cores=4 \
--conf spark.kubernetes.executor.limit.cores=4 \
```

### Total Parallelism

Your total parallelism = executors × cores per executor

```
Example:
  10 executors × 4 cores each = 40 concurrent tasks

If your data has 200 partitions:
  200 partitions ÷ 40 slots = 5 waves of tasks
```

---

## Node Selection and Scheduling {#node-selection}

Control which nodes your pods run on.

### Node Selectors (Simple)

```bash
# Run executors only on nodes with specific labels
--conf spark.kubernetes.executor.node.selector.node-type=spark-executor
--conf spark.kubernetes.executor.node.selector.disk-type=ssd

# Run driver on different node type
--conf spark.kubernetes.driver.node.selector.node-type=spark-driver
```

This requires nodes to have matching labels:
```bash
# Label your nodes
kubectl label nodes worker-1 node-type=spark-executor disk-type=ssd
```

### Why Separate Node Types?

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    RECOMMENDED NODE POOL STRATEGY                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   NODE POOL: "driver"                                                   │
│   ────────────────────                                                  │
│   • Small, reliable instances (e.g., e2-standard-4)                    │
│   • On-demand (not Spot/Preemptible)                                   │
│   • Driver pods need stability                                         │
│                                                                          │
│   NODE POOL: "executor"                                                 │
│   ─────────────────────                                                 │
│   • Large, compute-optimized instances (e.g., c2-standard-30)          │
│   • Spot/Preemptible (70-90% cheaper!)                                 │
│   • Executors can be replaced if preempted                             │
│                                                                          │
│   NODE POOL: "system"                                                   │
│   ────────────────────                                                  │
│   • Runs kube-system pods, monitoring, etc.                            │
│   • Small instances                                                     │
│   • On-demand                                                           │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Dynamic Allocation {#dynamic-allocation}

### The Problem It Solves

Static executor count is wasteful:

```
Scenario: Job has 2 stages
  Stage 1: Heavy shuffle, needs 50 executors
  Stage 2: Light aggregation, needs 5 executors

With static allocation (50 executors):
  Stage 2 runs with 50 executors, 45 are idle = WASTED RESOURCES

With dynamic allocation:
  Stage 1: Scales up to 50 executors
  Stage 2: Scales down to 5 executors = COST SAVINGS
```

### How It Works

```
┌─────────────────────────────────────────────────────────────────────────┐
│                  DYNAMIC ALLOCATION LIFECYCLE                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Time 0s: Job starts                                                   │
│   ─────────────────────                                                 │
│   • initialExecutors = 5 (start with 5)                                │
│   • Pending tasks: 1000                                                │
│                                                                          │
│   ┌─────────────────────────────────────┐                              │
│   │  ■ ■ ■ ■ ■   (5 executors)          │  ← Not enough!               │
│   └─────────────────────────────────────┘                              │
│                                                                          │
│   Time 5s: schedulerBacklogTimeout triggers                            │
│   ──────────────────────────────────────────                           │
│   • Pending tasks have waited 5 seconds                                │
│   • Spark requests more executors                                      │
│                                                                          │
│   ┌─────────────────────────────────────────────────────────┐          │
│   │  ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ■   (20 executors)│          │
│   └─────────────────────────────────────────────────────────┘          │
│                                                                          │
│   Time 30s: Still not enough, continues scaling                        │
│   ───────────────────────────────────────────                          │
│   • maxExecutors = 50 (won't go above this)                            │
│                                                                          │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │  ■ ■ ■ ■ ■ ■ ■ ■ ■ ■ ... (50 executors, MAX!)                  │   │
│   └────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│   Time 60s: All tasks complete, some executors idle                    │
│   ───────────────────────────────────────────────                      │
│   • executorIdleTimeout = 60s                                          │
│   • Idle executors start countdown                                     │
│                                                                          │
│   Time 120s: Idle timeout reached                                      │
│   ─────────────────────────────                                        │
│   • Idle executors removed                                             │
│   • minExecutors = 2 (never go below this)                             │
│                                                                          │
│   ┌─────────────────────────────────────┐                              │
│   │  ■ ■   (2 executors, MIN!)          │  ← Ready for next stage      │
│   └─────────────────────────────────────┘                              │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Configuration

```bash
spark-submit \
  # Enable dynamic allocation
  --conf spark.dynamicAllocation.enabled=true \
  
  # REQUIRED for Kubernetes (tracks shuffle files without external service)
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  
  # Limits
  --conf spark.dynamicAllocation.minExecutors=2 \
  --conf spark.dynamicAllocation.maxExecutors=50 \
  --conf spark.dynamicAllocation.initialExecutors=5 \
  
  # Scale-up sensitivity (how long to wait before requesting more)
  --conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
  
  # Scale-down sensitivity
  --conf spark.dynamicAllocation.executorIdleTimeout=60s \
  --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=180s \
  ...
```

### When NOT to Use

| Scenario | Why Avoid Dynamic Allocation |
|----------|------------------------------|
| Streaming jobs | Executors shouldn't be removed mid-stream |
| Very short jobs (<1 min) | Scaling overhead not worth it |
| Predictable, steady workloads | Just set the right static count |

---

## Pod Templates {#pod-templates}

For advanced customization beyond spark-submit flags.

### When You Need Them

- Security contexts (runAsNonRoot, readOnlyFilesystem)
- Volume mounts (for shuffle storage, configs)
- Tolerations (for tainted nodes like Spot instances)
- Init containers
- Sidecar containers (logging, monitoring)
- Advanced affinity/anti-affinity

### Example: Hardened Executor Template

```yaml
# executor-template.yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: spark
    team: data-platform
spec:
  # Security hardening
  securityContext:
    runAsUser: 185      # spark user
    runAsGroup: 185
    fsGroup: 185
  
  containers:
    - name: spark-kubernetes-executor  # Name must match!
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop:
            - ALL
      
      # Volume mounts for shuffle
      volumeMounts:
        - name: spark-local
          mountPath: /data/spark-local
  
  # Volumes
  volumes:
    - name: spark-local
      emptyDir:
        sizeLimit: 100Gi
  
  # Tolerations for Spot nodes
  tolerations:
    - key: "cloud.google.com/gke-preemptible"
      operator: "Equal"
      value: "true"
      effect: "NoSchedule"
  
  # Node selection
  nodeSelector:
    node-type: spark-executor
```

### Using the Template

```bash
spark-submit \
  --conf spark.kubernetes.executor.podTemplateFile=/path/to/executor-template.yaml \
  --conf spark.kubernetes.driver.podTemplateFile=/path/to/driver-template.yaml \
  ...
```

---

## PySpark Considerations {#pyspark}

### Basic PySpark Submit

```bash
spark-submit \
  --master k8s://https://$(kubectl config view -o jsonpath='{.clusters[0].cluster.server}') \
  --deploy-mode cluster \
  --name my-pyspark-job \
  \
  # Image with Python installed
  --conf spark.kubernetes.container.image=gcr.io/my-project/spark-py:3.5.0 \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  \
  # Point to Python executable
  --conf spark.pyspark.python=/usr/bin/python3 \
  --conf spark.pyspark.driver.python=/usr/bin/python3 \
  \
  # Extra memory for Python processes
  --conf spark.executor.memoryOverhead=2g \
  \
  local:///opt/spark/work-dir/my_job.py \
  --input gs://bucket/data
```

### Dependency Management

**Best: Bake into image** (recommended for production)

```dockerfile
FROM apache/spark-py:3.5.0

# Install Python dependencies
COPY requirements.txt /opt/spark/work-dir/
RUN pip install --no-cache-dir -r /opt/spark/work-dir/requirements.txt

# Copy your code
COPY my_package/ /opt/spark/work-dir/my_package/
```

---

## Debugging Methodology {#debugging}

A systematic approach to troubleshooting.

### Step 1: Check Pod Status

```bash
# Watch pods being created
kubectl get pods -n spark-jobs -l spark-role -w

# Common statuses and their meanings:
# Pending     → Waiting to be scheduled
# ContainerCreating → Pulling image or setting up volumes
# Running     → Container is running
# Completed   → Finished successfully
# Error       → Container exited with error
# OOMKilled   → Out of memory
# CrashLoopBackOff → Keeps crashing and restarting
```

### Step 2: Get Pod Events

```bash
kubectl describe pod -n spark-jobs <pod-name>

# Look at the Events section at the bottom
# This tells you WHY pods are in a certain state
```

### Step 3: Get Logs

```bash
# Driver logs (your application output)
kubectl logs -n spark-jobs <driver-pod-name>

# Follow logs in real-time
kubectl logs -n spark-jobs <driver-pod-name> -f

# If pod has restarted, get previous logs
kubectl logs -n spark-jobs <pod-name> --previous
```

### Common Errors Reference

| Error | Likely Cause | Solution |
|-------|--------------|----------|
| `Forbidden: pods is forbidden` | RBAC misconfigured | Check ServiceAccount and RoleBinding |
| `ImagePullBackOff` | Can't pull image | Check image name, registry auth |
| `Pending` (stuck) | No schedulable nodes | Check resources, selectors, tolerations |
| `OOMKilled` | Out of memory | Increase memoryOverhead |
| `CrashLoopBackOff` | Application crashing | Check logs for root cause |
| `Connection refused` | Network issue | Check cluster mode vs client mode |

---

## Summary

You now understand:

✅ **What spark-submit is** — Your interface to run Spark on any cluster  
✅ **Spark application anatomy** — Driver, Executors, Cluster Manager  
✅ **Kubernetes integration** — How spark-submit creates pods  
✅ **Deployment modes** — Cluster for production, Client for development  
✅ **Memory management** — Heap + Overhead = K8s request  
✅ **CPU/parallelism** — Tasks, cores, and throughput  
✅ **Dynamic allocation** — Automatic scaling  
✅ **Pod templates** — Advanced customization  
✅ **Debugging** — Systematic troubleshooting approach  

---

## What's Next

In **Part 6: Spark Operator**, we'll cover:
- Declarative SparkApplication CRDs
- ScheduledSparkApplication for cron-like jobs
- GitOps integration with ArgoCD
- Why operators are better for production

---

*Next: [Part 6: Spark Operator →](./article-06-spark-operator.md)*

*Previous: [Part 4: Security Deep Dive ←](./article-04-security.md)*
