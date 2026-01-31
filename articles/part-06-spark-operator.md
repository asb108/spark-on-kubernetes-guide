# Part 6: Spark Operator — Declarative Spark on Kubernetes

**Mastering Spark on Kubernetes Series**

*In Part 5, you learned spark-submit — the imperative way to run Spark jobs. This part introduces the Spark Operator, a Kubernetes-native way to manage Spark jobs. We'll start from the very basics: what is an operator, why does it exist, and how does it change everything.*

---

## What You'll Learn

By the end of this guide, you'll understand:

- ✅ What "imperative" and "declarative" actually mean
- ✅ What Kubernetes operators are and why they exist
- ✅ How the Spark Operator works, step by step
- ✅ The SparkApplication Custom Resource — every field explained
- ✅ How to install and configure the operator
- ✅ Scheduled jobs with ScheduledSparkApplication
- ✅ Retry policies and failure handling
- ✅ Debugging operator-managed Spark jobs

**Reading Time:** 60 minutes  
**Hands-On Time:** 45 minutes  
**Prerequisites:** [Part 5: spark-submit Mastery](./article-05-spark-submit.md)  
**Visual Reference:** [View Diagrams & Tables (Interactive)](./diagrams-part6.html)

---

## Table of Contents

1. [The Problem with spark-submit](#the-problem)
2. [Imperative vs Declarative — A Fundamental Concept](#imperative-vs-declarative)
3. [What is a Kubernetes Operator?](#what-is-operator)
4. [The Spark Operator — How It Works](#how-it-works)
5. [Installing the Spark Operator](#installation)
6. [Your First SparkApplication](#first-application)
7. [SparkApplication Deep Dive — Every Field Explained](#sparkapplication-deep-dive)
8. [ScheduledSparkApplication — Cron Jobs for Spark](#scheduled-spark)
9. [Restart Policies and Failure Handling](#restart-policies)
10. [Debugging Operator-Managed Jobs](#debugging)
11. [When to Use spark-submit vs Operator](#when-to-use)

---

## The Problem with spark-submit {#the-problem}

In Part 5, you learned spark-submit. It works, but it has problems in production.

### Scenario: Running a Daily ETL Job

You have a Spark job that runs every day at 2 AM:

```bash
spark-submit \
  --master k8s://https://cluster-api:6443 \
  --deploy-mode cluster \
  --conf spark.executor.instances=10 \
  --conf spark.kubernetes.container.image=myrepo/spark:3.5 \
  local:///opt/spark/jobs/daily_etl.py
```

**Question 1: How do you run this every day?**

You need something external — a cron job, Airflow, Jenkins. spark-submit is just a one-time command.

**Question 2: What if it fails?**

spark-submit runs once and exits. If the job fails, nothing happens. You have to manually detect the failure and re-run.

**Question 3: How do you know the current status?**

After running spark-submit, the command exits. To see status, you have to:
- Check if the driver pod exists
- Read its logs
- Check if it succeeded or failed

There's no single place showing "job X is RUNNING" or "job Y FAILED".

**Question 4: How do you version control this?**

The spark-submit command is... a command. You can put it in a script, but:
- Changes aren't tracked
- No review process
- Hard to see history

### The Root Problem

spark-submit is **imperative** — you tell the system "do this action right now."

What if you could instead say "I want this job to exist and run with these specifications"? The system would figure out how to make it happen.

This is the **declarative** approach, and it's what the Spark Operator provides.

---

## Imperative vs Declarative — A Fundamental Concept {#imperative-vs-declarative}

This concept is fundamental to Kubernetes and explains why operators exist.

### What is Imperative?

**Imperative** means giving step-by-step commands:

```
YOU: "Create a pod named my-app with image nginx"
YOU: "If the pod crashes, create it again"
YOU: "When load is high, create more pods"
YOU: "When load is low, delete some pods"
```

You're telling the system **what to do** at each moment.

### What is Declarative?

**Declarative** means describing the desired end state:

```yaml
# YOU: "I want this to exist"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 3
  template:
    spec:
      containers:
        - name: app
          image: nginx
```

You're telling the system **what you want**, not how to achieve it.

The system (Kubernetes) figures out:
- How to create the pods
- How to restart them if they crash
- How to scale up/down
- How to roll out updates

### Real-World Analogy

**Imperative (Restaurant):**
```
"Take the chicken from the fridge"
"Cut it into pieces"
"Heat oil in pan"
"Fry for 5 minutes on each side"
"Add salt, pepper, garlic"
"Serve on white plate"
```

**Declarative (Restaurant):**
```
"I want grilled chicken, medium spicy, with rice"
```

The chef (system) knows how to make it happen. You describe the end result.

### Why Declarative is Better for Infrastructure

```
┌─────────────────────────────────────────────────────────────────────────┐
│              IMPERATIVE vs DECLARATIVE COMPARISON                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   IMPERATIVE (spark-submit)                                             │
│   ─────────────────────────                                             │
│                                                                          │
│   Timeline:                                                             │
│   ──────────────────────────────────────────────────────►               │
│   │                   │                    │                            │
│   Run command         Job fails            You notice,                  │
│   (2:00 AM)          (2:45 AM)            re-run manually              │
│                                            (next morning)               │
│                                                                          │
│   Problems:                                                             │
│   • No automatic retry                                                  │
│   • No visibility of status                                            │
│   • No audit trail                                                      │
│   • Hard to version control                                            │
│                                                                          │
│   ─────────────────────────────────────────────────────────────────────  │
│                                                                          │
│   DECLARATIVE (Spark Operator)                                          │
│   ────────────────────────────                                          │
│                                                                          │
│   Timeline:                                                             │
│   ──────────────────────────────────────────────────────►               │
│   │                   │         │          │                            │
│   Apply YAML          Job fails Operator   Job succeeds                 │
│   (2:00 AM)          (2:45 AM) auto-retries                            │
│                                (2:46 AM)                                │
│                                                                          │
│   Benefits:                                                             │
│   • Automatic retry (configurable)                                      │
│   • Status visible: kubectl get sparkapplication                        │
│   • Audit trail: YAML in git + K8s events                              │
│   • Version controlled: the YAML file IS the configuration             │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### The Key Insight

With declarative, you describe your intent once, and the system continuously works to maintain that state:

- If the job fails → retry it
- If the driver crashes → record the failure
- If you want to change something → update the YAML, apply it

This is the philosophy that all of Kubernetes is built on.

---

## What is a Kubernetes Operator? {#what-is-operator}

Now that you understand declarative, let's understand operators.

### Kubernetes Already Has Controllers

Kubernetes has built-in controllers that manage common resources:

| Resource | Controller | What It Does |
|----------|-----------|--------------|
| Deployment | Deployment Controller | Ensures N replicas are running |
| Service | Endpoints Controller | Routes traffic to pods |
| Job | Job Controller | Runs a pod to completion |
| CronJob | CronJob Controller | Runs jobs on schedule |

These controllers follow the same pattern:
1. **Watch** for changes to their resource type
2. **Compare** desired state vs actual state
3. **Act** to make actual state match desired state

### The Limitation

But what about Spark? Kubernetes doesn't know about:
- Spark drivers and executors
- How they communicate
- What "job succeeded" means for Spark
- How to read Spark's application ID

You could use a Kubernetes Job, but it only knows "pod finished" — not "Spark job finished."

### Custom Resource Definitions (CRDs)

Kubernetes allows you to extend its API with Custom Resource Definitions:

```yaml
# This tells Kubernetes: "There's a new type of thing called SparkApplication"
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: sparkapplications.sparkoperator.k8s.io
spec:
  group: sparkoperator.k8s.io
  names:
    kind: SparkApplication
    plural: sparkapplications
    singular: sparkapplication
    shortNames:
      - sparkapp
  scope: Namespaced
  versions:
    - name: v1beta2
      served: true
      storage: true
```

After applying this, you can do:
```bash
kubectl get sparkapplications
kubectl describe sparkapplication my-job
```

But the CRD is just a definition. It doesn't DO anything.

### The Operator = CRD + Controller

An **Operator** is:
1. A **Custom Resource Definition (CRD)** — extends the API
2. A **Controller** — watches for CRD instances and takes action

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     THE OPERATOR PATTERN                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    CUSTOM RESOURCE DEFINITION                     │  │
│   │                                                                   │  │
│   │   "Kind: SparkApplication exists as a resource type"             │  │
│   │                                                                   │  │
│   │   You can now: kubectl get sparkapplications                      │  │
│   │                 kubectl apply -f spark-job.yaml                   │  │
│   │                 kubectl describe sparkapplication my-job          │  │
│   │                                                                   │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                              │                                           │
│                              ▼                                           │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                       CONTROLLER (the Operator)                   │  │
│   │                                                                   │  │
│   │   A pod that:                                                     │  │
│   │                                                                   │  │
│   │   1. WATCHES SparkApplication resources                          │  │
│   │      "Tell me when someone creates/updates/deletes one"          │  │
│   │                                                                   │  │
│   │   2. UNDERSTANDS Spark                                           │  │
│   │      "A Spark job needs a driver pod + executor pods"            │  │
│   │                                                                   │  │
│   │   3. TAKES ACTION                                                 │  │
│   │      "Create driver pod with these specs"                        │  │
│   │      "Update status to RUNNING"                                  │  │
│   │      "If job fails, maybe retry"                                 │  │
│   │                                                                   │  │
│   │   4. UPDATES STATUS                                               │  │
│   │      "SparkApplication.status.applicationState = COMPLETED"      │  │
│   │                                                                   │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│   The key insight: the operator contains DOMAIN KNOWLEDGE about Spark.  │
│   Kubernetes doesn't know Spark. The operator does.                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Why Call it an "Operator"?

The name comes from "human operator" — a person who knows how to run a complex system.

An SRE (Site Reliability Engineer) operating a Spark cluster knows:
- How to submit jobs
- How to monitor them
- How to restart on failure
- How to tune resources

An **operator** encodes this operational knowledge into software.

---

## The Spark Operator — How It Works {#how-it-works}

Let's trace through exactly what happens when you use the Spark Operator.

### Step 0: Operator is Running

Before anything, the Spark Operator controller is running as a pod in your cluster:

```bash
kubectl get pods -n spark-operator
# NAME                                        READY   STATUS    AGE
# spark-operator-controller-7b9c8d9f4-x2k9j   1/1     Running   5d
```

This pod is constantly watching for SparkApplication resources.

### Step 1: You Apply a SparkApplication

```bash
kubectl apply -f my-spark-job.yaml
```

Where `my-spark-job.yaml` contains:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: my-spark-job
  namespace: spark-jobs
spec:
  type: Python
  mode: cluster
  image: myrepo/spark:3.5.0
  mainApplicationFile: local:///opt/spark/work-dir/my_job.py
  driver:
    cores: 2
    memory: "4g"
    serviceAccount: spark
  executor:
    cores: 4
    instances: 10
    memory: "8g"
```

### Step 2: Kubernetes API Stores the Resource

The Kubernetes API server stores this SparkApplication in etcd (Kubernetes's database).

### Step 3: Operator Gets Notified

The operator was watching SparkApplication resources. It receives a notification: "New SparkApplication created!"

### Step 4: Operator Reads the Spec

```python
# Pseudocode of what the operator does internally
spark_app = get_spark_application("my-spark-job", "spark-jobs")

# Extract configuration
image = spark_app.spec.image  # "myrepo/spark:3.5.0"
driver_cores = spark_app.spec.driver.cores  # 2
driver_memory = spark_app.spec.driver.memory  # "4g"
executor_instances = spark_app.spec.executor.instances  # 10
# ... etc
```

### Step 5: Operator Creates Driver Pod

The operator constructs a Pod spec for the driver:

```yaml
# Operator programmatically creates this (simplified)
apiVersion: v1
kind: Pod
metadata:
  name: my-spark-job-driver
  namespace: spark-jobs
  labels:
    spark-role: driver
    spark-app-name: my-spark-job
spec:
  containers:
    - name: driver
      image: myrepo/spark:3.5.0
      args:
        - driver
        - "--conf spark.driver.cores=2"
        - "--conf spark.executor.instances=10"
        - "local:///opt/spark/work-dir/my_job.py"
  serviceAccountName: spark
```

The operator calls the Kubernetes API to create this pod.

### Step 6: Operator Updates SparkApplication Status

```bash
kubectl get sparkapplication my-spark-job -n spark-jobs
# NAME           STATUS      ATTEMPTS   AGE
# my-spark-job   SUBMITTED   1          5s
```

The operator wrote to `sparkapplication.status.applicationState.state = "SUBMITTED"`.

### Step 7: Driver Starts and Creates Executors

The driver pod starts running. Using the ServiceAccount, the driver creates executor pods (just like with spark-submit).

### Step 8: Operator Monitors Progress

The operator continues watching:
- Is the driver still running?
- Did the driver exit?
- What's the exit code?

It updates the status accordingly:

```bash
kubectl get sparkapplication my-spark-job -n spark-jobs
# NAME           STATUS    ATTEMPTS   AGE
# my-spark-job   RUNNING   1          30s
```

### Step 9: Job Completes (or Fails)

When the driver pod terminates:
- Exit code 0 → status becomes COMPLETED
- Exit code non-zero → status becomes FAILED

```bash
kubectl get sparkapplication my-spark-job -n spark-jobs
# NAME           STATUS      ATTEMPTS   AGE
# my-spark-job   COMPLETED   1          5m
```

### Step 10: Cleanup

Based on configuration:
- Executor pods are deleted (by the driver, before it exits)
- Driver pod may be kept or deleted (configurable)
- SparkApplication resource remains for you to inspect

### The Complete Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SPARK OPERATOR FLOW                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   1. YOU                                                                │
│      │                                                                  │
│      │ kubectl apply -f my-spark-job.yaml                               │
│      ▼                                                                  │
│   2. KUBERNETES API SERVER                                              │
│      │                                                                  │
│      │ Stores SparkApplication in etcd                                  │
│      │ Notifies watchers                                                │
│      ▼                                                                  │
│   3. SPARK OPERATOR (controller pod)                                    │
│      │                                                                  │
│      │ "I see a new SparkApplication!"                                  │
│      │ Reads spec: image, resources, code location                      │
│      │ Updates status: SUBMITTED                                        │
│      │ Creates driver pod                                               │
│      ▼                                                                  │
│   4. DRIVER POD                                                         │
│      │                                                                  │
│      │ Scheduled to a node                                              │
│      │ Initializes SparkContext                                         │
│      │                                                                  │
│      │────── Updates status: RUNNING                                    │
│      │                                                                  │
│      │ Creates executor pods                                            │
│      ▼                                                                  │
│   5. EXECUTOR PODS                                                      │
│      │                                                                  │
│      │ Register with driver                                             │
│      │ Receive and execute tasks                                        │
│      │ Return results to driver                                         │
│      ▼                                                                  │
│   6. JOB COMPLETES                                                      │
│      │                                                                  │
│      │ Driver exits with code 0 (success) or non-zero (failure)         │
│      │                                                                  │
│      ▼                                                                  │
│   7. SPARK OPERATOR                                                     │
│      │                                                                  │
│      │ Notices driver pod terminated                                    │
│      │ Reads exit code                                                  │
│      │ Updates status: COMPLETED or FAILED                              │
│      │                                                                  │
│      │ If FAILED and retries configured:                                │
│      │   Creates new driver pod                                         │
│      │   Increments attempt counter                                     │
│      ▼                                                                  │
│   8. STATUS VISIBLE                                                     │
│                                                                          │
│      kubectl get sparkapplication my-spark-job                          │
│      NAME           STATUS      ATTEMPTS   AGE                          │
│      my-spark-job   COMPLETED   1          5m                           │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Installing the Spark Operator {#installation}

### Prerequisites

Before installing, you need:

1. **A Kubernetes cluster** — KinD, GKE, or any cluster (Part 2)
2. **helm** — The Kubernetes package manager
3. **kubectl** — Configured to talk to your cluster

### Step 1: Add the Helm Repository

Helm uses "repositories" to find charts (packages). Add the Spark Operator repo:

```bash
# Add the repository
helm repo add spark-operator https://kubeflow.github.io/spark-operator

# Update to get latest chart versions
helm repo update

# Verify it was added
helm search repo spark-operator
# NAME                                    CHART VERSION   APP VERSION
# spark-operator/spark-operator           2.0.0           v2.0.0
```

### Step 2: Create Namespaces

Best practice: separate the operator from your Spark jobs.

```bash
# Namespace for the operator itself
kubectl create namespace spark-operator

# Namespace for your Spark jobs
kubectl create namespace spark-jobs
```

**Why separate?**
- The operator needs its own ServiceAccount and RBAC
- Jobs can have different RBAC (less privileged)
- Easier to manage quotas and limits
- Clear separation of concerns

### Step 3: Install the Operator

```bash
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --set webhook.enable=true \
  --set sparkJobNamespace=spark-jobs
```

**Breaking down the options:**

| Option | Meaning |
|--------|---------|
| `spark-operator` (first) | Release name — what to call this installation |
| `spark-operator/spark-operator` | Chart name — what to install |
| `--namespace spark-operator` | Install operator INTO this namespace |
| `--set webhook.enable=true` | Enable validation webhook (catches errors early) |
| `--set sparkJobNamespace=spark-jobs` | Which namespace to watch for SparkApplications |

### Step 4: Verify Installation

```bash
# Check the operator pod is running
kubectl get pods -n spark-operator
# NAME                                            READY   STATUS    AGE
# spark-operator-controller-xxxxxxxxxx-xxxxx      1/1     Running   30s

# Check the CRDs were installed
kubectl get crds | grep spark
# sparkapplications.sparkoperator.k8s.io               2024-01-15T10:30:00Z
# scheduledsparkapplications.sparkoperator.k8s.io      2024-01-15T10:30:00Z

# Check you can list SparkApplications (should be empty)
kubectl get sparkapplications -n spark-jobs
# No resources found in spark-jobs namespace.
```

### Step 5: Create ServiceAccount for Spark Jobs

The operator creates driver pods, but those pods need permission to create executor pods:

```yaml
# spark-rbac.yaml
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
    verbs: ["get", "list", "watch", "create", "delete"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["get", "list", "watch", "create", "delete"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "list", "watch", "create", "delete"]
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

Apply it:
```bash
kubectl apply -f spark-rbac.yaml
```

Now you're ready to run Spark jobs!

---

## Your First SparkApplication {#first-application}

Let's run the classic SparkPi example using the operator.

### The YAML File

Create `spark-pi.yaml`:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-pi
  namespace: spark-jobs
spec:
  type: Scala
  mode: cluster
  image: apache/spark:3.5.0
  imagePullPolicy: IfNotPresent
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar
  arguments:
    - "1000"
  sparkVersion: "3.5.0"
  driver:
    cores: 1
    memory: "512m"
    serviceAccount: spark
  executor:
    cores: 1
    instances: 2
    memory: "512m"
  restartPolicy:
    type: Never
```

### Apply and Watch

```bash
# Submit the job
kubectl apply -f spark-pi.yaml

# Watch status changes
kubectl get sparkapplication spark-pi -n spark-jobs -w
```

You'll see:
```
NAME       STATUS       ATTEMPTS   AGE
spark-pi   SUBMITTED    1          0s
spark-pi   RUNNING      1          15s
spark-pi   COMPLETED    1          45s
```

### Check the Result

```bash
# Get driver logs
kubectl logs -n spark-jobs spark-pi-driver | tail -5
# Pi is roughly 3.141592653589793
```

### Delete When Done

```bash
kubectl delete sparkapplication spark-pi -n spark-jobs
```

This also cleans up the driver pod.

---

## SparkApplication Deep Dive — Every Field Explained {#sparkapplication-deep-dive}

Let's go through every important field in detail.

### The Complete Structure

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: my-etl-job             # Unique name for this job
  namespace: spark-jobs         # Where to create this resource
  labels:                       # Optional: for selecting/filtering
    team: data-platform
    environment: production
spec:
  # ─── WHAT LANGUAGE ───
  type: Python                  # Scala, Java, Python, or R
  pythonVersion: "3"            # For Python: major version
  
  # ─── DEPLOYMENT MODE ───
  mode: cluster                 # Always "cluster" for Kubernetes
  
  # ─── CONTAINER IMAGE ───
  image: myrepo/spark:3.5.0     # Docker image containing Spark + your code
  imagePullPolicy: IfNotPresent # Always, IfNotPresent, Never
  imagePullSecrets:             # For private registries
    - name: my-registry-secret
  
  # ─── YOUR CODE ───
  mainApplicationFile: local:///opt/spark/work-dir/my_job.py
  mainClass: ""                 # Only for Scala/Java
  arguments:                    # Arguments passed to your code
    - "--input"
    - "gs://my-bucket/data/input/"
    - "--output"
    - "gs://my-bucket/data/output/"
  
  # ─── DEPENDENCIES ───
  deps:
    jars:                       # Additional JAR files
      - "gs://my-bucket/jars/gcs-connector.jar"
    pyFiles:                    # Python packages (.zip, .py)
      - "gs://my-bucket/python/my_utils.zip"
    files:                      # Files to distribute to all executors
      - "gs://my-bucket/config/app.conf"
  
  # ─── SPARK CONFIGURATION ───
  sparkConf:
    spark.sql.shuffle.partitions: "200"
    spark.dynamicAllocation.enabled: "true"
    spark.dynamicAllocation.minExecutors: "2"
    spark.dynamicAllocation.maxExecutors: "50"
  
  # ─── DRIVER POD ───
  driver:
    cores: 2                    # CPU cores for driver
    coreLimit: "2"              # K8s CPU limit
    memory: "4g"                # JVM heap
    memoryOverhead: "1g"        # Off-heap (containers, Python)
    serviceAccount: spark       # RBAC ServiceAccount
    labels:
      role: driver
    envVars:
      APP_ENV: "production"
    volumeMounts:
      - name: config-volume
        mountPath: /etc/config
  
  # ─── EXECUTOR PODS ───
  executor:
    cores: 4                    # CPU cores per executor
    coreLimit: "4"              # K8s CPU limit
    instances: 10               # Number of executors
    memory: "8g"                # JVM heap per executor
    memoryOverhead: "2g"        # Off-heap
    labels:
      role: executor
    envVars:
      APP_ENV: "production"
  
  # ─── VOLUMES ───
  volumes:
    - name: config-volume
      configMap:
        name: my-config
  
  # ─── RESTART POLICY ───
  restartPolicy:
    type: OnFailure             # Never, OnFailure, Always
    onFailureRetries: 3
    onFailureRetryInterval: 60  # seconds
    onSubmissionFailureRetries: 3
    onSubmissionFailureRetryInterval: 30
  
  # ─── TIMEOUTS ───
  timeToLiveSeconds: 86400      # Delete completed job after 24 hours
```

### Key Fields Explained in Detail

#### type

What language is your code written in?

| Value | Meaning | Main File Extension |
|-------|---------|---------------------|
| `Scala` | Scala (or Java) in a JAR | `.jar` |
| `Java` | Java in a JAR | `.jar` |
| `Python` | Python script | `.py` |
| `R` | R script | `.R` |

#### mainApplicationFile

Where is your code?

```yaml
# If code is baked into the Docker image:
mainApplicationFile: local:///opt/spark/work-dir/my_job.py

# If code is in cloud storage (downloaded at runtime):
mainApplicationFile: gs://my-bucket/code/my_job.py
mainApplicationFile: s3a://my-bucket/code/my_job.py
```

**local://** means the file is inside the Docker image at that path.

#### sparkConf

Any Spark configuration goes here as key-value pairs:

```yaml
sparkConf:
  # Parallelism
  spark.sql.shuffle.partitions: "200"
  spark.default.parallelism: "100"
  
  # Memory
  spark.memory.fraction: "0.8"
  spark.memory.storageFraction: "0.3"
  
  # Dynamic allocation
  spark.dynamicAllocation.enabled: "true"
  spark.dynamicAllocation.shuffleTracking.enabled: "true"
  
  # Kubernetes specific
  spark.kubernetes.driver.pod.name: "my-custom-driver"
```

**Important**: All values must be strings (in quotes), even numbers!

#### driver and executor cores/memory

These map directly to spark-submit options:

| YAML Field | spark-submit Equivalent |
|------------|------------------------|
| `driver.cores` | `--conf spark.driver.cores=` |
| `driver.memory` | `--conf spark.driver.memory=` |
| `executor.cores` | `--conf spark.executor.cores=` |
| `executor.instances` | `--conf spark.executor.instances=` |
| `executor.memory` | `--conf spark.executor.memory=` |

---

## ScheduledSparkApplication — Cron Jobs for Spark {#scheduled-spark}

Run Spark jobs on a schedule, like cron.

### Basic Example

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: ScheduledSparkApplication
metadata:
  name: daily-etl
  namespace: spark-jobs
spec:
  # ═══════════════════════════════════════════════════════════════════
  # SCHEDULING OPTIONS
  # ═══════════════════════════════════════════════════════════════════
  
  # Cron expression (UTC timezone)
  schedule: "0 2 * * *"  # Every day at 2:00 AM UTC
  
  # What if previous run is still running when next run is due?
  concurrencyPolicy: Forbid  # Forbid, Allow, or Replace
  
  # How many completed runs to keep (for history/debugging)
  successfulRunHistoryLimit: 3
  failedRunHistoryLimit: 5
  
  # Is this schedule active?
  suspend: false
  
  # ═══════════════════════════════════════════════════════════════════
  # SPARK APPLICATION TEMPLATE (same as SparkApplication spec)
  # ═══════════════════════════════════════════════════════════════════
  
  template:
    type: Python
    mode: cluster
    image: myrepo/spark:3.5.0
    mainApplicationFile: local:///opt/spark/work-dir/daily_etl.py
    arguments:
      - "--date"
      - "{{ .RunTime.Format \"2006-01-02\" }}"  # Templated!
    driver:
      cores: 2
      memory: "4g"
      serviceAccount: spark
    executor:
      cores: 4
      instances: 10
      memory: "8g"
    restartPolicy:
      type: OnFailure
      onFailureRetries: 3
```

### Understanding the Cron Expression

```
┌───────────── minute (0 - 59)
│ ┌───────────── hour (0 - 23)
│ │ ┌───────────── day of month (1 - 31)
│ │ │ ┌───────────── month (1 - 12)
│ │ │ │ ┌───────────── day of week (0 - 6, Sunday = 0)
│ │ │ │ │
* * * * *
```

| Schedule | Cron Expression | Explanation |
|----------|-----------------|-------------|
| Every hour | `0 * * * *` | At minute 0 of every hour |
| Every day at midnight | `0 0 * * *` | At 00:00 every day |
| Every day at 2 AM | `0 2 * * *` | At 02:00 every day |
| Every Monday at 9 AM | `0 9 * * 1` | At 09:00 every Monday |
| Every 15 minutes | `*/15 * * * *` | At 0, 15, 30, 45 minutes |
| First of month at 1 AM | `0 1 1 * *` | At 01:00 on day 1 |

### Concurrency Policy

What happens if the cron trigger fires while a previous run is still executing?

| Policy | Behavior |
|--------|----------|
| `Forbid` | Skip the new run. The running job continues. |
| `Allow` | Start the new run anyway. Multiple runs execute simultaneously. |
| `Replace` | Kill the running job and start the new one. |

**For ETL jobs**: Use `Forbid` — you usually don't want overlapping runs.

### Template Variables

You can use template variables in your SparkApplication:

```yaml
arguments:
  - "--date"
  - "{{ .RunTime.Format \"2006-01-02\" }}"  # 2024-01-15
  - "--timestamp"
  - "{{ .RunTime.Unix }}"  # 1705312800
```

This lets you pass the scheduled run time to your Spark job.

### Managing Scheduled Jobs

```bash
# List scheduled jobs
kubectl get scheduledsparkapplication -n spark-jobs

# See the actual runs (SparkApplications created by the schedule)
kubectl get sparkapplication -n spark-jobs -l scheduledSparkApplicationName=daily-etl

# Suspend a schedule (stop new runs, keep existing)
kubectl patch scheduledsparkapplication daily-etl -n spark-jobs \
  --type merge -p '{"spec":{"suspend":true}}'

# Resume
kubectl patch scheduledsparkapplication daily-etl -n spark-jobs \
  --type merge -p '{"spec":{"suspend":false}}'
```

---

## Restart Policies and Failure Handling {#restart-policies}

### Understanding Failure Types

There are two types of failures:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    FAILURE TYPES                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   1. SUBMISSION FAILURE                                                 │
│      ───────────────────                                                │
│      The driver pod couldn't even start.                                │
│                                                                          │
│      Causes:                                                            │
│      • Image not found (ImagePullBackOff)                               │
│      • Node doesn't have enough resources                               │
│      • ServiceAccount doesn't exist                                     │
│      • Invalid configuration                                            │
│                                                                          │
│      Retry with: onSubmissionFailureRetries                             │
│                                                                          │
│   ─────────────────────────────────────────────────────────────────────  │
│                                                                          │
│   2. EXECUTION FAILURE                                                  │
│      ─────────────────                                                  │
│      The driver pod started but the job failed.                         │
│                                                                          │
│      Causes:                                                            │
│      • Application threw an exception                                   │
│      • OOMKilled (ran out of memory)                                    │
│      • Too many executor failures                                       │
│      • Driver crashed                                                   │
│                                                                          │
│      Retry with: onFailureRetries                                       │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Restart Policy Configuration

```yaml
restartPolicy:
  type: OnFailure       # When to restart
  
  # For execution failures (job ran but failed)
  onFailureRetries: 3                 # Try up to 3 times
  onFailureRetryInterval: 60          # Wait 60s between retries
  
  # For submission failures (couldn't start)
  onSubmissionFailureRetries: 3       # Try up to 3 times
  onSubmissionFailureRetryInterval: 30  # Wait 30s between retries
```

### Restart Policy Types

| Type | When It Restarts | Use Case |
|------|-----------------|----------|
| `Never` | Never restarts | One-shot jobs where failure means "stop" |
| `OnFailure` | Only on failure | Most ETL jobs |
| `Always` | On success or failure | Long-running apps (rare for batch) |

### What Retry Looks Like

```bash
kubectl get sparkapplication my-job -n spark-jobs -w
```

```
NAME      STATUS       ATTEMPTS   AGE
my-job    SUBMITTED    1          0s
my-job    RUNNING      1          10s
my-job    FAILED       1          5m     # First attempt failed
my-job    SUBMITTED    2          5m     # Retry started (after 60s interval)
my-job    RUNNING      2          5m10s
my-job    COMPLETED    2          10m    # Second attempt succeeded!
```

---

## Debugging Operator-Managed Jobs {#debugging}

### Check SparkApplication Status

```bash
# Quick status
kubectl get sparkapplication my-job -n spark-jobs

# Detailed status
kubectl describe sparkapplication my-job -n spark-jobs
```

The describe output shows:
- Current state (SUBMITTED, RUNNING, COMPLETED, FAILED)
- Driver pod name
- Executor states
- Events (what happened and when)

### Get Driver Logs

```bash
# If job is running or just completed
kubectl logs -n spark-jobs my-job-driver

# Follow logs in real-time
kubectl logs -n spark-jobs my-job-driver -f
```

### Access Spark UI

The driver runs the Spark UI on port 4040:

```bash
# Port-forward
kubectl port-forward -n spark-jobs my-job-driver 4040:4040

# Open browser
open http://localhost:4040
```

### Check Kubernetes Events

```bash
# Events in the namespace
kubectl get events -n spark-jobs --sort-by='.lastTimestamp'

# Events for specific pod
kubectl describe pod my-job-driver -n spark-jobs | grep -A 20 Events
```

### Common Issues and Solutions

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| Status stays SUBMITTED | Driver pod can't start | Check `kubectl describe pod` for events |
| ImagePullBackOff | Can't download image | Check image name, registry auth |
| Pending (no scheduling) | No node has resources | Reduce resource requests or add nodes |
| OOMKilled | Ran out of memory | Increase `memoryOverhead` |
| FAILED immediately | Application error | Check driver logs |

### Keep Driver Logs After Completion

By default, driver pods are deleted after completion. To keep them for debugging:

```yaml
sparkConf:
  spark.kubernetes.driver.pod.deleteOnTermination: "false"
```

Remember to clean them up manually later!

---

## When to Use spark-submit vs Operator {#when-to-use}

| Scenario | Recommendation | Reason |
|----------|----------------|--------|
| Quick one-off test | spark-submit | Faster to run a command |
| Development/debugging | spark-submit | Direct feedback, easy iteration |
| Production scheduled jobs | Operator | Retry policies, visibility, GitOps |
| CI/CD pipelines | Operator | Declarative, version controlled |
| Airflow-managed jobs | Both work | Airflow has operators for both |
| GitOps/ArgoCD | Operator | YAML files in git |

### The Decision Tree

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    WHEN TO USE WHAT?                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Is this a one-off test or development?                                │
│   ├── YES → spark-submit                                                │
│   └── NO                                                                │
│        │                                                                │
│        ▼                                                                │
│   Do you need retry on failure?                                         │
│   ├── YES → Spark Operator                                              │
│   └── NO                                                                │
│        │                                                                │
│        ▼                                                                │
│   Do you need cron-style scheduling?                                    │
│   ├── YES → ScheduledSparkApplication (Operator)                        │
│   └── NO                                                                │
│        │                                                                │
│        ▼                                                                │
│   Do you want job status visible in Kubernetes?                         │
│   ├── YES → Spark Operator                                              │
│   └── NO                                                                │
│        │                                                                │
│        ▼                                                                │
│   Are you using GitOps (ArgoCD, Flux)?                                  │
│   ├── YES → Spark Operator (declarative YAML)                           │
│   └── NO → Either works, Operator preferred for production              │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Summary

You now understand:

✅ **Why spark-submit has limitations** — One-time command, no retries, no visibility  
✅ **Imperative vs Declarative** — Commands vs desired state  
✅ **What an Operator is** — CRD + Controller that encodes domain knowledge  
✅ **How the Spark Operator works** — Watch, reconcile, update status  
✅ **Installation** — Helm-based, separate namespaces  
✅ **SparkApplication** — Every field explained  
✅ **ScheduledSparkApplication** — Cron-based scheduling  
✅ **Restart policies** — Automatic failure recovery  
✅ **Debugging** — Status, logs, Spark UI  

---

## What's Next

In **Part 7: Airflow Integration**, we'll cover:
- SparkKubernetesOperator explained
- DAG design patterns for Spark
- Passing parameters with Jinja templating
- Monitoring Spark jobs from Airflow

---

*Next: [Part 7: Airflow Integration →](./article-07-airflow-integration.md)*

*Previous: [Part 5: spark-submit Mastery ←](./article-05-spark-submit.md)*
