# Part 7: Airflow Integration — Orchestrating Spark on Kubernetes

**Mastering Spark on Kubernetes Series**

*You know how to run Spark jobs with spark-submit (Part 5) and the Spark Operator (Part 6). But in production, you rarely run just one job. You have pipelines — sequences of jobs where output of one becomes input of another. That's where orchestration comes in.*

---

## What You'll Learn

By the end of this guide, you'll understand:

- ✅ What orchestration is (with real-world analogies)
- ✅ Why you need it (problems it solves)
- ✅ Airflow fundamentals from absolute scratch
- ✅ How Airflow runs on Kubernetes
- ✅ **SparkSubmitOperator** — running spark-submit from Airflow
- ✅ **SparkKubernetesOperator** — running Spark via the Operator
- ✅ Detailed comparison of both approaches
- ✅ Jinja templating for dynamic parameters
- ✅ XCom for passing data between tasks
- ✅ DAG design patterns
- ✅ Error handling and retries

**Reading Time:** 75 minutes  
**Hands-On Time:** 60 minutes  
**Prerequisites:** [Part 6: Spark Operator](./article-06-spark-operator.md)  
**Visual Reference:** [View Diagrams & Tables (Interactive)](./diagrams-part7.html)

---

## Table of Contents

1. [What is Orchestration?](#what-is-orchestration)
2. [Why Do You Need Orchestration?](#why-orchestration)
3. [What is Apache Airflow?](#what-is-airflow)
4. [Airflow Architecture — From the Ground Up](#airflow-architecture)
5. [Core Concepts — DAGs, Tasks, Operators](#core-concepts)
6. [How Airflow Runs on Kubernetes](#airflow-on-kubernetes)
7. [SparkSubmitOperator — The Direct Approach](#spark-submit-operator)
8. [SparkKubernetesOperator — The Operator Approach](#spark-kubernetes-operator)
9. [Comparison: Which Should You Use?](#comparison)
10. [Jinja Templating — Dynamic Parameters](#jinja-templating)
11. [XCom — Passing Data Between Tasks](#xcom)
12. [DAG Design Patterns](#dag-patterns)
13. [Error Handling and Retries](#error-handling)
14. [Complete Example: Production ETL DAG](#complete-example)

---

## What is Orchestration? {#what-is-orchestration}

### Let's Start with an Analogy

Think of a **symphony orchestra**:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SYMPHONY ORCHESTRA                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   You have many musicians:                                              │
│   • Violins (30 people)                                                 │
│   • Cellos (10 people)                                                  │
│   • Trumpets (4 people)                                                 │
│   • Drums (2 people)                                                    │
│   • And more...                                                         │
│                                                                          │
│   Each knows how to play their instrument.                              │
│                                                                          │
│   But a symphony isn't just "everyone play at the same time!"           │
│   It requires:                                                          │
│   • Violins start at bar 1                                              │
│   • Cellos join at bar 8                                                │
│   • Trumpets come in for the chorus                                     │
│   • If the lead violin makes a mistake, pause and retry                 │
│                                                                          │
│   WHO coordinates all this?                                             │
│                                                                          │
│                      THE CONDUCTOR                                       │
│                                                                          │
│   The conductor doesn't play any instrument.                            │
│   The conductor ORCHESTRATES — tells each section when to play,         │
│   how fast, when to pause, when to restart.                             │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**In data engineering:**
- Musicians = Individual jobs (Spark, Python scripts, SQL queries)
- Conductor = Orchestration tool (Airflow)
- Symphony = Your data pipeline

### A Data Pipeline Example

Imagine you're building an e-commerce analytics system:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATA PIPELINE                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Step 1: Extract orders from database                                  │
│   Step 2: Extract user data from database                               │
│   Step 3: Wait for both extracts to complete                            │
│   Step 4: Join orders with users (Spark job)                            │
│   Step 5: Calculate daily metrics                                       │
│   Step 6: Write to data warehouse                                       │
│   Step 7: Send email report                                             │
│                                                                          │
│   Rules:                                                                │
│   • Step 4 cannot start until steps 1 and 2 are done                    │
│   • If step 4 fails, retry 3 times                                      │
│   • If step 6 fails, alert the on-call engineer                         │
│   • Run this entire pipeline every day at 2 AM                          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**Orchestration** is the automation of:
- **Scheduling**: When does each step run?
- **Dependencies**: Which steps must complete before others start?
- **Retries**: What happens when a step fails?
- **Monitoring**: How do you know what's happening?
- **Alerting**: How do you get notified of problems?

---

## Why Do You Need Orchestration? {#why-orchestration}

### Without Orchestration — The Nightmare

Let's say you try to run the above pipeline with bash scripts:

```bash
#!/bin/bash
# daily_pipeline.sh

echo "Starting daily pipeline..."

# Step 1 & 2: Extract data
./extract_orders.sh &
PID1=$!
./extract_users.sh &
PID2=$!

# Wait for both
wait $PID1
if [ $? -ne 0 ]; then
    echo "Orders extract failed!"
    exit 1
fi

wait $PID2
if [ $? -ne 0 ]; then
    echo "Users extract failed!"
    exit 1
fi

# Step 4: Run Spark job
spark-submit \
    --master k8s://https://kubernetes:6443 \
    --deploy-mode cluster \
    /path/to/join_job.py

if [ $? -ne 0 ]; then
    echo "Spark job failed, retrying..."
    spark-submit ...  # retry
    if [ $? -ne 0 ]; then
        echo "Retry failed!"
        # Send alert somehow?
        exit 1
    fi
fi

# ... and so on
```

**Problems with this approach:**

| Problem | Description |
|---------|-------------|
| **No visibility** | Where is the pipeline now? Is it stuck? |
| **No history** | Did yesterday's run succeed? When did it finish? |
| **Manual retries** | You have to code retry logic yourself |
| **No scheduling UI** | Cron files scattered across servers |
| **Hard to maintain** | What if you need to add step 3.5? |
| **No parallel tracking** | Which runs are concurrent? |
| **Alert fatigue** | You have to build your own alerting |

### With Orchestration — The Dream

```python
# Same pipeline in Airflow
with DAG("daily_analytics", schedule="0 2 * * *"):
    
    extract_orders = PythonOperator(task_id="extract_orders", ...)
    extract_users = PythonOperator(task_id="extract_users", ...)
    
    join_data = SparkKubernetesOperator(
        task_id="join_data",
        retries=3,  # Automatic retries!
        ...
    )
    
    calculate_metrics = SparkKubernetesOperator(...)
    write_to_warehouse = PythonOperator(...)
    send_report = EmailOperator(...)
    
    # Define dependencies with simple syntax
    [extract_orders, extract_users] >> join_data >> calculate_metrics >> write_to_warehouse >> send_report
```

**What you get:**

| Feature | Benefit |
|---------|---------|
| **Visual UI** | See all pipelines, their status, history |
| **Automatic scheduling** | Cron-like scheduling with UI |
| **Built-in retries** | Just set `retries=3` |
| **Dependency management** | `>>` operator defines order |
| **Logs** | Click any task to see its logs |
| **Alerts** | Built-in email, Slack, PagerDuty |
| **History** | See every run for the past N days |
| **Backfills** | Re-run historical dates easily |

---

## What is Apache Airflow? {#what-is-airflow}

### A Brief History

```
2014: Airbnb creates Airflow internally
2015: Open-sourced
2016: Became Apache Incubator project
2019: Graduated to top-level Apache project
2020: Airflow 2.0 released (major rewrite)
2024: Most popular workflow orchestration tool
```

### What Airflow Is

Apache Airflow is:
1. **A workflow orchestration platform** — schedules and monitors pipelines
2. **Written in Python** — you define workflows as Python code
3. **Web-based UI** — visual interface for monitoring
4. **Extensible** — plugins, operators, hooks for any system

### What Airflow Is NOT

Airflow is NOT:
- **A data processing engine** — it doesn't process data itself, it orchestrates tools that do
- **A streaming platform** — it's for batch workflows, not real-time
- **An ETL tool** — it orchestrates ETL, but doesn't do the E, T, or L

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    WHAT AIRFLOW DOES                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Airflow says:                                                         │
│                                                                          │
│   "At 2 AM, run Task A.                                                 │
│    When Task A finishes successfully, run Task B.                       │
│    If Task B fails, retry 3 times.                                      │
│    If it still fails, send an email.                                    │
│    Also, show me a dashboard of all this."                              │
│                                                                          │
│   Airflow does NOT say:                                                 │
│                                                                          │
│   "Read this CSV file, filter rows, join with another table"            │
│   (That's what Spark/Pandas/SQL does)                                   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Airflow Architecture — From the Ground Up {#airflow-architecture}

Let's understand how Airflow works internally.

### The Four Core Components

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    AIRFLOW ARCHITECTURE                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                       1. WEB SERVER                              │   │
│   │                                                                   │   │
│   │   • The UI you interact with                                     │   │
│   │   • Shows DAGs, runs, logs, metrics                              │   │
│   │   • Runs as a Flask web application                              │   │
│   │   • Does NOT execute any tasks                                   │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                       2. SCHEDULER                               │   │
│   │                                                                   │   │
│   │   • The brain of Airflow                                         │   │
│   │   • Continuously scans DAG files                                 │   │
│   │   • Determines which tasks need to run                           │   │
│   │   • Sends tasks to the Executor                                  │   │
│   │   • Updates task states in the database                          │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                       3. EXECUTOR                                │   │
│   │                                                                   │   │
│   │   • Actually runs the tasks                                      │   │
│   │   • Multiple types available:                                    │   │
│   │     - LocalExecutor: runs in the scheduler process               │   │
│   │     - CeleryExecutor: runs on worker machines                    │   │
│   │     - KubernetesExecutor: runs as Kubernetes pods                │   │
│   │   • We'll use KubernetesExecutor                                 │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                       4. METADATA DATABASE                       │   │
│   │                                                                   │   │
│   │   • Stores all state                                             │   │
│   │   • DAG definitions, run history, task states                    │   │
│   │   • Usually PostgreSQL or MySQL                                  │   │
│   │   • Shared by all components                                     │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### How They Work Together

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    FLOW OF A DAG RUN                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   1. You create a DAG file (Python) and put it in the dags/ folder     │
│                                                                          │
│   2. Scheduler scans dags/ folder and parses your DAG                  │
│      • Reads the schedule: "0 2 * * *" (2 AM daily)                    │
│      • Stores DAG definition in database                               │
│                                                                          │
│   3. At 2 AM, Scheduler creates a "DAG Run" for today                  │
│      • DAG Run = one execution of your DAG                             │
│      • Has a "logical date" (the date it's processing)                 │
│                                                                          │
│   4. Scheduler determines which tasks are ready to run                 │
│      • Task A has no dependencies → ready!                             │
│      • Task B depends on A → wait                                      │
│                                                                          │
│   5. Scheduler sends Task A to the Executor                            │
│                                                                          │
│   6. Executor runs Task A                                              │
│      • (In KubernetesExecutor: creates a pod for Task A)               │
│                                                                          │
│   7. Task A completes, Executor reports status to Scheduler            │
│                                                                          │
│   8. Scheduler marks Task A as "success" in database                   │
│      • Now Task B is ready (its dependency is satisfied)               │
│                                                                          │
│   9. Web Server reads database and shows you the progress              │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Core Concepts — DAGs, Tasks, Operators {#core-concepts}

### DAG (Directed Acyclic Graph)

**What is it?**

A DAG is your workflow definition. It's a collection of tasks with dependencies.

**Breaking down the name:**
- **Directed**: Tasks have a direction (A → B means A runs before B)
- **Acyclic**: No loops (you can't have A → B → C → A)
- **Graph**: Tasks are nodes, dependencies are edges

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    WHAT IS A DAG?                                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   This is a DAG:                                                        │
│                                                                          │
│       ┌───┐     ┌───┐     ┌───┐                                         │
│       │ A │ ──► │ B │ ──► │ D │                                         │
│       └───┘     └───┘     └───┘                                         │
│                   │         ▲                                           │
│                   ▼         │                                           │
│                 ┌───┐ ──────┘                                           │
│                 │ C │                                                   │
│                 └───┘                                                   │
│                                                                          │
│   • A runs first (no dependencies)                                      │
│   • B runs after A completes                                            │
│   • C runs after B completes                                            │
│   • D runs after both B and C complete                                  │
│                                                                          │
│   This is NOT a valid DAG (has a cycle):                                │
│                                                                          │
│       ┌───┐     ┌───┐     ┌───┐                                         │
│       │ A │ ──► │ B │ ──► │ C │                                         │
│       └───┘     └───┘     └───┘                                         │
│         ▲                   │                                           │
│         └───────────────────┘   ✗ C cannot go back to A                │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**A simple DAG in Python:**

```python
from airflow import DAG
from datetime import datetime

# Create a DAG
with DAG(
    dag_id="my_first_dag",           # Unique name
    start_date=datetime(2024, 1, 1), # When to start scheduling
    schedule="0 2 * * *",            # Cron expression: daily at 2 AM
    catchup=False,                   # Don't backfill past dates
) as dag:
    # Tasks go here
    pass
```

### Task

A task is one unit of work in your DAG. It's one node in the graph.

**Examples of tasks:**
- Run a Spark job
- Execute a SQL query
- Send an email
- Copy a file
- Call an API

### Operator

An operator is a **template** for a task. It defines *what kind of work* the task does.

Think of it like:
- **Operator** = The blueprint
- **Task** = The actual house built from that blueprint

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    OPERATORS AND TASKS                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Operator (the template):              Task (the instance):            │
│                                                                          │
│   PythonOperator                        task_1 = PythonOperator(        │
│   • Runs a Python function                  task_id="extract_data",     │
│   • Parameters: python_callable,            python_callable=my_func,    │
│     op_kwargs, etc.                     )                               │
│                                                                          │
│   BashOperator                          task_2 = BashOperator(          │
│   • Runs a bash command                     task_id="list_files",       │
│   • Parameters: bash_command                bash_command="ls -la",      │
│                                         )                               │
│                                                                          │
│   SparkKubernetesOperator               task_3 = SparkKubernetesOperator│
│   • Runs Spark on K8s                       task_id="spark_job",        │
│   • Parameters: application_file,           application_file="...",     │
│     namespace, etc.                     )                               │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**Common operators:**

| Operator | What It Does |
|----------|--------------|
| `PythonOperator` | Runs a Python function |
| `BashOperator` | Runs a bash command |
| `SparkSubmitOperator` | Runs spark-submit command |
| `SparkKubernetesOperator` | Creates a SparkApplication CRD |
| `EmailOperator` | Sends an email |
| `SlackWebhookOperator` | Sends a Slack message |
| `S3ToGCSOperator` | Copies files from S3 to GCS |
| `BigQueryOperator` | Runs a BigQuery SQL query |

### Task Instance

A task instance is one execution of a task for a specific date.

```
DAG: "daily_etl"
  └── Task: "extract"
        ├── Task Instance for 2024-01-15 (success)
        ├── Task Instance for 2024-01-16 (success)
        └── Task Instance for 2024-01-17 (running)
```

### DAG Run

A DAG run is one execution of the entire DAG for a specific logical date.

```
DAG: "daily_etl"
  ├── DAG Run for 2024-01-15
  │     ├── Task Instance: extract (success)
  │     ├── Task Instance: transform (success)
  │     └── Task Instance: load (success)
  ├── DAG Run for 2024-01-16
  │     ├── Task Instance: extract (success)
  │     ├── Task Instance: transform (failed)
  │     └── Task Instance: load (no_status)
  └── DAG Run for 2024-01-17
        └── (not started yet)
```

---

## How Airflow Runs on Kubernetes {#airflow-on-kubernetes}

### The KubernetesExecutor

When Airflow runs on Kubernetes with the `KubernetesExecutor`:

1. The scheduler runs as a Kubernetes pod
2. The web server runs as a Kubernetes pod
3. The database (PostgreSQL) runs as a pod or external service
4. **Each task runs as a separate, temporary Kubernetes pod**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    KUBERNETES EXECUTOR                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │ Kubernetes Cluster                                               │   │
│   │                                                                   │   │
│   │   ┌───────────────┐  ┌───────────────┐  ┌───────────────┐       │   │
│   │   │ Web Server    │  │ Scheduler     │  │ PostgreSQL    │       │   │
│   │   │ (pod)         │  │ (pod)         │  │ (pod)         │       │   │
│   │   └───────────────┘  └───────┬───────┘  └───────────────┘       │   │
│   │                              │                                   │   │
│   │                              │ Creates pods per task             │   │
│   │                              ▼                                   │   │
│   │   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐              │   │
│   │   │ Task A  │ │ Task B  │ │ Task C  │ │ Task D  │              │   │
│   │   │ (pod)   │ │ (pod)   │ │ (pod)   │ │ (pod)   │              │   │
│   │   │         │ │         │ │         │ │         │              │   │
│   │   │ runs    │ │ runs    │ │ runs    │ │ runs    │              │   │
│   │   │ extract │ │ transform│ │ load   │ │ notify  │              │   │
│   │   └─────────┘ └─────────┘ └─────────┘ └─────────┘              │   │
│   │       ▲                                     │                   │   │
│   │       │        Pods are temporary           │                   │   │
│   │       │        Created when task runs       │                   │   │
│   │       └─────────────────────────────────────┘                   │   │
│   │                Deleted when task completes                      │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**Why is this good?**

| Benefit | Explanation |
|---------|-------------|
| **Isolation** | Each task runs in its own pod with its own resources |
| **Scalability** | Kubernetes can schedule hundreds of task pods |
| **Resource efficiency** | Pods only exist while tasks run |
| **No workers to manage** | No pre-provisioned worker pool needed |

---

## SparkSubmitOperator — The Direct Approach {#spark-submit-operator}

The `SparkSubmitOperator` lets you run `spark-submit` commands directly from Airflow.

### What It Does

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SPARK SUBMIT OPERATOR FLOW                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   1. Airflow scheduler creates a worker pod for your task               │
│                                                                          │
│   2. Worker pod runs the SparkSubmitOperator code                       │
│                                                                          │
│   3. SparkSubmitOperator executes spark-submit command:                 │
│                                                                          │
│      spark-submit \                                                     │
│        --master k8s://https://kubernetes:6443 \                         │
│        --deploy-mode cluster \                                          │
│        --conf spark.kubernetes.namespace=spark-jobs \                   │
│        local:///opt/spark/jobs/my_job.py                                │
│                                                                          │
│   4. spark-submit creates the driver pod directly                       │
│                                                                          │
│   5. Driver pod creates executor pods                                   │
│                                                                          │
│   6. SparkSubmitOperator waits for driver to complete                   │
│                                                                          │
│   7. Reports success/failure back to Airflow                            │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Basic Usage

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_job = SparkSubmitOperator(
    task_id="run_spark_job",
    application="/opt/spark/jobs/my_job.py",
    conn_id="spark_kubernetes",  # Airflow connection
    
    # Kubernetes-specific configs
    conf={
        "spark.master": "k8s://https://kubernetes.default.svc:443",
        "spark.kubernetes.namespace": "spark-jobs",
        "spark.kubernetes.container.image": "myrepo/spark:3.5.0",
        "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
        
        # Driver resources
        "spark.driver.cores": "2",
        "spark.driver.memory": "4g",
        
        # Executor resources
        "spark.executor.cores": "4",
        "spark.executor.memory": "8g",
        "spark.executor.instances": "10",
    },
    
    # Pass arguments to your application
    application_args=["--date", "{{ ds }}", "--mode", "full"],
    
    # Cluster mode for Kubernetes
    deploy_mode="cluster",
)
```

### Explaining Each Parameter

| Parameter | What It Does | Example |
|-----------|--------------|---------|
| `task_id` | Unique identifier for this task | `"run_spark_job"` |
| `application` | Path to your JAR or Python file | `"/opt/spark/jobs/my_job.py"` |
| `conn_id` | Airflow connection for Spark | `"spark_kubernetes"` |
| `conf` | Spark configuration as a dictionary | See above |
| `application_args` | Arguments passed to your code | `["--date", "2024-01-15"]` |
| `deploy_mode` | Where driver runs: `cluster` or `client` | `"cluster"` |

### Setting Up the Airflow Connection

In Airflow UI → Admin → Connections:

```
Connection Id: spark_kubernetes
Connection Type: Spark
Host: k8s://https://kubernetes.default.svc:443
Extra: {
    "deploy-mode": "cluster",
    "spark.kubernetes.namespace": "spark-jobs"
}
```

### Full Example with SparkSubmitOperator

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_submit_example",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
) as dag:
    
    # Task 1: Run Spark job using SparkSubmitOperator
    transform_data = SparkSubmitOperator(
        task_id="transform_data",
        application="local:///opt/spark/jobs/transform.py",
        conn_id="spark_kubernetes",
        deploy_mode="cluster",
        conf={
            "spark.master": "k8s://https://kubernetes.default.svc:443",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.container.image": "myrepo/spark:3.5.0",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.driver.cores": "2",
            "spark.driver.memory": "4g",
            "spark.executor.cores": "4",
            "spark.executor.memory": "8g",
            "spark.executor.instances": "10",
        },
        application_args=[
            "--date", "{{ ds }}",
            "--input", "gs://bucket/raw/{{ ds }}/",
            "--output", "gs://bucket/processed/{{ ds }}/",
        ],
    )
    
    # Task 2: Validate output
    def validate_output(**context):
        ds = context["ds"]
        print(f"Validating output for {ds}")
        # Your validation logic here
    
    validate = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )
    
    transform_data >> validate
```

### Pros and Cons of SparkSubmitOperator

| Pros | Cons |
|------|------|
| Simple if you know spark-submit | No native Kubernetes status tracking |
| Works with any Spark cluster | Driver logs in driver pod, not Airflow |
| Familiar CLI interface | No SparkApplication CRD created |
| Direct control | No automatic cleanup by Spark Operator |

---

## SparkKubernetesOperator — The Operator Approach {#spark-kubernetes-operator}

The `SparkKubernetesOperator` creates a SparkApplication CRD, which the Spark Operator then manages.

### What It Does

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SPARK KUBERNETES OPERATOR FLOW                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   1. Airflow scheduler creates a worker pod for your task               │
│                                                                          │
│   2. Worker pod runs the SparkKubernetesOperator code                   │
│                                                                          │
│   3. SparkKubernetesOperator creates a SparkApplication resource:       │
│                                                                          │
│      kubectl apply -f <<EOF                                             │
│      apiVersion: sparkoperator.k8s.io/v1beta2                           │
│      kind: SparkApplication                                             │
│      metadata:                                                          │
│        name: my-job-20240115                                            │
│      spec:                                                              │
│        ...                                                              │
│      EOF                                                                │
│                                                                          │
│   4. Spark Operator (running in cluster) sees the new CRD              │
│                                                                          │
│   5. Spark Operator creates the driver pod                             │
│                                                                          │
│   6. Driver creates executor pods                                       │
│                                                                          │
│   7. SparkKubernetesOperator polls SparkApplication status             │
│                                                                          │
│   8. When job completes, reports to Airflow                            │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### The Key Difference

| SparkSubmitOperator | SparkKubernetesOperator |
|---------------------|-------------------------|
| Runs `spark-submit` command | Creates `SparkApplication` CRD |
| Direct pod creation | Operator-managed pod creation |
| No status tracking in K8s | Status visible via `kubectl get sparkapplication` |
| No automatic retries at K8s level | Operator can retry automatically |

### Basic Usage

```python
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

spark_job = SparkKubernetesOperator(
    task_id="run_spark_job",
    namespace="spark-jobs",
    application_file="spark_applications/my_job.yaml",  # Path to YAML
    kubernetes_conn_id="kubernetes_default",
    do_xcom_push=True,  # Push SparkApplication name to XCom
)
```

### The Application File (YAML)

Create a SparkApplication YAML file:

```yaml
# dags/spark_applications/my_job.yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: my-job-{{ ds_nodash }}  # Templated for uniqueness
  namespace: spark-jobs
spec:
  type: Python
  mode: cluster
  image: myrepo/spark:3.5.0
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///opt/spark/jobs/my_job.py
  
  arguments:
    - "--date"
    - "{{ ds }}"  # Jinja template - Airflow injects the date
    - "--input"
    - "gs://bucket/raw/{{ ds }}/"
    - "--output"
    - "gs://bucket/processed/{{ ds }}/"
  
  sparkConf:
    spark.sql.shuffle.partitions: "200"
  
  driver:
    cores: 2
    memory: "4g"
    memoryOverhead: "1g"
    serviceAccount: spark
  
  executor:
    cores: 4
    memory: "8g"
    memoryOverhead: "2g"
    instances: 10
  
  restartPolicy:
    type: Never  # Let Airflow handle retries
```

### Alternative: Inline SparkApplication

You can define the SparkApplication directly in Python:

```python
spark_job = SparkKubernetesOperator(
    task_id="run_spark_job",
    namespace="spark-jobs",
    application_file=None,  # Not using a file
    template_spec={
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": "my-job-{{ ds_nodash }}",
            "namespace": "spark-jobs",
        },
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": "myrepo/spark:3.5.0",
            "mainApplicationFile": "local:///opt/spark/jobs/my_job.py",
            "arguments": ["--date", "{{ ds }}"],
            "driver": {
                "cores": 2,
                "memory": "4g",
                "serviceAccount": "spark",
            },
            "executor": {
                "cores": 4,
                "memory": "8g",
                "instances": 10,
            },
            "restartPolicy": {"type": "Never"},
        },
    },
)
```

### Pros and Cons of SparkKubernetesOperator

| Pros | Cons |
|------|------|
| Native Kubernetes integration | Requires Spark Operator installed |
| Status visible via kubectl | More complex setup |
| Can use Operator's retry logic | Need to understand CRDs |
| Clean SparkApplication lifecycle | YAML files to maintain |
| GitOps compatible | |

---

## Comparison: Which Should You Use? {#comparison}

### Detailed Comparison Table

| Feature | SparkSubmitOperator | SparkKubernetesOperator |
|---------|---------------------|-------------------------|
| **How it works** | Runs spark-submit command | Creates SparkApplication CRD |
| **Requires** | Spark installed in Airflow image | Spark Operator in cluster |
| **Job visibility** | Pod logs only | `kubectl get sparkapplication` |
| **Status updates** | Polls process exit | Polls CRD status |
| **Retries** | Airflow-level only | Airflow + Operator level |
| **Resource cleanup** | Manual | Operator handles it |
| **Configuration** | Spark conf dict | YAML or dict |
| **GitOps friendly** | Less (config in code) | Yes (YAML in git) |
| **Debugging** | Driver pod logs | CRD status + driver logs |
| **Learning curve** | Lower (familiar spark-submit) | Higher (need to learn CRDs) |

### When to Use SparkSubmitOperator

✅ **Use SparkSubmitOperator when:**
- You don't have Spark Operator installed
- You want a simpler setup
- You're migrating from spark-submit scripts
- You're comfortable with spark-submit syntax
- You don't need GitOps for job definitions

```python
# SparkSubmitOperator is simpler if you know spark-submit
SparkSubmitOperator(
    task_id="transform",
    application="local:///app/job.py",
    conn_id="spark_k8s",
    conf={
        "spark.kubernetes.container.image": "spark:3.5.0",
        "spark.executor.instances": "10",
    },
)
```

### When to Use SparkKubernetesOperator

✅ **Use SparkKubernetesOperator when:**
- You already have Spark Operator installed
- You want native Kubernetes resource management
- You use GitOps (ArgoCD, Flux)
- You want to see job status via `kubectl`
- You want the Operator's automatic cleanup

```python
# SparkKubernetesOperator for full K8s integration
SparkKubernetesOperator(
    task_id="transform",
    namespace="spark-jobs",
    application_file="spark_apps/transform.yaml",
)
```

### Decision Flowchart

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    WHICH OPERATOR TO USE?                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Is Spark Operator installed in your cluster?                          │
│                                                                          │
│     NO ──────► Use SparkSubmitOperator                                  │
│                (it works without the Operator)                          │
│                                                                          │
│     YES                                                                 │
│       │                                                                 │
│       ▼                                                                 │
│   Do you use GitOps (ArgoCD/Flux)?                                     │
│                                                                          │
│     YES ─────► Use SparkKubernetesOperator                             │
│                (YAML in git, declarative)                               │
│                                                                          │
│     NO                                                                  │
│       │                                                                 │
│       ▼                                                                 │
│   Do you want kubectl visibility into job status?                       │
│                                                                          │
│     YES ─────► Use SparkKubernetesOperator                             │
│                (kubectl get sparkapplication shows status)              │
│                                                                          │
│     NO ──────► Either works! Use what your team prefers.               │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Jinja Templating — Dynamic Parameters {#jinja-templating}

Both operators support Jinja templating to inject dynamic values.

### What is Jinja?

Jinja is a templating language. You write placeholders like `{{ variable }}` and they get replaced at runtime.

```
Template:  "Hello {{ name }}!"
Variable:  name = "Alice"
Result:    "Hello Alice!"
```

### Airflow's Built-in Variables

Airflow provides many variables automatically:

| Variable | Description | Example Value |
|----------|-------------|---------------|
| `{{ ds }}` | Execution date (YYYY-MM-DD) | `2024-01-15` |
| `{{ ds_nodash }}` | Execution date without dashes | `20240115` |
| `{{ ts }}` | Execution timestamp (ISO) | `2024-01-15T02:00:00+00:00` |
| `{{ prev_ds }}` | Previous execution date | `2024-01-14` |
| `{{ next_ds }}` | Next execution date | `2024-01-16` |
| `{{ dag.dag_id }}` | The DAG ID | `daily_etl` |
| `{{ task.task_id }}` | The task ID | `transform` |
| `{{ params.key }}` | Custom parameter | (user-defined) |

### Using Templates in SparkSubmitOperator

```python
SparkSubmitOperator(
    task_id="transform",
    application="local:///app/transform.py",
    application_args=[
        "--date", "{{ ds }}",
        "--input", "gs://bucket/raw/{{ ds }}/",
        "--output", "gs://bucket/processed/{{ ds }}/",
    ],
    conf={
        # Template in pod name for uniqueness
        "spark.kubernetes.driver.pod.name": "transform-{{ ds_nodash }}",
    },
)
```

### Using Templates in SparkKubernetesOperator YAML

```yaml
# spark_applications/transform.yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: transform-{{ ds_nodash }}  # Unique per run
  namespace: spark-jobs
spec:
  mainApplicationFile: local:///app/transform.py
  arguments:
    - "--date"
    - "{{ ds }}"
    - "--input"
    - "gs://bucket/raw/{{ ds }}/"
```

### Macros for Complex Logic

Airflow provides macros for more complex operations:

| Macro | Description | Example |
|-------|-------------|---------|
| `macros.ds_add(ds, N)` | Add N days to date | `{{ macros.ds_add(ds, -7) }}` → 7 days ago |
| `macros.ds_format(ds, in, out)` | Reformat date | `{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m/%d') }}` |

```python
# Process last 7 days of data
application_args=[
    "--start-date", "{{ macros.ds_add(ds, -7) }}",
    "--end-date", "{{ ds }}",
]
```

---

## XCom — Passing Data Between Tasks {#xcom}

XCom (cross-communication) lets tasks share small pieces of data.

### What is XCom?

- A key-value store for task communication
- Each task can **push** values
- Other tasks can **pull** values
- Stored in Airflow's metadata database

**Important:** XCom is for **small** data (IDs, paths, counts). NOT for large datasets!

### How SparkKubernetesOperator Uses XCom

When `do_xcom_push=True`:

```python
spark_job = SparkKubernetesOperator(
    task_id="transform",
    do_xcom_push=True,  # Push SparkApplication name
    ...
)
```

The operator pushes:
- `spark_application_name`: Name of the created SparkApplication

### Pulling XCom in a Downstream Task

```python
def check_spark_result(**context):
    ti = context["ti"]  # Task Instance
    
    # Pull the SparkApplication name
    app_name = ti.xcom_pull(
        task_ids="transform",
        key="spark_application_name"
    )
    
    print(f"Spark job name was: {app_name}")
    # You could use kubectl to get more details

check = PythonOperator(
    task_id="check_result",
    python_callable=check_spark_result,
)
```

---

## DAG Design Patterns {#dag-patterns}

### Pattern 1: Simple Linear Pipeline

```python
extract >> transform >> load >> notify
```

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
│ Extract │ ──► │Transform│ ──► │  Load   │ ──► │ Notify  │
└─────────┘     └─────────┘     └─────────┘     └─────────┘
```

### Pattern 2: Parallel Processing

Run independent tasks concurrently:

```python
extract >> [transform_orders, transform_users, transform_products] >> load
```

```
                ┌──────────────────┐
                │ Transform Orders │
         ┌─────►│                  │─────┐
         │      └──────────────────┘     │
┌────────┴─┐    ┌──────────────────┐    ┌─▼──────┐
│ Extract  │───►│ Transform Users  │────│  Load  │
└────────┬─┘    └──────────────────┘    └───▲────┘
         │      ┌──────────────────┐        │
         └─────►│Transform Products│────────┘
                └──────────────────┘
```

### Pattern 3: Conditional Branching

Different paths based on conditions:

```python
from airflow.operators.python import BranchPythonOperator

def choose_path(**context):
    ds = context["ds"]
    # If first day of month, do full load
    if ds.endswith("-01"):
        return "full_load"
    else:
        return "incremental_load"

branch = BranchPythonOperator(
    task_id="choose_path",
    python_callable=choose_path,
)

full_load = SparkKubernetesOperator(task_id="full_load", ...)
incremental = SparkKubernetesOperator(task_id="incremental_load", ...)

branch >> [full_load, incremental]
```

### Pattern 4: Dynamic Task Generation

Generate tasks from a list:

```python
tables = ["orders", "users", "products", "inventory"]

with DAG(...) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    for table in tables:
        transform = SparkKubernetesOperator(
            task_id=f"transform_{table}",
            application_file=f"spark_apps/{table}.yaml",
        )
        start >> transform >> end
```

---

## Error Handling and Retries {#error-handling}

### Task-Level Retries

```python
SparkKubernetesOperator(
    task_id="transform",
    retries=3,                          # Retry up to 3 times
    retry_delay=timedelta(minutes=5),   # Wait 5 minutes between retries
    retry_exponential_backoff=True,     # 5min, 10min, 20min...
    max_retry_delay=timedelta(hours=1), # Cap at 1 hour
)
```

### Recommended: Let Airflow Handle Retries

In your SparkApplication YAML:

```yaml
restartPolicy:
  type: Never  # Don't let Spark Operator retry
```

Why? Airflow's retries give you:
- Visibility in the UI (see each attempt)
- Consistent retry behavior across all operators
- Alerts per retry attempt

### Callbacks

```python
def alert_on_failure(context):
    task_id = context["task_instance"].task_id
    ds = context["ds"]
    print(f"Task {task_id} failed for {ds}")
    # Send Slack/email/PagerDuty alert

SparkKubernetesOperator(
    task_id="transform",
    on_failure_callback=alert_on_failure,
    on_success_callback=log_success,
    on_retry_callback=log_retry,
)
```

### Timeouts

```python
SparkKubernetesOperator(
    task_id="transform",
    execution_timeout=timedelta(hours=2),  # Kill if running > 2 hours
)
```

---

## Complete Example: Production ETL DAG {#complete-example}

Here's a complete, production-ready DAG:

```python
# dags/production_etl.py
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Default arguments for all tasks
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
    "execution_timeout": timedelta(hours=3),
}

def check_source_data(**context):
    """Check if source data is available."""
    ds = context["ds"]
    logger.info(f"Checking source data for {ds}")
    # Your actual check logic here
    # Return True if data exists, False otherwise
    data_ready = True  # Replace with actual check
    if data_ready:
        return "run_transform"
    else:
        return "skip_transform"

def send_failure_alert(context):
    """Send alert on task failure."""
    task_instance = context["task_instance"]
    exception = context.get("exception", "Unknown error")
    
    message = f"""
    🚨 Spark Job Failed!
    
    DAG: {task_instance.dag_id}
    Task: {task_instance.task_id}
    Execution Date: {task_instance.execution_date}
    Error: {str(exception)[:500]}
    
    Check logs: {task_instance.log_url}
    """
    
    logger.error(message)
    # Send to Slack, PagerDuty, etc.

def validate_output(**context):
    """Validate the output data."""
    ds = context["ds"]
    logger.info(f"Validating output for {ds}")
    # Your validation logic here
    # Raise exception if validation fails
    return True

with DAG(
    dag_id="production_etl",
    default_args=default_args,
    description="Production ETL pipeline with Spark on Kubernetes",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",  # Every day at 2 AM UTC
    catchup=False,
    max_active_runs=1,  # Only one run at a time
    tags=["spark", "etl", "production"],
) as dag:
    
    # Task 1: Check if source data is ready
    check_data = BranchPythonOperator(
        task_id="check_source_data",
        python_callable=check_source_data,
    )
    
    # Task 2a: Run Spark transformation
    run_transform = SparkKubernetesOperator(
        task_id="run_transform",
        namespace="spark-jobs",
        application_file="spark_applications/daily_transform.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
        on_failure_callback=send_failure_alert,
    )
    
    # Task 2b: Skip if no data
    skip_transform = EmptyOperator(
        task_id="skip_transform",
    )
    
    # Task 3: Join branches (none_failed_min_one_success trigger rule)
    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )
    
    # Task 4: Validate output
    validate = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )
    
    # Task 5: Mark as complete
    complete = EmptyOperator(
        task_id="complete",
    )
    
    # Define the DAG structure
    check_data >> [run_transform, skip_transform] >> join >> validate >> complete
```

And the SparkApplication YAML:

```yaml
# dags/spark_applications/daily_transform.yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: daily-transform-{{ ds_nodash }}
  namespace: spark-jobs
  labels:
    dag_id: production_etl
    task_id: run_transform
    execution_date: "{{ ds }}"
spec:
  type: Python
  mode: cluster
  image: myrepo/spark:3.5.0
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///opt/spark/jobs/transform.py
  
  arguments:
    - "--date"
    - "{{ ds }}"
    - "--input-path"
    - "gs://data-lake/raw/{{ ds }}/"
    - "--output-path"
    - "gs://data-lake/processed/{{ ds }}/"
  
  sparkConf:
    spark.sql.shuffle.partitions: "200"
    spark.dynamicAllocation.enabled: "true"
    spark.dynamicAllocation.minExecutors: "2"
    spark.dynamicAllocation.maxExecutors: "50"
    spark.dynamicAllocation.initialExecutors: "10"
  
  driver:
    cores: 2
    memory: "4g"
    memoryOverhead: "1g"
    serviceAccount: spark
    labels:
      dag_id: production_etl
  
  executor:
    cores: 4
    memory: "8g"
    memoryOverhead: "2g"
    labels:
      dag_id: production_etl
  
  restartPolicy:
    type: Never  # Let Airflow handle retries
```

---

## Summary

You now understand:

✅ **What orchestration is** — Coordinating multiple jobs in sequence  
✅ **Why you need it** — Visibility, retries, scheduling, alerting  
✅ **Airflow architecture** — Scheduler, Executor, Web Server, Database  
✅ **Core concepts** — DAGs, Tasks, Operators, Task Instances  
✅ **KubernetesExecutor** — Each task runs as a pod  
✅ **SparkSubmitOperator** — Runs spark-submit directly  
✅ **SparkKubernetesOperator** — Creates SparkApplication CRD  
✅ **When to use which** — Based on your needs and setup  
✅ **Jinja templating** — Dynamic parameters like `{{ ds }}`  
✅ **XCom** — Passing data between tasks  
✅ **DAG patterns** — Linear, parallel, branching, dynamic  
✅ **Error handling** — Retries, callbacks, timeouts  

---

## What's Next

In **Part 8: Production Best Practices**, we'll cover:
- Resource quotas and limits
- Multi-tenancy
- Cost optimization
- Logging and metrics
- Security hardening

---

*Next: [Part 8: Production Best Practices →](./article-08-production.md)*

*Previous: [Part 6: Spark Operator ←](./article-06-spark-operator.md)*
