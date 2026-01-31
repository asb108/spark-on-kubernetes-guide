# 🚀 Mastering Spark on Kubernetes

> **The most comprehensive guide to running Apache Spark on Kubernetes** — from architecture deep dives to production deployment patterns.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.27+-326CE5?logo=kubernetes&logoColor=white)](https://kubernetes.io/)
[![Substack](https://img.shields.io/badge/Substack-Subscribe-orange?logo=substack&logoColor=white)](https://atulbisht.substack.com/p/apache-spark-on-kubernetes-the-ultimate)

---

## 📚 Article Series

| Part | Title | Status | Read Time |
|------|-------|--------|-----------|
| 1 | [Architecture Deep Dive](./articles/part-01-architecture-deep-dive.md) | ✅ Published | 45 min |
| 2 | [Environment Setup (KinD + GKE)](./articles/part-02-environment-setup.md) | ✅ Published | 55 min |
| 3 | [Building Production Spark Images](./articles/part-03-docker-images.md) | ✅ Published | 50 min |
| 4 | [Security Deep Dive](./articles/part-04-security.md) | ✅ Published | 60 min |
| 5 | [spark-submit Mastery](./articles/part-05-spark-submit.md) | ✅ Published | 65 min |
| 6 | [Spark Operator](./articles/part-06-spark-operator.md) | ✅ Published | 55 min |
| 7 | [Airflow Integration](./articles/part-07-airflow-integration.md) | ✅ Published | 70 min |
| 8 | Production Best Practices | 📝 Planned | - |
| 9 | Advanced Methods (Livy, Argo) | 📝 Planned | - |
| 10 | Cloud Managed Services | 📝 Planned | - |

---

## 🎯 What You'll Learn

- **Architecture**: How Spark and Kubernetes integrate at the source code level
- **Environment Setup**: From local KinD clusters to production GKE with autoscaling
- **Docker Images**: Building optimized images with Delta Lake, cloud connectors, and monitoring
- **Security**: RBAC, Workload Identity, Network Policies, and secrets management
- **Deployment Methods**: spark-submit, Spark Operator, Airflow, Livy, and more
- **Cost Optimization**: Spot instances, right-sizing, and scale-to-zero patterns
- **Troubleshooting**: Common errors and production debugging techniques

---

## 📁 Repository Structure

```
.
├── articles/                    # Main article content (Markdown)
│   ├── part-01-architecture-deep-dive.md
│   ├── part-02-environment-setup.md
│   └── ...
├── diagrams/                    # Visual assets (HTML, open in browser)
│   ├── part-01-diagrams.html
│   └── part-02-diagrams.html
├── examples/                    # Ready-to-use code examples
│   ├── kind/                    # KinD cluster configurations
│   ├── gke/                     # GKE setup scripts
│   ├── docker/                  # Spark Docker images
│   ├── rbac/                    # Security configurations
│   ├── spark-operator/          # Spark Operator examples
│   └── airflow/                 # Airflow DAG examples
└── README.md
```

---

## 🚀 Quick Start

### Local Development with KinD

```bash
# 1. Create the cluster
kind create cluster --config examples/kind/kind-spark-cluster.yaml

# 2. Create namespace and RBAC
kubectl create namespace spark-jobs
kubectl apply -f examples/rbac/spark-rbac.yaml

# 3. Install Spark Operator
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace

# 4. Run a test job
kubectl apply -f examples/spark-operator/spark-pi.yaml
```

---

## 📰 Read on Substack

📖 **[Apache Spark on Kubernetes: The Ultimate Deep Dive](https://atulbisht.substack.com/p/apache-spark-on-kubernetes-the-ultimate)**

---

## 🔗 Related Resources

- [Apache Spark: Running on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Kubeflow Spark Operator](https://github.com/kubeflow/spark-operator)
- [GKE: Running Spark](https://cloud.google.com/kubernetes-engine/docs/tutorials/spark-on-gke)

---

## 🤝 Contributing

Contributions welcome! Fork, create a branch, and open a PR.

---

## 📝 License

MIT License - see [LICENSE](LICENSE) for details.

---

*Built with ❤️ by [@asb108](https://github.com/asb108) for the Data Engineering community*

