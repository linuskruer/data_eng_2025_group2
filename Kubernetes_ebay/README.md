
# eBay → ClickHouse (Bronze) — Airflow on Kubernetes

This component runs an **hourly eBay ingestion job** inside the university Kubernetes cluster.
It collects public product listings from the eBay Browse API and writes them into the ClickHouse `bronze.ebay_raw_data` table.

---

## 📌 Components

| File                | Purpose                                       |
| ------------------- | --------------------------------------------- |
| `ebay_dag.py`     | Airflow DAG (runs hourly)                     |
| `all_in_one.py`   | Python ingestion script (eBay → ClickHouse)  |
| `ebay_auth.py`    | Handles OAuth token for eBay API              |
| `Dockerfile`      | Container image used by Kubernetes            |
| `k8s-secret.yaml` | Template for Kubernetes environment variables |
| `README.md`       | (this file)                                   |

The Airflow task executes the container, which runs `all_in_one.py`.

---

## 📌 Container Image (already built and pushed)


registry.gitlab.cs.ut.ee/linuskruer/ebay-ingestor:latest

Kubernetes must be able to pull this image from the private registry.

---

## 📌 Registry Access (private GitLab image)

Create an image pull secret in the cluster:

```bash
kubectl create secret docker-registry gitlab-regcred \
  --docker-server=registry.gitlab.cs.ut.ee \
  --docker-username=<USERNAME> \
  --docker-password=<PASSWORD>
```


<pre class="overflow-visible!" data-start="1075" data-end="1422"><div class="contain-inline-size rounded-2xl relative bg-token-sidebar-surface-primary"><div class="sticky top-9"><div class="absolute end-0 bottom-0 flex h-9 items-center pe-2"><div class="bg-token-bg-elevated-secondary text-token-text-secondary flex items-center gap-4 rounded-sm px-2 font-sans text-xs"></div></div></div><div class="overflow-y-auto p-4" dir="ltr"></div></div></pre>
