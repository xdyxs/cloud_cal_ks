#!/bin/bash
set -e

source scripts/env.sh

echo "== 1. 加载离线镜像 =="
docker load -i 离线包/spark/spark-operator-2.5.0.tar
docker load -i 离线包/spark/pyspark-v9.tar
# docker load -i 离线包/mpi/mpi4py-latest.tar   # 不做 MPI，注释掉
docker load -i 离线包/monitoring/monitoring-all.tar

echo "== 2. 给镜像重新打 Tag =="

# Spark Operator
docker tag ghcr.io/kubeflow/spark-operator/controller:2.5.0 ${SWR}/spark-operator:2.5.0

# PySpark
docker tag swr.cn-east-3.myhuaweicloud.com/cloud-course-2025212245/pyspark:v9 ${SWR}/pyspark:v9

# 监控套件（修正为 docker load 后的真实镜像名）
docker tag grafana/grafana:12.4.3                              ${SWR}/grafana:12.4.3
docker tag quay.io/prometheus/prometheus:v3.11.2               ${SWR}/prometheus:v3.11.2
docker tag quay.io/prometheus/alertmanager:v0.32.0             ${SWR}/alertmanager:v0.32.0
docker tag quay.io/prometheus/node-exporter:v1.11.1            ${SWR}/node-exporter:v1.11.1
docker tag quay.io/prometheus-operator/prometheus-operator:v0.90.1       ${SWR}/prometheus-operator:v0.90.1
docker tag quay.io/prometheus-operator/prometheus-config-reloader:v0.90.1 ${SWR}/prometheus-config-reloader:v0.90.1
docker tag ghcr.io/jkroepke/kube-webhook-certgen:1.8.1        ${SWR}/kube-webhook-certgen:1.8.1
docker tag registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.18.0 ${SWR}/kube-state-metrics:v2.18.0

# 如果 monitoring-all.tar 里还有 k8s-sidecar，请确认实际镜像名，常见为：
# docker tag kiwigrid/k8s-sidecar:2.6.0 ${SWR}/k8s-sidecar:2.6.0

echo "== 3. 推送到 SWR =="
docker push ${SWR}/spark-operator:2.5.0
docker push ${SWR}/pyspark:v9
docker push ${SWR}/grafana:12.4.3
docker push ${SWR}/prometheus:v3.11.2
docker push ${SWR}/alertmanager:v0.32.0
docker push ${SWR}/node-exporter:v1.11.1
docker push ${SWR}/prometheus-operator:v0.90.1
docker push ${SWR}/kube-webhook-certgen:1.8.1
docker push ${SWR}/prometheus-config-reloader:v0.90.1
docker push ${SWR}/kube-state-metrics:v2.18.0
# docker push ${SWR}/k8s-sidecar:2.6.0   # 确认有镜像后再取消注释

echo "== 完成，请在 SWR 控制台将这些镜像设为公开 =="