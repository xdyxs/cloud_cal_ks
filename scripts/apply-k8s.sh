#!/bin/bash
set -e

# 自动设置 kubeconfig（如果存在本地配置文件）
if [ -f "D:/cloud_calculate/kubeconfig.json" ]; then
    export KUBECONFIG="D:/cloud_calculate/kubeconfig.json"
fi

echo "== 应用所有 K8s 资源 =="
kubectl apply -f k8s/01-config-secret.yaml
kubectl apply -f k8s/02-pvc.yaml
kubectl apply -f k8s/03-redis-deployment.yaml
kubectl apply -f k8s/04-redis-service.yaml
kubectl apply -f k8s/05-backend-deployment.yaml
kubectl apply -f k8s/06-backend-service.yaml
kubectl apply -f k8s/07-nginx-configmap.yaml
kubectl apply -f k8s/08-frontend-deployment-service.yaml
kubectl apply -f k8s/09-hpa.yaml

echo "== 等待 Pod 启动 =="
sleep 5
kubectl get pods -o wide
kubectl get svc
kubectl get pvc
