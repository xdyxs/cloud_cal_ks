#!/bin/bash
set -e

source scripts/env.sh

echo "== 构建后端镜像 =="
docker build --provenance=false -t ${SWR}/backend:v1 -f backend/Dockerfile backend/

echo "== 构建前端镜像 =="
docker build --provenance=false -t ${SWR}/frontend:v1 -f frontend/Dockerfile frontend/

echo "== 推送到 SWR =="
docker push ${SWR}/backend:v1
docker push ${SWR}/frontend:v1

echo "== 完成 =="
