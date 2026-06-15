# 环境变量配置
# 使用方式：source scripts/env.sh
export REGION=af-north-1
export ORG=xdeng
export SWR=swr.${REGION}.myhuaweicloud.com/${ORG}
export REDIS_PASSWORD=cloud-course-2025

echo "SWR 地址: ${SWR}"
echo "Redis 密码: ${REDIS_PASSWORD}"
