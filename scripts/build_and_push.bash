export tag="0.3.1"
export repo_name="hot-standby-job-operator"

docker buildx build --platform linux/amd64,linux/arm64 \
-t paiaimages.azurecr.io/${repo_name}:${tag} -t paiaimages.azurecr.io/${repo_name}:latest \
-f ./Dockerfile . --push