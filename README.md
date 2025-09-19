# HotStandbyJobPython

A lightweight Python operator and CRD that provisions **hot standby** Kubernetes Jobs—keeping a configurable number of pre-spawned pods “warm” to reduce cold-start latency when real workload arrives.

> TL;DR: Apply the CRD, deploy the operator, then create a `HotStandbyJob` custom resource. The operator maintains standby pods and spins up real Jobs instantly when needed.

---

## ✨ Features

- **Hot Standby Pods**: Keep N pods ready to serve to minimize first-request latency.  
- **On-Demand Job Launch**: Converts standby capacity into real Jobs when signals/conditions are met.  
- **Auto Scale-Down**: Optional delay window before tearing down surplus pods to avoid thrash.  
- **Parallelism Variant**: An alternative operator path supporting parallel job launches.  
- **K8s-Native UX**: Define behavior declaratively via a CRD and manage with `kubectl`.

---

## 📦 Repository Layout

```
.
├─ operate_hsj.py                 # Main operator/controller (hot-standby logic)
├─ operate_hsj_parallelism.py     # Variant focusing on parallel job orchestration
├─ crd.yaml                       # CustomResourceDefinition for HotStandbyJob
├─ example.yaml                   # Example HotStandbyJob resource
├─ deploy-operate.yaml            # Deployment/Pod for running the operator in-cluster
├─ rbac.yaml                      # ClusterRole/Role + RoleBinding for operator
├─ sa-rbac.yaml                   # ServiceAccount + RBAC combo (alt. manifest)
├─ requirements.txt               # Python dependencies
└─ Dockerfile                     # Container image for operator
```
（以上檔名出自專案首頁檔案清單。）

---

## 🚀 Quick Start

### Prerequisites
- Kubernetes 1.22+（建議 1.25+）
- `kubectl` 已連到你的叢集
-（可選）本機開發：Python 3.10+，能安裝 `requirements.txt`

### 1) 安裝 CRD
```bash
kubectl apply -f crd.yaml
```

### 2) 佈署 Operator（叢集內執行）
```bash
# RBAC / ServiceAccount
kubectl apply -f sa-rbac.yaml

# 部署 operator
kubectl apply -f deploy-operate.yaml
```

> 如果你要以容器執行，`deploy-operate.yaml` 會參考由 `Dockerfile` 打好的 image。請依你自己的 registry 調整。

### 3) 建立範例 HotStandbyJob
```bash
kubectl apply -f example.yaml
kubectl get hotstandbyjobs
kubectl describe hotstandbyjob <name>
```

### （選擇）本機執行 Operator
若要在本機直連叢集測試：
```bash
pip install -r requirements.txt
python operate_hsj.py
# 或針對平行版：
# python operate_hsj_parallelism.py
```

---

## 🧩 Custom Resource（CRD）概念與欄位

> 以下為預期欄位說明，實際以 `crd.yaml` 與 `example.yaml` 為準。

`HotStandbyJob`（namespaced）常見欄位示意：

```yaml
apiVersion: your.group/v1alpha1
kind: HotStandbyJob
metadata:
  name: demo-hsj
spec:
  standbyReplicas: 2            # 想維持的熱身（standby）Pod 數量
  scaleDownDelaySeconds: 60     # 間隔多長時間才回收多餘 Standby，避免反覆震盪

  # 你的工作負載模板（通常類似 Job 的模板）
  jobTemplate:
    image: busybox
    command: ["sh", "-c", "echo hello && sleep 5"]
    parallelism: 1
    backoffLimit: 0
    ttlSecondsAfterFinished: 60

  triggers:
    type: "annotation|http|queue"
```

---

## 🔄 Parallelism 變體

- `operate_hsj_parallelism.py` 針對需要 **一次啟多個工作** 的場景設計。
- 使用方式同主程式，但請先確認 `crd.yaml`／`example.yaml` 是否支援 `parallelism`。

---

## 🛠️ 開發與建置

### Build Image
```bash
docker build -t <your-registry>/hotstandbyjob-operator:latest .
docker push <your-registry>/hotstandbyjob-operator:latest
kubectl apply -f deploy-operate.yaml
```

### 本機測試
```bash
python operate_hsj.py
kubectl get pods,job,hotstandbyjob -w
```

---

## 🧪 驗證與觀察

```bash
kubectl get hotstandbyjob
kubectl get pods -w
kubectl get jobs -w
kubectl describe hotstandbyjob <name>
```
---

## 🧹 清除

```bash
kubectl delete -f example.yaml
kubectl delete -f deploy-operate.yaml
kubectl delete -f rbac.yaml
kubectl delete -f crd.yaml
```