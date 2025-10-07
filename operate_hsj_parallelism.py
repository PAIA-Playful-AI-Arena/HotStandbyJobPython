import os
import typing as t
import kopf
import requests
from copy import deepcopy
from datetime import datetime, timezone
from loguru import logger
from kubernetes import client
from kubernetes.config import load_kube_config, load_incluster_config
from kubernetes.client import CoreV1Api, BatchV1Api
from kubernetes.client.exceptions import ApiException
from kubernetes.config.config_exception import ConfigException
from kubernetes.stream import stream as k8s_stream
from dotenv import load_dotenv

load_dotenv(".env")

# 導入 PodStatusMonitor（可選）
try:
    from pod_status_manager import PodStatusMonitor, RedisConnectionManager, PodStatus
    REDIS_AVAILABLE = True
except ImportError:
    logger.warning("⚠️ PodStatusMonitor not available, Redis mode disabled")
    REDIS_AVAILABLE = False
    PodStatusMonitor = None
    RedisConnectionManager = None
    PodStatus = None

GROUP = "apps.paia.tech"
VERSION = "v1alpha1"
PLURAL = "hotstandbyjobs"

DEFAULT_BUSY_ANN = "paia.tech/busy"

HTTP_DEFAULTS = {
    "port": 8080,
    "path": "/busy",
    "successIsBusy": True,
    "timeoutSeconds": 1,
    "periodSeconds": 10,
}

EXEC_DEFAULTS = {
    "command": ["cat", "/tmp/healthy"],
    "container": None,
    "timeoutSeconds": 1,
    "successIsBusy": True,
}

JOB_OWNER_LABEL = "hsj.paia.tech/name"


# ---------- startup ----------
@kopf.on.startup()
def init_clients(memo: kopf.Memo, **_):
    try:
        if os.getenv("KUBERNETES_SERVICE_HOST"):
            load_incluster_config()
        else:
            load_kube_config()
    except ConfigException:
        load_kube_config()

    memo.v1: CoreV1Api = client.CoreV1Api()
    memo.batch: BatchV1Api = client.BatchV1Api()
    
    # 初始化 PodStatusMonitor（如果有配置 Redis）
    if REDIS_AVAILABLE:
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
        redis_db = int(os.getenv("REDIS_DB", "0"))
        redis_password = os.getenv("REDIS_PASSWORD")
        redis_key_prefix = os.getenv("REDIS_KEY_PREFIX", "mlgame-daemon-pod")
        
        try:
            # 建立 Redis 客戶端
            redis_client = RedisConnectionManager.create_client(
                redis_host=redis_host,
                redis_port=redis_port,
                redis_db=redis_db,
                redis_password=redis_password,
                decode_responses=True
            )
            
            # 建立 PodStatusMonitor
            memo.pod_status_monitor = PodStatusMonitor(
                redis_client=redis_client,
                key_prefix=redis_key_prefix
            )
            logger.info(f"✅ PodStatusMonitor initialized (Redis: {redis_host}:{redis_port}/{redis_db}, prefix: {redis_key_prefix})")
        except Exception as e:
            logger.warning(f"⚠️ Failed to initialize PodStatusMonitor: {e}. Redis mode disabled.")
            memo.pod_status_monitor = None
    else:
        memo.pod_status_monitor = None


# ---------- helpers ----------
def _merge_labels(*dicts: t.Dict[str, str]) -> t.Dict[str, str]:
    out: dict[str, str] = {}
    for d in dicts:
        if d:
            out.update(d)
    return out


def _pods_by_selector(v1: CoreV1Api, namespace: str, match_labels: dict) -> list[client.V1Pod]:
    sel = ",".join([f"{k}={v}" for k, v in (match_labels or {}).items()])
    pods = v1.list_namespaced_pod(namespace, label_selector=sel).items
    # <<< 只保留「未標記刪除、且 phase=Running」的 Pod，避免 Completed/Failed 影響忙碌/Idle 計數
    return [
        p for p in pods
        if not p.metadata.deletion_timestamp and p.status and p.status.phase == "Running"
    ]


def _is_pod_busy_by_annotation(pod: client.V1Pod, ann_key: str) -> bool:
    anns = pod.metadata.annotations or {}
    return str(anns.get(ann_key, "false")).lower() == "true"


def _is_pod_busy_by_http(pod: client.V1Pod, http_cfg: dict) -> bool:
    if not pod.status or not pod.status.pod_ip:
        return False
    if pod.status.phase != "Running":
        return False
    port = http_cfg.get("port", HTTP_DEFAULTS["port"])
    path = http_cfg.get("path", HTTP_DEFAULTS["path"])
    timeout = http_cfg.get("timeoutSeconds", HTTP_DEFAULTS["timeoutSeconds"])
    success_is_busy = http_cfg.get("successIsBusy", HTTP_DEFAULTS["successIsBusy"])
    url = f"http://{pod.status.pod_ip}:{port}{path}"
    try:
        resp = requests.get(url, timeout=timeout)
        ok = 200 <= resp.status_code < 300
        return bool(ok) if success_is_busy else (not ok)
    except requests.RequestException:
        return False


def _is_pod_busy_by_exec(v1: CoreV1Api, pod: client.V1Pod, namespace: str, exec_cfg: dict) -> bool:
    if pod.status is None or pod.status.phase != "Running":
        return False
    cmd = exec_cfg.get("command", EXEC_DEFAULTS["command"]) or EXEC_DEFAULTS["command"]
    container = exec_cfg.get("container", EXEC_DEFAULTS["container"])
    timeout = int(exec_cfg.get("timeoutSeconds", EXEC_DEFAULTS["timeoutSeconds"]))
    # success_is_busy = bool(exec_cfg.get("successIsBusy", EXEC_DEFAULTS["successIsBusy"]))
    try:
        resp = k8s_stream(
            v1.connect_get_namespaced_pod_exec,
            name=pod.metadata.name,
            namespace=namespace,
            command=cmd,
            container=container,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False,
        )
        resp.run_forever(timeout=timeout)
        ok = (resp.returncode == 0)
        resp.close()
        
        if not ok :
            # check the pod is successed or not
            pod = v1.read_namespaced_pod(pod.metadata.name, namespace)
            if pod.status.phase == "Succeeded":
                ok = True
    except Exception:
        ok = False
    return bool(ok) 


def _count_busy_idle(
    v1: CoreV1Api,
    namespace: str,
    match_labels: dict,
    mode: str,
    ann_key: str,
    http_cfg: dict,
    exec_cfg: dict,
    pod_status_monitor: t.Optional[PodStatusMonitor] = None,
) -> tuple[int, int, int]:
    """
    計算 busy 和 idle 的 Pod 數量
    
    支援多種模式：
    - redis: 從 Redis 查詢狀態（最高效，推薦）
    - http: HTTP 探測
    - exec: exec 命令探測
    - annotation: 透過 annotation 判斷
    """
    
    # 優先使用 Redis 模式（如果有配置且 mode 為 redis）
    if mode == "redis" and pod_status_monitor:
        return _count_busy_idle_redis(v1, namespace, match_labels, pod_status_monitor)
    
    # Fallback 到原有的探測方式
    pods = _pods_by_selector(v1, namespace, match_labels)
    busy_count = 0
    successed_pods = 0
    running_pods_count = 0
    failed_pods = 0
    running_pods = []
    
    for p in pods:
        if p.status.phase == "Succeeded":
            successed_pods += 1
        elif p.status.phase == "Running":
            running_pods_count += 1
            running_pods.append(p)
        elif p.status.phase == "Failed":
            failed_pods += 1
    
    if mode == "http":
        for p in running_pods:
            if _is_pod_busy_by_http(p, http_cfg):
                busy_count += 1
    elif mode == "exec":
        for p in running_pods:
            if _is_pod_busy_by_exec(v1, p, namespace, exec_cfg):
                busy_count += 1
    elif mode == "annotation":
        for p in running_pods:
            if _is_pod_busy_by_annotation(p, ann_key):
                busy_count += 1
    elif mode == "redis":
        # Redis 模式但沒有 pod_status_monitor，記錄警告並 fallback 到 annotation
        logger.warning("Redis mode requested but PodStatusMonitor not available, falling back to annotation mode")
        for p in running_pods:
            if _is_pod_busy_by_annotation(p, ann_key):
                busy_count += 1
    else:
        raise ValueError(f"invalid probe mode: {mode}")

    # total_active = len(running_pods)
    idle = max(0, running_pods_count - busy_count)
    if bool(exec_cfg.get("successIsBusy", EXEC_DEFAULTS["successIsBusy"])):
        busy_count += successed_pods
    return busy_count, idle, running_pods_count


def _count_busy_idle_redis(
    v1: CoreV1Api,
    namespace: str,
    match_labels: dict,
    pod_status_monitor: PodStatusMonitor,
) -> tuple[int, int, int]:
    """
    使用 Redis 來計算 busy/idle 數量（高效能版本）
    
    優點：
    1. 避免頻繁呼叫 K8s API
    2. 批次查詢，只需 1 次 Redis 往返（HGETALL）
    3. 避免 WebSocket 連線問題
    """
    # 先從 K8s 取得 Pod 列表（只取名稱）
    pods = _pods_by_selector(v1, namespace, match_labels)
    pod_names = [p.metadata.name for p in pods]
    
    if not pod_names:
        return 0, 0, 0
    
    # 批次從 Redis 查詢所有 Pod 的狀態（一次 HGETALL 完成！）
    try:
        # 使用新的 API：get_all_pods() 回傳 {pod_name: {"status": "idle", "updated_at": 1234567890}}
        all_statuses = pod_status_monitor.get_all_pods()
        logger.debug(f"[{namespace}] Retrieved {len(all_statuses)} pod statuses from Redis")
    except Exception as e:
        logger.error(f"❌ Failed to get pod statuses from Redis: {e}")
        # Fallback: 從 K8s API 計算（假設全部為 busy，需要擴容）
        return len(pods), 0, len(pods)
    
    # 統計各種狀態
    busy_count = 0
    idle_count = 0
    starting_count = 0
    error_count = 0
    unknown_count = 0
    
    for pod_name in pod_names:
        status_dict = all_statuses.get(pod_name)
        
        if not status_dict:
            # Redis 中沒有該 Pod 的狀態，視為 unknown（可能是新啟動或尚未註冊）
            unknown_count += 1
            logger.debug(f"Pod {pod_name} not found in Redis, treating as starting")
            starting_count += 1
            continue
        
        pod_status = status_dict.get("status", PodStatus.IDLE)
        
        if pod_status == PodStatus.BUSY:
            busy_count += 1
        elif pod_status == PodStatus.IDLE:
            idle_count += 1
        elif pod_status == PodStatus.STARTING:
            starting_count += 1
        elif pod_status == PodStatus.ERROR:
            error_count += 1
        else:
            # 未知狀態當作 idle
            unknown_count += 1
            idle_count += 1
    
    total_active = len(pod_names)
    
    logger.debug(
        f"[{namespace}] Redis status summary: total={total_active}, busy={busy_count}, "
        f"idle={idle_count}, starting={starting_count}, error={error_count}, unknown={unknown_count}"
    )
    
    # 回傳：busy_count, idle_count, total_active
    # 注意：starting/error 的 Pod 不計入 idle，也不計入 busy
    return busy_count, idle_count, total_active


def _get_probe_conf(spec: dict) -> tuple[str, str, dict, dict]:
    probe = spec.get("busyProbe") or {}
    mode = (probe.get("mode") or "annotation").lower()
    ann_key = probe.get("annotationKey") or DEFAULT_BUSY_ANN
    http_cfg = {**HTTP_DEFAULTS, **(probe.get("http") or {})}
    exec_cfg = {**EXEC_DEFAULTS, **(probe.get("exec") or {})}
    return mode, ann_key, http_cfg, exec_cfg


def _desired_active_replicas(busy: int, idle_target: int, min_r: t.Optional[int], max_r: t.Optional[int]) -> int:
    desired = int(busy) + int(idle_target)
    if min_r is not None:
        desired = max(desired, int(min_r))
    if max_r is not None:
        desired = min(desired, int(max_r))
    return max(0, desired)


def _ensure_child_job(batch: BatchV1Api, owner_body: dict, name: str, namespace: str, job_spec: dict, selector: dict) -> client.V1Job:
    job_name = f"{name}-workload"
    try:
        return batch.read_namespaced_job(name=job_name, namespace=namespace)
    except ApiException as e:
        if e.status != 404:
            raise

    spec_copy = deepcopy(job_spec)

    tmpl = spec_copy.setdefault("template", {}).setdefault("metadata", {})
    tmpl_labels = tmpl.setdefault("labels", {})
    tmpl["labels"] = _merge_labels(tmpl_labels, selector, {JOB_OWNER_LABEL: owner_body["metadata"]["name"]})

    pod_spec = spec_copy["template"].setdefault("spec", {})
    if pod_spec.get("restartPolicy") not in (None, "Never"):
        pod_spec["restartPolicy"] = "Never"

    body = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": namespace,
            "labels": {
                JOB_OWNER_LABEL: owner_body["metadata"]["name"],
            },
            "ownerReferences": [{
                "apiVersion": owner_body["apiVersion"],
                "kind": owner_body["kind"],
                "name": owner_body["metadata"]["name"],
                "uid": owner_body["metadata"]["uid"],
                "controller": True,
                "blockOwnerDeletion": True,
            }],
        },
        "spec": spec_copy,
    }
    return batch.create_namespaced_job(namespace=namespace, body=body)


def _patch_job(batch: BatchV1Api, namespace: str, name: str, parallelism: int, completions: int) -> client.V1Job:
    job_name = f"{name}-workload"
    patch = {"spec": {"parallelism": int(parallelism), "completions": int(completions)}}  # <<<
    batch.patch_namespaced_job(name=job_name, namespace=namespace, body=patch)
    return batch.read_namespaced_job(name=job_name, namespace=namespace)


def _list_child_jobs(batch: BatchV1Api, namespace: str, owner_name: str) -> list[client.V1Job]:
    sel = f"{JOB_OWNER_LABEL}={owner_name}"
    return batch.list_namespaced_job(namespace, label_selector=sel).items

def _job_is_active(j: client.V1Job) -> bool:
    s = j.status or client.V1JobStatus()
    return bool(s.active)

def _job_is_completed(j: client.V1Job) -> bool:
    s = j.status or client.V1JobStatus()
    return (s.succeeded or 0) > 0 and not s.active

def _job_is_failed(j: client.V1Job) -> bool:
    s = j.status or client.V1JobStatus()
    return (s.failed or 0) > 0 and not s.active

def _create_one_child_job(batch: BatchV1Api, owner_body: dict, name: str, namespace: str, job_spec: dict, selector: dict) -> client.V1Job:
    import random, string
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
    job_name = f"{name}-w-{suffix}"

    spec_copy = deepcopy(job_spec)

    tmpl = spec_copy.setdefault("template", {}).setdefault("metadata", {})
    tmpl_labels = tmpl.setdefault("labels", {})
    tmpl["labels"] = _merge_labels(tmpl_labels, selector, {JOB_OWNER_LABEL: owner_body["metadata"]["name"]})

    pod_spec = spec_copy["template"].setdefault("spec", {})
    pod_spec["restartPolicy"] = "Never"

    spec_copy["completions"] = 1
    spec_copy["parallelism"] = 1

    body = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": namespace,
            "labels": {JOB_OWNER_LABEL: owner_body["metadata"]["name"]},
            "ownerReferences": [{
                "apiVersion": owner_body["apiVersion"],
                "kind": owner_body["kind"],
                "name": owner_body["metadata"]["name"],
                "uid": owner_body["metadata"]["uid"],
                "controller": True,
                "blockOwnerDeletion": True,
            }],
        },
        "spec": spec_copy,
    }
    # logger.info(body)
    return batch.create_namespaced_job(namespace=namespace, body=body)

def _scale_job_pool(batch: BatchV1Api, namespace: str, owner_body: dict, owner_name: str,
                    desired_active: int, job_template: dict, selector: dict):
    jobs = _list_child_jobs(batch, namespace, owner_name)
    active_jobs = [j for j in jobs if _job_is_active(j)]
    completed_jobs = [j for j in jobs if _job_is_completed(j)]
    cur_active = len(active_jobs)

    if cur_active < desired_active:
        need = desired_active - cur_active
        for _ in range(need):
            _create_one_child_job(batch, owner_body, owner_name, namespace, job_template, selector)
    # TODO 如果採用redis 來判斷忙碌/閒置，這裡可以刪除 idle 的 Job
    # elif cur_active > desired_active:
    #     # 這裡好像有可能刪除到正在運行遊戲的 Job
    #     too_many = cur_active - desired_active
    #     to_delete = active_jobs[-too_many:]
    #     for j in to_delete:
    #         batch.delete_namespaced_job(
    #             name=j.metadata.name,
    #             namespace=namespace,
    #             body=client.V1DeleteOptions(propagation_policy="Background")
    #         )


def _sync_once(memo: kopf.Memo, body: dict, spec: dict, status: dict, meta: dict) -> dict:
    namespace = meta["namespace"]
    name = meta["name"]

    idle_target = int(spec.get("idleTarget", 0))
    min_r = spec.get("minReplicas"); max_r = spec.get("maxReplicas")
    min_r = int(min_r) if min_r is not None else None
    max_r = int(max_r) if max_r is not None else None

    selector = (spec.get("selector") or {}).get("matchLabels") or {}
    job_template = spec.get("jobTemplate") or {}

    mode, ann_key, http_cfg, exec_cfg = _get_probe_conf(spec)

    busy, idle, total_active = _count_busy_idle(
        v1=memo.v1, 
        namespace=namespace, 
        match_labels=selector,
        mode=mode, 
        ann_key=ann_key, 
        http_cfg=http_cfg, 
        exec_cfg=exec_cfg,
        pod_status_monitor=getattr(memo, 'pod_status_monitor', None),
    )
    desired_active = _desired_active_replicas(busy, idle_target, min_r, max_r)
    _scale_job_pool(
        batch=memo.batch,
        namespace=namespace,
        owner_body=body,
        owner_name=name,
        desired_active=desired_active,
        job_template=job_template,
        selector=selector,
    )
    jobs = _list_child_jobs(memo.batch, namespace, name)
    active_jobs = [j for j in jobs if _job_is_active(j)]
    completed_jobs = [j for j in jobs if _job_is_completed(j)]
    failed_jobs = [j for j in jobs if _job_is_failed(j)]
    return {
        "busyCount": int(busy),
        "idleCount": int(idle),
        "activeCount": int(total_active),  # 只計 Running Pods
        "desiredActive": int(desired_active),
        "childJobs": len(jobs),
        "activeJobs": len(active_jobs),
        "completedJobs": len(completed_jobs),
        "failedJobs": len(failed_jobs),
        "lastSyncTime": datetime.now(timezone.utc).isoformat(),
        "observedGeneration": int(meta.get("generation", 0)),
        # 如果你也想要 conditions，可直接放在這裡（會覆蓋整個陣列）
        # "conditions": [{
        #   "type": "Ready", "status": "True",
        #   "reason": "Reconciled",
        #   "message": "Reconcile OK",
        #   "lastTransitionTime": datetime.now(timezone.utc).isoformat(),
        # }],
    }

@kopf.on.create(GROUP, VERSION, PLURAL)
@kopf.on.update(GROUP, VERSION, PLURAL)
@kopf.on.resume(GROUP, VERSION, PLURAL)
def reconcile(spec, status, meta, body, memo: kopf.Memo, **_):
    return _sync_once(memo, body, spec, status or {}, meta)


SYNC_INTERVAL = float(os.getenv("SYNC_INTERVAL", "10.0"))
@kopf.timer(GROUP, VERSION, PLURAL, interval=SYNC_INTERVAL)
def periodic(spec, status, meta, body, memo: kopf.Memo, **_):
    try:
        logger.info(f"[{meta['namespace']}/{meta['name']}] Periodic reconcile started")
        result = _sync_once(memo, body, spec, status or {}, meta)
        # logger.info(f"[{meta['namespace']}/{meta['name']}] Periodic reconcile completed: {result}")
        return result
    except Exception as e:
        logger.error(f"[{meta['namespace']}/{meta['name']}] Periodic reconcile failed: {e}")
        kopf.info(body, reason="ReconcileError", message=f"timer reconcile failed: {e}")
        raise e
