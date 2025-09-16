# operate_hsj_parallelism.py
import os
import typing as t
import time
import kopf
import requests
from copy import deepcopy

from kubernetes import client
from kubernetes.config import load_kube_config, load_incluster_config
from kubernetes.client import CoreV1Api, BatchV1Api
from kubernetes.client.exceptions import ApiException
from kubernetes.config.config_exception import ConfigException
from kubernetes.stream import stream as k8s_stream

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
    return [p for p in pods if not p.metadata.deletion_timestamp]


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
    success_is_busy = bool(exec_cfg.get("successIsBusy", EXEC_DEFAULTS["successIsBusy"]))
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
    except Exception:
        ok = False
    return bool(ok) if success_is_busy else (not ok)


def _count_busy_idle(
    v1: CoreV1Api,
    namespace: str,
    match_labels: dict,
    mode: str,
    ann_key: str,
    http_cfg: dict,
    exec_cfg: dict,
) -> tuple[int, int]:
    pods = _pods_by_selector(v1, namespace, match_labels)
    busy = 0
    if mode == "http":
        for p in pods:
            if _is_pod_busy_by_http(p, http_cfg):
                busy += 1
    elif mode == "exec":
        for p in pods:
            if _is_pod_busy_by_exec(v1, p, namespace, exec_cfg):
                busy += 1
    else:
        for p in pods:
            if _is_pod_busy_by_annotation(p, ann_key):
                busy += 1
    idle = max(0, len(pods) - busy)
    return busy, idle


def _get_probe_conf(spec: dict) -> tuple[str, str, dict, dict]:
    probe = spec.get("busyProbe") or {}
    mode = (probe.get("mode") or "annotation").lower()
    ann_key = probe.get("annotationKey") or DEFAULT_BUSY_ANN
    http_cfg = {**HTTP_DEFAULTS, **(probe.get("http") or {})}
    exec_cfg = {**EXEC_DEFAULTS, **(probe.get("exec") or {})}
    return mode, ann_key, http_cfg, exec_cfg


def _desired_replicas(busy: int, idle_target: int, min_r: t.Optional[int], max_r: t.Optional[int]) -> int:
    desired = int(busy) + int(idle_target)
    if min_r is not None:
        desired = max(desired, int(min_r))
    if max_r is not None:
        desired = min(desired, int(max_r))
    return desired


def _ensure_child_job(batch: BatchV1Api, owner_body: dict, name: str, namespace: str, job_spec: dict) -> client.V1Job:
    """
    確保存在一個 Job：<name>-workload。
    若不存在則使用 CR.spec.jobTemplate 建立；存在則直接回傳。
    之後僅 patch .spec.parallelism。
    """
    job_name = f"{name}-workload"
    try:
        return batch.read_namespaced_job(name=job_name, namespace=namespace)
    except ApiException as e:
        if e.status != 404:
            raise

    # create new
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
        "spec": deepcopy(job_spec),
    }
    return batch.create_namespaced_job(namespace=namespace, body=body)


def _patch_job_parallelism(batch: BatchV1Api, namespace: str, name: str, parallelism: int) -> client.V1Job:
    job_name = f"{name}-workload"
    patch = {"spec": {"parallelism": int(parallelism)}}
    batch.patch_namespaced_job(name=job_name, namespace=namespace, body=patch)
    return batch.read_namespaced_job(name=job_name, namespace=namespace)


# ---------- core reconcile ----------
def _sync_once(
    memo: kopf.Memo,
    body: dict,
    spec: dict,
    status: dict,
    meta: dict,
) -> dict:
    namespace = meta["namespace"]
    name = meta["name"]

    idle_target = int(spec.get("idleTarget", 0))
    min_r = spec.get("minReplicas")
    max_r = spec.get("maxReplicas")
    min_r = int(min_r) if min_r is not None else None
    max_r = int(max_r) if max_r is not None else None

    selector = (spec.get("selector") or {}).get("matchLabels") or {}
    job_template = spec.get("jobTemplate") or {}

    mode, ann_key, http_cfg, exec_cfg = _get_probe_conf(spec)

    # 1) 確保子 Job 存在
    job = _ensure_child_job(memo.batch, body, name, namespace, job_template)

    # 2) 計算 busy/idle
    busy, idle = _count_busy_idle(
        v1=memo.v1,
        namespace=namespace,
        match_labels=selector,
        mode=mode,
        ann_key=ann_key,
        http_cfg=http_cfg,
        exec_cfg=exec_cfg,
    )

    # 3) 期望並調整 parallelism
    desired = _desired_replicas(busy, idle_target, min_r, max_r)

    current_parallelism = int(job.spec.parallelism or 0)
    if current_parallelism != desired:
        job = _patch_job_parallelism(memo.batch, namespace, name, desired)

    return {
        "busyCount": int(busy),
        "idleCount": int(idle),
        "desiredReplicas": int(desired),
        "observedGeneration": int(meta.get("generation", 0)),
    }


# ---------- handlers ----------
@kopf.on.create(GROUP, VERSION, PLURAL)
@kopf.on.update(GROUP, VERSION, PLURAL)
@kopf.on.resume(GROUP, VERSION, PLURAL)
def reconcile(spec, status, meta, body, memo: kopf.Memo, **_):
    return _sync_once(memo, body, spec, status or {}, meta)


@kopf.timer(GROUP, VERSION, PLURAL, interval=10.0)
def periodic(spec, status, meta, body, memo: kopf.Memo, **_):
    try:
        return _sync_once(memo, body, spec, status or {}, meta)
    except Exception as e:
        kopf.info(body, reason="ReconcileError", message=f"timer reconcile failed: {e}")
        return None
