from fastapi import APIRouter, HTTPException
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import httpx
from app.config import settings

router = APIRouter(prefix="/api", tags=["dashboard"])

def core():
    try: config.load_incluster_config()
    except config.ConfigException: config.load_kube_config()
    return client.CoreV1Api(), client.AppsV1Api()

@router.get("/overview")
def overview():
    v1, apps = core()
    try:
        pods = v1.list_pod_for_all_namespaces().items
        nodes = v1.list_node().items
        deps = apps.list_deployment_for_all_namespaces().items
    except ApiException as e:
        raise HTTPException(status_code=502, detail=f"Kubernetes API error: {e.reason}")
    pods = [p for p in pods if p.metadata.namespace not in settings.excluded_namespaces]
    running = sum(p.status.phase == "Running" for p in pods)
    pending = sum(p.status.phase == "Pending" for p in pods)
    failed = sum(p.status.phase == "Failed" for p in pods)
    unknown = sum(p.status.phase == "Unknown" for p in pods)
    ready_pods = sum(bool(p.status.container_statuses) and all(c.ready for c in p.status.container_statuses) for p in pods)
    ready_nodes = sum(any(c.type == "Ready" and c.status == "True" for c in (n.status.conditions or [])) for n in nodes)
    desired = sum(d.spec.replicas or 0 for d in deps)
    available = sum(d.status.available_replicas or 0 for d in deps)
    health = (100 * ready_pods / len(pods)) if pods else 100.0
    return {"total_pods":len(pods),"running":running,"pending":pending,"failed":failed,
        "unknown":unknown,"unhealthy_pods":len(pods)-ready_pods,"ready_pods":ready_pods,
        "total_nodes":len(nodes),"ready_nodes":ready_nodes,"health_pct":round(health,1),
        "desired_replicas":desired,"available_replicas":available,
        "deployment_pct":round(100*available/desired,1) if desired else None}

@router.get("/nodes")
def nodes():
    v1, _ = core()
    try: items = v1.list_node().items
    except ApiException as e: raise HTTPException(status_code=502, detail=str(e))
    out=[]
    for n in items:
        cond={c.type:c.status=="True" for c in (n.status.conditions or [])}
        out.append({"name":n.metadata.name,"ready":cond.get("Ready",False),
            "disk_pressure":cond.get("DiskPressure",False),"pid_pressure":cond.get("PIDPressure",False),
            "memory_pressure":cond.get("MemoryPressure",False),
            "cpu":n.status.capacity.get("cpu") if n.status.capacity else None,
            "memory":n.status.capacity.get("memory") if n.status.capacity else None})
    return out

@router.get("/workloads")
def workloads(namespace: str | None = None):
    _, apps = core()
    out=[]
    try:
        if namespace:
            deps=apps.list_namespaced_deployment(namespace).items
            sts=apps.list_namespaced_stateful_set(namespace).items
            ds=apps.list_namespaced_daemon_set(namespace).items
            rs=apps.list_namespaced_replica_set(namespace).items
        else:
            deps=apps.list_deployment_for_all_namespaces().items
            sts=apps.list_stateful_set_for_all_namespaces().items
            ds=apps.list_daemon_set_for_all_namespaces().items
            rs=apps.list_replica_set_for_all_namespaces().items
    except ApiException as e: raise HTTPException(status_code=502, detail=str(e))
    for kind, items in [("Deployment",deps),("StatefulSet",sts),("DaemonSet",ds),("ReplicaSet",rs)]:
        for x in items:
            ns=x.metadata.namespace
            if ns in settings.excluded_namespaces: continue
            if kind == "DaemonSet":
                desired = x.status.desired_number_scheduled or 0
                ready = x.status.number_ready or 0
            else:
                desired = x.spec.replicas or 0
                ready = x.status.ready_replicas or 0
            out.append({"namespace":ns,"name":x.metadata.name,"kind":kind,"desired":desired,"ready":ready})
    return out


@router.get("/pods")
def pods(namespace: str | None = None, search: str | None = None, phase: str | None = None, limit: int = 500):
    v1, _ = core()
    try:
        items = (v1.list_namespaced_pod(namespace).items if namespace
                 else v1.list_pod_for_all_namespaces(limit=min(max(limit, 1), 1000)).items)
    except ApiException as e:
        raise HTTPException(status_code=502, detail=f"Kubernetes API error: {e.reason}")
    out = []
    needle = (search or "").lower()
    for pod in items:
        ns = pod.metadata.namespace or ""
        if ns in settings.excluded_namespaces:
            continue
        name = pod.metadata.name or ""
        statuses = pod.status.container_statuses or []
        restarts = sum(c.restart_count or 0 for c in statuses)
        ready = sum(1 for c in statuses if c.ready)
        total = len(statuses)
        state = pod.status.phase or "Unknown"
        waiting = [getattr(getattr(c, "state", None), "waiting", None) for c in statuses]
        reasons = [w.reason for w in waiting if w and w.reason]
        reason = reasons[0] if reasons else ""
        if phase and state.lower() != phase.lower():
            continue
        if needle and needle not in f"{ns}/{name} {pod.spec.node_name or ''}".lower():
            continue
        out.append({"namespace": ns, "name": name, "phase": state, "ready": ready,
                    "containers": total, "restarts": restarts, "reason": reason,
                    "node": pod.spec.node_name or "",
                    "created": pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else None,
                    "labels": pod.metadata.labels or {},
                    "container_names": [c.name for c in (pod.spec.containers or [])]})
    out.sort(key=lambda x: (x["phase"] == "Running", x["restarts"]), reverse=False)
    return out[:min(max(limit, 1), 1000)]


@router.get("/logs")
def pod_logs(namespace: str, pod: str, container: str | None = None,
             tail: int = 200, previous: bool = False, timestamps: bool = True):
    if not namespace or not pod:
        raise HTTPException(status_code=400, detail="namespace and pod are required")
    v1, _ = core()
    try:
        result = v1.read_namespaced_pod_log(
            name=pod, namespace=namespace, container=container,
            tail_lines=min(max(tail, 1), 2000), previous=previous,
            timestamps=timestamps)
        return {"namespace": namespace, "pod": pod, "container": container,
                "previous": previous, "lines": result or ""}
    except ApiException as e:
        raise HTTPException(status_code=e.status or 502, detail=f"Unable to read pod logs: {e.reason}")

@router.get("/metrics")
async def metrics():
    if not settings.prometheus_url:
        return {"cpu_pct":None,"memory_pct":None,"configured":False}
    async def query(expr):
        async with httpx.AsyncClient(timeout=8) as c:
            r=await c.get(settings.prometheus_url.rstrip("/")+"/api/v1/query",params={"query":expr})
            r.raise_for_status()
            data=r.json()
            result=data.get("data",{}).get("result",[])
            return float(result[0]["value"][1]) if result else None
    try:
        cpu=await query('100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])))')
        mem=await query('100 * (1 - sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes))')
        return {"cpu_pct":round(cpu,1) if cpu is not None else None,
                "memory_pct":round(mem,1) if mem is not None else None,"configured":True}
    except Exception:
        return {"cpu_pct":None,"memory_pct":None,"configured":True,"error":"Prometheus query failed"}
