import asyncio, hashlib, json, logging, uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from app.config import settings
from app.db import SessionLocal, Incident
from app.ai import analyze
from app.emailer import send_incident_email

log = logging.getLogger("monitor")
RCA_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="rca-worker")

def load_kube_config():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

def _fingerprint(kind, namespace, name, reason):
    return hashlib.sha256(f"{kind}|{namespace}|{name}|{reason}".encode()).hexdigest()

def _severity(reason):
    if reason in {"OOMKilled", "CrashLoopBackOff", "NodeNotReady", "FailedScheduling"}:
        return "HIGH"
    if reason in {"ImagePullBackOff", "ErrImagePull", "FailedMount", "Evicted"}:
        return "MEDIUM"
    return "LOW"

def _save_and_analyze(evidence):
    fp = _fingerprint(evidence["kind"], evidence.get("namespace",""),
                      evidence.get("name",""), evidence["reason"])
    with SessionLocal() as db:
        existing = db.query(Incident).filter_by(fingerprint=fp, status="OPEN").first()
        if existing:
            existing.occurrences += 1
            existing.updated_at = datetime.now(timezone.utc)
            db.commit()
            return
        incident = Incident(id=str(uuid.uuid4()), fingerprint=fp,
            severity=_severity(evidence["reason"]), namespace=evidence.get("namespace",""),
            pod_name=evidence.get("name",""), node_name=evidence.get("node",""),
            summary=evidence.get("summary", evidence["reason"]), evidence=evidence)
        db.add(incident)
        db.commit()
        db.refresh(incident)
        incident_id = incident.id
    RCA_POOL.submit(lambda: asyncio.run(_complete_incident(incident_id)))

async def _complete_incident(incident_id):
    with SessionLocal() as db:
        inc = db.get(Incident, incident_id)
        if not inc:
            return
        evidence = dict(inc.evidence)
        try:
            rca = await analyze(evidence)
            inc.rca = rca
            inc.email_status = await send_incident_email(inc, rca)
        except Exception:
            log.exception("RCA/email processing failed for incident %s", incident_id)
            inc.email_status = "FAILED"
        db.commit()

def _pod_evidence(v1, pod):
    ns, name = pod.metadata.namespace, pod.metadata.name
    if ns in settings.excluded_namespaces:
        return None
    statuses = pod.status.container_statuses or []
    restarts = sum(x.restart_count or 0 for x in statuses)
    reasons = []
    for s in statuses:
        state = s.state
        waiting = getattr(state, "waiting", None)
        terminated = getattr(state, "terminated", None)
        last = s.last_state.terminated if s.last_state else None
        reason = (waiting.reason if waiting else None) or (terminated.reason if terminated else None) or (last.reason if last else None)
        if reason in {"CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull"}:
            reasons.append(reason)
        if last and last.reason == "OOMKilled":
            reasons.append("OOMKilled")
    phase = pod.status.phase or "Unknown"
    if phase == "Pending":
        age = (datetime.now(timezone.utc) - pod.metadata.creation_timestamp).total_seconds() if pod.metadata.creation_timestamp else 0
        if age >= settings.pending_minutes * 60:
            reasons.append("PendingTooLong")
    if restarts >= settings.restart_threshold:
        reasons.append("RepeatedRestarts")
    if phase == "Failed":
        reasons.append("PodFailed")
    if not reasons:
        return None
    reason = reasons[0]
    logs = []
    for s in statuses[:5]:
        try:
            logs.append(v1.read_namespaced_pod_log(name, ns, container=s.name,
                previous=True, tail_lines=settings.log_tail_lines))
        except ApiException:
            try:
                logs.append(v1.read_namespaced_pod_log(name, ns, container=s.name,
                    tail_lines=settings.log_tail_lines))
            except ApiException:
                pass
    events = []
    try:
        evs = v1.list_namespaced_event(ns, field_selector=f"involvedObject.name={name}", limit=40)
        events = [{"reason": e.reason, "message": e.message, "type": e.type,
                   "count": e.count, "lastTimestamp": str(e.last_timestamp or e.event_time)}
                  for e in evs.items]
    except ApiException:
        pass
    return {"kind":"Pod", "namespace":ns, "name":name,
        "node":pod.spec.node_name or "", "reason":reason,
        "summary":f"Pod {ns}/{name} phase={phase}; restarts={restarts}; detected={','.join(reasons)}",
        "phase":phase, "restart_count":restarts,
        "container_statuses":[{"name":s.name,"ready":s.ready,"restart_count":s.restart_count,
          "state":str(s.state),"last_state":str(s.last_state)} for s in statuses],
        "logs":"\n".join(logs)[:settings.max_log_chars], "events":events}

def pod_watch_loop():
    v1 = client.CoreV1Api()
    while True:
        w = watch.Watch()
        try:
            for event in w.stream(v1.list_pod_for_all_namespaces, timeout_seconds=300):
                pod = event["object"]
                if event["type"] == "DELETED":
                    continue
                ev = _pod_evidence(v1, pod)
                if ev:
                    _save_and_analyze(ev)
        except Exception:
            log.exception("Pod watch disconnected; reconnecting")
            import time; time.sleep(3)
        finally:
            w.stop()

def event_watch_loop():
    v1 = client.CoreV1Api()
    while True:
        w = watch.Watch()
        try:
            for event in w.stream(v1.list_event_for_all_namespaces, timeout_seconds=300):
                obj = event["object"]
                if obj.type != "Warning":
                    continue
                ns = obj.metadata.namespace or ""
                if ns in settings.excluded_namespaces:
                    continue
                reason = obj.reason or "WarningEvent"
                evidence = {"kind":"Event", "namespace":ns,
                    "name":obj.involved_object.name if obj.involved_object else "",
                    "node":obj.source.host if obj.source else "",
                    "reason":reason, "summary":obj.message or reason,
                    "events":[{"reason":reason,"message":obj.message,"type":obj.type,
                               "count":obj.count}]}
                _save_and_analyze(evidence)
        except Exception:
            log.exception("Event watch disconnected; reconnecting")
            import time; time.sleep(3)
        finally:
            w.stop()

def start_watchers():
    load_kube_config()
    import threading
    threading.Thread(target=pod_watch_loop, daemon=True, name="pod-watcher").start()
    threading.Thread(target=event_watch_loop, daemon=True, name="event-watcher").start()
