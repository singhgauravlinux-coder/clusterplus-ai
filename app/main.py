import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db import init_db, SessionLocal, Incident
from app.monitor import start_watchers
from app.schemas import IncidentOut, CloseIncidentRequest
from fastapi.responses import HTMLResponse
from pathlib import Path
from app.dashboard_api import router as dashboard_router

logging.basicConfig(level=logging.INFO)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    start_watchers()
    yield

app = FastAPI(title="Kubernetes AI RCA Agent", version="0.2.0", lifespan=lifespan)
app.include_router(dashboard_router)

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    return (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")

@app.get("/healthz")
def health():
    return {"status":"ok"}

@app.get("/readyz")
def ready():
    return {"status":"ready"}

@app.get("/incidents", response_model=list[IncidentOut])
def incidents(limit: int = 100, db: Session = Depends(get_db)):
    return db.query(Incident).order_by(Incident.created_at.desc()).limit(min(limit, 500)).all()

@app.get("/incidents/{incident_id}", response_model=IncidentOut)
def incident_detail(incident_id: str, db: Session = Depends(get_db)):
    item = db.get(Incident, incident_id)
    if not item:
        raise HTTPException(status_code=404, detail="Incident not found")
    return item

@app.post("/incidents/{incident_id}/close", response_model=IncidentOut)
def close_incident(incident_id: str, payload: CloseIncidentRequest, db: Session = Depends(get_db)):
    item = db.get(Incident, incident_id)

    if not item:
        raise HTTPException(status_code=404, detail="Incident not found")

    if item.status == "CLOSED":
        return item

    from datetime import datetime, timezone
    item.status = "CLOSED"
    item.closure_remark = payload.remark.strip()
    item.closed_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(item)

    logging.info("Incident %s closed by operator", incident_id)
    return item
