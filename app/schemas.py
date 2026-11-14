from pydantic import BaseModel
from typing import Any

class IncidentOut(BaseModel):
    id: str
    severity: str
    status: str
    namespace: str
    pod_name: str
    node_name: str
    summary: str
    rca: dict[str, Any] | None
    email_status: str
    occurrences: int
    class Config:
        from_attributes = True
