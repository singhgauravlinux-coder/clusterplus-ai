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
    rca_status: str
    rca_attempts: int
    rca_next_retry: Any | None
    rca_last_error: str | None
    email_status: str
    occurrences: int
    class Config:
        from_attributes = True
