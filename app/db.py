from sqlalchemy import create_engine, String, Text, DateTime, Integer, JSON, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from app.config import settings

engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class Incident(Base):
    __tablename__ = "incidents"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String(512), index=True)
    severity: Mapped[str] = mapped_column(String(16), index=True)
    status: Mapped[str] = mapped_column(String(24), default="OPEN")
    namespace: Mapped[str] = mapped_column(String(253), default="")
    pod_name: Mapped[str] = mapped_column(String(253), default="")
    node_name: Mapped[str] = mapped_column(String(253), default="")
    summary: Mapped[str] = mapped_column(Text, default="")
    evidence: Mapped[dict] = mapped_column(JSON, default=dict)
    rca: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    email_status: Mapped[str] = mapped_column(String(24), default="PENDING")
    occurrences: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

def init_db():
    Base.metadata.create_all(engine)
