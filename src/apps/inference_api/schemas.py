from __future__ import annotations
from typing import Optional, List, Dict
from pydantic import BaseModel, Field, ConfigDict


class InferRequestMeta(BaseModel):
    product_code: str = Field(..., min_length=1, description="Mã sản phẩm/board")
    station_id: str = Field(..., min_length=1, description="Mã trạm AOI (map tới runner)")
    board_serial: Optional[str] = Field(None, description="Số serial/lot (tùy chọn)")

    model_config = ConfigDict(extra="forbid")


class DefectBBox(BaseModel):
    x: int
    y: int
    w: int
    h: int


class DefectItem(BaseModel):
    cls: str
    score: float
    bbox: DefectBBox
    mask_url: Optional[str] = None


class InferResponse(BaseModel):
    event_id: str
    aql_mini_decision: str
    overlay_url: str
    defect_count: int
    latency_ms: int

    product_code: str
    station_id: str
    model_family: str
    model_version: str

    defects_preview: Optional[List[DefectItem]] = None


class HealthzResponse(BaseModel):
    status: str = "ok"
    kafka: str = "mock"  
    minio: str = "unknown"
    details: Dict[str, str] = {}
