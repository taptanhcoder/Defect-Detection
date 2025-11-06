from __future__ import annotations
import os
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import deps 
from .routes import router as api_router  

logging.basicConfig(
    level=os.getenv("AOI_LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("aoi.inference_api")


def create_app() -> FastAPI:
    app = FastAPI(
        title="AOI Inference API",
        version="0.1.0",
        docs_url="/docs",
        openapi_url="/openapi.json",
        description="FastAPI nhận ảnh + metadata, chạy model và phát sự kiện inference_results.",
    )


    allow_origins = os.getenv("AOI_CORS_ORIGINS", "")
    if allow_origins:
        origins = [o.strip() for o in allow_origins.split(",") if o.strip()]
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    @app.on_event("startup")
    async def _startup():
        cfg_path = os.getenv("AOI_INFER_CONFIG", "configs/inference.yaml")
        project_root = os.getenv("AOI_PROJECT_ROOT", ".")
        deps.init(config_path=cfg_path, project_root=project_root)
        log.info("Inference API started.")

    @app.on_event("shutdown")
    async def _shutdown():
        deps.shutdown()
        log.info("Inference API stopped.")

    app.include_router(api_router, prefix="")
    return app


app = create_app()
