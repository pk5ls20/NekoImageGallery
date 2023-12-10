from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.Controllers.admin import admin_router
from app.Controllers.search import searchRouter
from fastapi.staticfiles import StaticFiles
from datetime import datetime
from app.config import config
from .Models.api_response.base import WelcomeApiResponse
from .util.fastapi_log_handler import init_logging

app = FastAPI()
init_logging()

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(searchRouter, prefix="/search")
if config.admin_api_enable:
    app.include_router(admin_router, prefix="/admin")

if config.static_file.enable:
    app.mount("/static", StaticFiles(directory=config.static_file.path), name="static")


@app.get("/", description="Default portal. Test for server availability.")
def welcome() -> WelcomeApiResponse:
    return WelcomeApiResponse(
        message="Ciallo~ Welcome to NekoImageGallery API!",
        server_time=datetime.now(),
        wiki={
            "openAPI": "/openapi.json",
            "swagger UI": "/docs",
            "redoc": "/redoc"
        }
    )
