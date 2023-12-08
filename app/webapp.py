from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.Controllers.files import filesRouter
from app.Controllers.search import searchRouter
from fastapi.staticfiles import StaticFiles
from time import time
import app.config as config

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(filesRouter, prefix="/files")
app.include_router(searchRouter, prefix="/search")

if config.STATIC_FILE_ENABLE:
    app.mount("/static", StaticFiles(directory=config.STATIC_FILE_PATH), name="static")


@app.get("/")
def welcome():
    return {
        "serverTime": time(),
        "message": "Ciallo~ Welcome to NekoImageGallery API",
        "docs": "/docs"
    }