from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, params
from loguru import logger

from app.Models.api_models.admin_api_model import ImageOptUpdateModel
from app.Models.api_response.admin_api_response import ServerInfoResponse
from app.Models.api_response.base import NekoProtocol
from app.Services.authentication import force_admin_token_verify
from app.Services.provider import db_context
from app.Services.storage import storage_service
from app.Services.storage.exception import RemoteFileNotFoundError
from app.Services.vector_db_context import PointNotFoundError

admin_router = APIRouter(dependencies=[Depends(force_admin_token_verify)], tags=["Admin"])


@admin_router.delete("/delete/{image_id}",
                     description="Delete image with the given id from database. "
                                 "If the image is a local image, it will be moved to `/static/_deleted` folder.")
async def delete_image(
        image_id: Annotated[UUID, params.Path(description="The id of the image you want to delete.")]) -> NekoProtocol:
    try:
        point = await db_context.retrieve_by_id(str(image_id))
    except PointNotFoundError as ex:
        raise HTTPException(404, "Cannot find the image with the given ID.") from ex
    await db_context.deleteItems([str(point.id)])
    logger.success("Image {} deleted from database.", point.id)
    # local url to filename   e.g /static/xxx.jpg -> xxx.jpg
    if point.thumbnail_url.startswith("/"):
        point.thumbnail_url = point.thumbnail_url.rsplit('/', 1)[-1]
    try:
        if point.thumbnail_url:
            await storage_service.delete(f"thumbnails/{point.thumbnail_url}")
        await storage_service.delete(point.thumbnail_url)
    except RemoteFileNotFoundError as ex:
        logger.error("Error occurred when deleting image: {}", ex)
        raise HTTPException(404, str(ex)) from ex
    except Exception as ex:
        logger.error("Error occurred when deleting image: {}", ex)
        raise HTTPException(500, "Error occurred when deleting image.") from ex
    return NekoProtocol(message="Image deleted.")


@admin_router.put("/update_opt/{image_id}", description="Update a image's optional information")
async def update_image(image_id: Annotated[UUID, params.Path(description="The id of the image you want to delete.")],
                       model: ImageOptUpdateModel) -> NekoProtocol:
    if model.empty():
        raise HTTPException(422, "Nothing to update.")
    try:
        point = await db_context.retrieve_by_id(str(image_id))
    except PointNotFoundError as ex:
        raise HTTPException(404, "Cannot find the image with the given ID.") from ex

    if model.starred is not None:
        point.starred = model.starred
    if model.categories is not None:
        point.categories = model.categories

    await db_context.updatePayload(point)
    logger.success("Image {} updated.", point.id)

    return NekoProtocol(message="Image updated.")


@admin_router.get("/server_info", description="Get server information")
async def server_info():
    return ServerInfoResponse(message="Successfully get server information!",
                              image_count=await db_context.get_counts(exact=True))
