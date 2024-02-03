import pathlib
from shutil import copy2
from asyncio import to_thread
from typing import Optional, AsyncGenerator
from pathlib import Path as syncPath
from aiopath import Path as asyncPath
import aiofiles
from loguru import logger

from app.config import config
from app.Services.storage.base import BaseStorage, FileMetaDataT, RemoteFilePathType, LocalFilePathType
from app.Services.storage.exception import RemoteFileNotFoundError, LocalFileNotFoundError


class LocalStorage(BaseStorage[FileMetaDataT: None]):
    def __init__(self):
        self.static_dir = syncPath(config.storage.local.path)
        self.thumbnails_dir = self.static_dir / "thumbnails"
        self.deleted_dir = self.static_dir / "_deleted"
        self.file_metadata = None
        if not self.static_dir.is_dir():
            self.static_dir.mkdir(parents=True)
            logger.warning(f"static_dir {self.static_dir} not found, created.")
        if not self.thumbnails_dir.is_dir():
            self.thumbnails_dir.mkdir(parents=True)
            logger.warning(f"thumbnails_dir {self.thumbnails_dir} not found, created.")
        if not self.deleted_dir.is_dir():
            self.deleted_dir.mkdir(parents=True)
            logger.warning(f"deleted_dir {self.deleted_dir} not found, created.")

    # noinspection PyMethodMayBeStatic
    async def url(self,
                  remote_file: "RemoteFilePathType") -> str:
        return f"/static/{str(remote_file)}"

    async def upload(self,
                     local_file: "LocalFilePathType",
                     remote_file: "RemoteFilePathType") -> None:
        try:
            remote_file = self.static_dir / syncPath(remote_file)
            if isinstance(local_file, bytes):
                async with aiofiles.open(str(remote_file), 'wb') as file:
                    await file.write(local_file)
            else:
                await to_thread(copy2, str(local_file), str(remote_file))
        except FileNotFoundError as ex:
            raise LocalFileNotFoundError from ex
        logger.success(f"Successfully uploaded file {str(local_file)} to {str(remote_file)} via local_storage.")

    async def rename(self,
                     old_remote_file: "RemoteFilePathType",
                     new_remote_file: "RemoteFilePathType") -> None:
        old_remote_file = self.static_dir / syncPath(old_remote_file)
        new_remote_file = self.static_dir / syncPath(new_remote_file)
        if not old_remote_file.is_file():
            raise RemoteFileNotFoundError(f"Remote file not found: {old_remote_file}")
        await to_thread(old_remote_file.rename, new_remote_file)
        logger.success(f"Successfully renamed file {str(old_remote_file)} to {str(new_remote_file)} via local_storage.")

    async def update_metadata(self,
                              local_file_metadata: None,
                              remote_file_metadata: None) -> None:
        raise NotImplementedError

    async def delete(self,
                     remote_file: "RemoteFilePathType") -> None:
        remote_path = syncPath(remote_file) if isinstance(remote_file, str) else remote_file
        static_file = self.static_dir / remote_path
        delete_file = self.deleted_dir / remote_path
        if not static_file.is_file():
            raise RemoteFileNotFoundError(f"File not found: {static_file}")
        await to_thread(copy2, str(static_file), str(delete_file))
        logger.success(f"Successfully deleted file {str(remote_file)} via local_storage.")

    # noinspection PyMethodMayBeStatic
    async def list_files(self,
                         path: RemoteFilePathType,
                         pattern: Optional[str] = "*",
                         batch_max_files: Optional[int] = None,
                         valid_extensions: Optional[set[str]] = None) \
            -> AsyncGenerator[list[RemoteFilePathType], None]:
        _path = asyncPath(path)
        files = []
        async for file in _path.glob(pattern):
            if file.suffix.lower() in valid_extensions:
                files.append(syncPath(file))
                if batch_max_files is not None and len(files) == batch_max_files:
                    yield files
                    files = []
        if files:
            yield files


if __name__ == '__main__':

    async def test():
        await storage.upload(local_file=syncPath(r"C:\Users\pk5ls\Desktop\test.png"), remote_file="1.jpg")
        await storage.rename(old_remote_file="1.jpg", new_remote_file="2.jpg")
        await storage.delete(remote_file="2.jpg")
        async for file in storage.list_files(path=pathlib.Path(r"C:\Users\pk5ls\Desktop\another_bert_test_2"),
                                             pattern="*",
                                             batch_max_files=100,
                                             valid_extensions={".jpg", ".png"}):
            print(len(file), file)

        # for idx, itm in enumerate(glob("C:/Users/pk5ls/Pictures/Screenshots/*")):
        #     await storage.upload(local_file=Path(itm), remote_file=f"up21/{idx}.png")


    import asyncio

    storage = LocalStorage()
    asyncio.run(test())
