import os
import aiofiles
from wcmatch import glob
from loguru import logger
from pathlib import PurePosixPath
from opendal import AsyncOperator
from typing import Optional, AsyncGenerator

from app.config import config

from app.Services.storage.base import BaseStorage, FileMetaDataT, RemoteFilePathType, LocalFilePathType, \
    LocalFileMetaDataType, RemoteFileMetaDataType


class S3Storage(BaseStorage[FileMetaDataT: None]):
    def __init__(self):
        self.static_dir = config.storage.s3.path
        self.bucket = config.storage.s3.bucket
        self.region = config.storage.s3.region
        self.endpoint = config.storage.s3.endpoint_url
        self._access_key_id = config.storage.s3.access_key_id
        self._secret_access_key = config.storage.s3.secret_access_key
        self.resolve_path = lambda x: str(PurePosixPath(x))
        self.op = AsyncOperator("s3",
                                root=self.resolve_path(self.static_dir),
                                bucket=self.bucket,
                                region=self.region,
                                endpoint=self.endpoint,
                                access_key_id=self._access_key_id,
                                secret_access_key=self._secret_access_key)

    async def url(self,
                  remote_file: "RemoteFilePathType") -> str:
        _presign = await self.op.presign_read(remote_file, 3600)
        return _presign.url

    async def upload(self,
                     local_file: "LocalFilePathType",
                     remote_file: "RemoteFilePathType") -> None:
        async with aiofiles.open(local_file, "rb") as f:
            b = await f.read()
        await self.op.write(remote_file, b)
        logger.success(f"Successfully uploaded file {str(local_file)} to {str(remote_file)} via s3_storage.")

    async def rename(self,
                     old_remote_file: "RemoteFilePathType",
                     new_remote_file: "RemoteFilePathType") -> None:
        await self.op.copy(old_remote_file, new_remote_file)
        await self.op.delete(old_remote_file)
        logger.success(f"Successfully renamed file {str(old_remote_file)} to {str(new_remote_file)} via s3_storage.")

    async def update_metadata(self,
                              local_file_metadata: "LocalFileMetaDataType",
                              remote_file_metadata: "RemoteFileMetaDataType") -> None:
        raise NotImplementedError

    async def delete(self,
                     remote_file: "RemoteFilePathType") -> None:
        await self.op.copy(remote_file, f"_deleted/{remote_file}")
        await self.op.delete(remote_file)
        logger.success(f"Successfully deleted file {str(remote_file)} via s3_storage.")

    async def list_files(self,
                         path: RemoteFilePathType,
                         pattern: Optional[str] = "*",
                         batch_max_files: Optional[int] = None,
                         valid_extensions: Optional[set[str]] = None) \
            -> AsyncGenerator[list[RemoteFilePathType], None]:
        """
        # TODO: rewrite this function after https://github.com/apache/opendal/issues/3960 resolved
        As of 2024/2/1, opendal does not recursively list all files under a folder, but only subfolders.
        Based on this project, this function currently filters subfolders.
        (see https://github.com/apache/opendal/issues/3960)
        """
        if valid_extensions is None:
            valid_extensions = {'.jpg', '.png', '.jpeg', '.jfif', '.webp', '.gif'}
        ls = await self.op.list(path)
        files = []
        async for itm in ls:
            if self._list_files_check(itm.path, pattern, valid_extensions):
                files.append(PurePosixPath(itm.path))
                if batch_max_files is not None and len(files) == batch_max_files:
                    yield files
                    files = []
        if files:
            yield files

    @staticmethod
    def _list_files_check(x: str, pattern: str, valid_extensions: Optional[set[str]] = None) -> bool:
        matches_pattern = glob.globmatch(x, pattern, flags=glob.GLOBSTAR)
        has_valid_extension = os.path.splitext(x)[-1] in valid_extensions
        is_not_directory = not x.endswith("/")
        return matches_pattern and has_valid_extension and is_not_directory


if __name__ == '__main__':
    import pathlib


    async def test():
        await storage.upload(local_file=pathlib.Path(r"C:\Users\pk5ls\Desktop\test.png"), remote_file="1.jpg")
        await storage.upload(local_file=pathlib.Path(r"C:\Users\pk5ls\Desktop\test.png"), remote_file="2.jpg")
        await storage.rename(old_remote_file="1.jpg", new_remote_file="2.jpg")
        await storage.delete(remote_file="2.jpg")
        async for batch in storage.list_files(path="", batch_max_files=200, valid_extensions={".txt", ".png"}):
            print(f"Batch: {batch}")
        # for idx, itm in enumerate(glob("C:/Users/pk5ls/Pictures/Screenshots/*")):
        #     await storage.upload(local_file=Path(itm), remote_file=f"up50/{idx}.png")


    import asyncio

    storage = S3Storage()
    asyncio.run(test())
