class StorageExtension(Exception):
    pass


class LocalFileNotFoundError(StorageExtension):
    pass


class LocalFileExistsError(StorageExtension):
    pass


class RemoteFileNotFoundError(StorageExtension):
    pass


class RemoteFileExistsError(StorageExtension):
    pass


class RemotePermissionError(StorageExtension):
    pass


class RemoteConnectError(StorageExtension):
    pass
