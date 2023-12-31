from pathlib import Path

from loguru import logger


def gather_valid_files(root: Path, pattern: str = '**/*.*'):
    for item in root.glob(pattern):
        if item.suffix in ['.jpg', '.png', '.jpeg', '.jfif', '.webp', '.gif']:
            yield item
        else:
            logger.warning("Unsupported file type: {}. Skip...", item.suffix)