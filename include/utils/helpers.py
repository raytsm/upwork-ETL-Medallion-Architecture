import hashlib
from pathlib import Path

def file_hash(path:Path) -> str:
    with path.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256")
    return digest.hexdigest()