from __future__ import annotations

from pathlib import Path
from typing import Iterator


class TransformedFolderSource:
    def __init__(self, sink_dir: Path) -> None:
        self.sink_dir = sink_dir

    def iter_document_dirs(self) -> Iterator[Path]:
        if not self.sink_dir.exists():
            return

        for path in sorted(self.sink_dir.iterdir()):
            if not path.is_dir():
                continue
            if (path / "metadata.json").exists():
                yield path
