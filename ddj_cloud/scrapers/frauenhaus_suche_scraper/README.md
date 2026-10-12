# frauenhaus-suche-scraper

**Contact:** Leonhard Eckwert (Leonhard.Eckwert@fm.wdr.de)

Speichert den Status aller Frauenhäuser in der bundesweiten Frauenhaussuche der Zentralen Informationsstelle Frauenhäuser.

## Reference HTML snapshot

[2026-10-12.html](../../../docs/snapshots/frauenhaus_suche_scraper/2026-10-12.html)
contains the HTML downloaded from https://www.frauenhaus-suche.de/ on 2026-10-12
during the scraper review. It contains 352 shelter entries and passes the parser's
validation without warnings. Keep it as a baseline for investigating future
changes to the source page or parser. Snapshots live under `docs/snapshots` so they
are excluded from the Lambda deployment package.

To parse the snapshot offline, run this from the repository root:

```sh
uv run python - <<'PY'
import os
from pathlib import Path

os.environ["USE_LOCAL_STORAGE"] = "1"

from ddj_cloud.scrapers.frauenhaus_suche_scraper.frauenhaus_suche_scraper import (
    parse_page,
    validate,
)

snapshot = Path("docs/snapshots/frauenhaus_suche_scraper/2026-10-12.html")
records, announced = parse_page(snapshot.read_text(encoding="utf-8"))
fatal, warnings = validate(records, announced)
print(f"Parsed {len(records)} records; announced count: {announced}")
print("Fatal problems:", fatal)
print("Warnings:", warnings)
PY
```

This calls the parser and validator directly, without downloading a page or
uploading results.
