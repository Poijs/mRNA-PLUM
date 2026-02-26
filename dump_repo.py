from pathlib import Path
from datetime import datetime

ROOT = Path(".").resolve()
OUT = ROOT / "FULL_REPO_DUMP.txt"

EXCLUDE = {
    ".git",
    "__pycache__",
    ".venv",
    ".idea",
    ".pytest_cache",
    "dist",
    "build"
}

def should_skip(path: Path) -> bool:
    return any(part in EXCLUDE for part in path.parts)

with OUT.open("w", encoding="utf-8") as out:
    out.write(f"REPO DUMP\n")
    out.write(f"Generated: {datetime.now()}\n")
    out.write(f"Root: {ROOT}\n\n")

    for path in sorted(ROOT.rglob("*")):
        if path.is_dir():
            continue
        if should_skip(path):
            continue
        
        out.write("\n\n" + "="*80 + "\n")
        out.write(f"{path.relative_to(ROOT)}\n")
        out.write("="*80 + "\n\n")

        try:
            content = path.read_text(encoding="utf-8")
            out.write(content)
        except Exception:
            out.write("[BINARY OR UNREADABLE FILE]\n")

print("FULL_REPO_DUMP.txt został wygenerowany.")