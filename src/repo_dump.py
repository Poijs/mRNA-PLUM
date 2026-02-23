from pathlib import Path

ROOT = Path(".")
OUT = Path("FULL_REPO_DUMP.txt")

EXCLUDE = {".git", "__pycache__", ".venv", ".idea"}

with OUT.open("w", encoding="utf-8") as out:
    for path in sorted(ROOT.rglob("*")):
        if path.is_dir():
            continue
        
        if any(part in EXCLUDE for part in path.parts):
            continue
        
        out.write(f"\n\n{'='*80}\n")
        out.write(f"{path.relative_to(ROOT)}\n")
        out.write(f"{'='*80}\n\n")
        
        try:
            out.write(path.read_text(encoding="utf-8"))
        except Exception:
            out.write("[BINARY OR UNREADABLE FILE]\n")

print("Zrobione.")