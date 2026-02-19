from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from mrna_plum.store.duckdb_store import open_store
from mrna_plum.merge.merge_logs import merge_logs_to_parquet


def _write_csv_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _make_logs_name(course: str, ts: str) -> str:
    return f"logs_{course}_{ts}.csv"


def test_bom_utf8_and_delimiter_detection_and_insert(tmp_path: Path):
    root = tmp_path / "logs"
    db = tmp_path / "db.duckdb"

    # UTF-8 with BOM, delimiter ';'
    content = (
        "\ufeffCzas;Akcja;Opis\n"
        "2026-02-18 10:00:00;ADD;Zażółć gęślą\n"
    ).encode("utf-8")

    _write_csv_bytes(root / _make_logs_name("KURS1", "20260218-1000"), content)

    con = open_store(db)
    try:
        res = merge_logs_into_duckdb(root=root, con=con, export_mode="duckdb")
        assert res.courses == 1
        assert res.files == 1
        assert res.inserted_rows == 1

        row = con.execute(
            "SELECT course, payload_json FROM events_raw WHERE course='KURS1'"
        ).fetchone()
        assert row[0] == "KURS1"
        assert "Zażółć gęślą" in row[1]
    finally:
        con.close()


def test_windows_1250_polish_chars(tmp_path: Path):
    root = tmp_path / "logs"
    db = tmp_path / "db.duckdb"

    # cp1250, delimiter TAB
    text = "Czas\tAkcja\tOpis\n2026-02-18 10:00:00\tADD\tŚliwka w kompot\n"
    _write_csv_bytes(root / _make_logs_name("KURS2", "20260218-1000"), text.encode("cp1250"))

    con = open_store(db)
    try:
        res = merge_logs_into_duckdb(root=root, con=con, export_mode="duckdb")
        assert res.inserted_rows == 1

        payload = con.execute(
            "SELECT payload_json FROM events_raw WHERE course='KURS2'"
        ).fetchone()[0]
        assert "Śliwka w kompot" in payload
    finally:
        con.close()


def test_dedup_identical_whole_row_after_trim(tmp_path: Path):
    root = tmp_path / "logs"
    db = tmp_path / "db.duckdb"

    # dwa pliki, ten sam wiersz różniący się tylko spacingiem -> po trim ma być 1 wpis
    c1 = (
        "Czas;Akcja;Opis\n"
        "2026-02-18 10:00:00;ADD;  Duplikat  \n"
    ).encode("utf-8")
    c2 = (
        "Czas;Akcja;Opis\n"
        "2026-02-18 10:00:00;ADD;Duplikat\n"
    ).encode("utf-8")

    _write_csv_bytes(root / _make_logs_name("KURS3", "20260218-1000"), c1)
    _write_csv_bytes(root / _make_logs_name("KURS3", "20260218-1001"), c2)

    con = open_store(db)
    try:
        res = merge_logs_into_duckdb(root=root, con=con, export_mode="duckdb")
        assert res.inserted_rows == 1

        cnt = con.execute("SELECT COUNT(*) FROM events_raw WHERE course='KURS3'").fetchone()[0]
        assert cnt == 1
    finally:
        con.close()


def test_sorting_desc_by_time_on_export_csv(tmp_path: Path):
    root = tmp_path / "logs"
    db = tmp_path / "db.duckdb"
    out_dir = tmp_path / "out"

    # kolejność w pliku: starszy potem nowszy (celowo)
    content = (
        "Czas;Akcja;Opis\n"
        "2026-02-18 09:00:00;ADD;A\n"
        "2026-02-18 10:00:00;ADD;B\n"
    ).encode("utf-8")

    _write_csv_bytes(root / _make_logs_name("KURS4", "20260218-1000"), content)

    con = open_store(db)
    try:
        res = merge_logs_into_duckdb(root=root, con=con, export_mode="csv", export_dir=out_dir)
        assert res.inserted_rows == 2

        out_csv = out_dir / "KURS4_full_log.csv"
        assert out_csv.exists()

        # W eksporcie ma być B przed A (czas malejąco)
        rows = out_csv.read_text(encoding="utf-8").splitlines()
        # Header w COPY jest: course;time_text;time_ts;payload_json;source_file
        assert len(rows) >= 3
        assert '"B"' in rows[1] or "B" in rows[1]
        assert '"A"' in rows[2] or "A" in rows[2]
    finally:
        con.close()
