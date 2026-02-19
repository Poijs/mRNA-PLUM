from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from rna_plum.io.csv_read import detect_csv_dialect, iter_csv_rows_streaming


def merge_logs_to_parquet_streaming(
    input_files: list[Path],
    parquet_out: Path,
    *,
    dedup_per_file: bool = True,
    row_group_size: int = 50_000,
) -> int:
    """
    Streamingowy zapis do Parquet:
    - nie używa pandas
    - nie trzyma wszystkich danych w RAM
    - dedup per plik (opcjonalnie)
    """
    parquet_out.parent.mkdir(parents=True, exist_ok=True)

    writer: Optional[pq.ParquetWriter] = None
    total_rows = 0

    try:
        for fp in input_files:
            dialect = detect_csv_dialect(fp)

            # dedup tylko w obrębie tego pliku (set hashy resetowany per plik)
            seen: set[str] = set() if dedup_per_file else set()

            header_current: Optional[list[str]] = None
            batch_cols: dict[str, list[str]] = {}
            batch_cols["_source_file"] = []

            for header, row in iter_csv_rows_streaming(fp, dialect=dialect):
                if header_current is None:
                    header_current = header
                    # przygotuj kolumny na ten header
                    for h in header_current:
                        batch_cols.setdefault(h, [])

                # hash całego wiersza po trim (reader już trimuje)
                if dedup_per_file:
                    key = "\x1f".join(row)
                    if key in seen:
                        continue
                    seen.add(key)

                # dopisz wartości (jeśli row krótszy -> "")
                for i, h in enumerate(header):
                    batch_cols[h].append(row[i] if i < len(row) else "")
                batch_cols["_source_file"].append(str(fp))

                # flush row-group
                if len(batch_cols["_source_file"]) >= row_group_size:
                    table = pa.table(batch_cols)
                    if writer is None:
                        writer = pq.ParquetWriter(parquet_out, table.schema)
                    writer.write_table(table)
                    total_rows += table.num_rows
                    # reset batch
                    batch_cols = {k: [] for k in table.schema.names}

            # flush reszty dla pliku
            if header_current is not None and len(batch_cols["_source_file"]) > 0:
                table = pa.table(batch_cols)
                if writer is None:
                    writer = pq.ParquetWriter(parquet_out, table.schema)
                writer.write_table(table)
                total_rows += table.num_rows

        # jeśli nie było żadnych danych -> pusty parquet
        if writer is None:
            empty = pa.table({})
            pq.write_table(empty, parquet_out)

        return total_rows

    finally:
        if writer is not None:
            writer.close()
