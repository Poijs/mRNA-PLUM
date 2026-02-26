# mRNA-PLUM

System generowania raportów aktywności nauczycieli akademickich
(Excel VBA + Python + DuckDB)

## Architektura

merge-logs
→ parse-events
→ build-activities-state
→ compute-stats
→ export-excel
→ export-individual
→ PDF (VBA)

## Wymagania

- Python 3.13
- Excel z obsługą makr
- Windows 10/11

## Uruchomienie (CLI)

py -m mrna_plum.cli --root .

## PDF

Uruchamiane z Excel VBA (modNA_PdfEngine)
