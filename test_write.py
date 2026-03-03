from mrna_plum.reports.export_individual import _fetch_teacher_pers, _fetch_teacher_rows, _write_teacher_xlsx, _detect_hr_columns, sanitize_filename
from pathlib import Path
import duckdb, traceback

root = Path('E:/Skrypty/GRN_v2/mRNA-PLUM')
con = duckdb.connect(str(root / '_data/mrna_plum.duckdb'))
hr_cols = _detect_hr_columns(con)
out_dir = root / '_out/indywidualne'
out_dir.mkdir(parents=True, exist_ok=True)

teacher_id, full_name, email, id_bazus = '7111', 'Monika Michalak', 'monika.michalak@umw.edu.pl', '2426'
try:
    pers = _fetch_teacher_pers(con, teacher_id, hr_cols)
    rows = _fetch_teacher_rows(con, teacher_id)
    safe_name = sanitize_filename(full_name)
    out_file = out_dir / f'{safe_name}_{id_bazus}.xlsx'
    n = _write_teacher_xlsx(out_file, teacher_id, full_name, rows, pers, hr_cols)
    print('OK:', out_file, 'rows:', n)
except Exception as e:
    traceback.print_exc()

con.close()
