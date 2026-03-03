from mrna_plum.reports.export_individual import _list_teachers, _fetch_teacher_pers, _detect_hr_columns
from mrna_plum.config import load_config
from pathlib import Path
import duckdb, traceback

root = Path('E:/Skrypty/GRN_v2/mRNA-PLUM')
con = duckdb.connect(str(root / '_data/mrna_plum.duckdb'))
hr_cols = _detect_hr_columns(con)
teachers = _list_teachers(con)

for teacher_id, full_name, email, id_bazus in teachers:
    if not id_bazus:
        print(f'SKIP {teacher_id} - brak id_bazus')
        continue
    try:
        data = _fetch_teacher_pers(con, teacher_id, hr_cols)
        print(f'OK {teacher_id}: {data}')
    except Exception as e:
        print(f'ERROR {teacher_id}:')
        traceback.print_exc()

con.close()
