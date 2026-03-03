from mrna_plum.reports.export_individual import export_individual_reports, _list_teachers, _worker_export_one
from mrna_plum.config import load_config
from pathlib import Path
import duckdb

root = Path('E:/Skrypty/GRN_v2/mRNA-PLUM')
cfg = load_config(root / 'config.yaml')
con = duckdb.connect(str(root / '_data/mrna_plum.duckdb'))

# Testuj jednego nauczyciela z id_bazus
result = _worker_export_one(
    con=con,
    config=cfg,
    teacher_id='7111',
    full_name='Monika Michalak',
    email='monika.michalak@umw.edu.pl',
    id_bazus='2426',
    out_dir=str(root / '_out/indywidualne'),
    hr_cols=[],
    db_path=str(root / '_data/mrna_plum.duckdb'),
)
print('Result:', result)
con.close()
