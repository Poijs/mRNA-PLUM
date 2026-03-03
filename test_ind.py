from mrna_plum.reports.export_individual import export_individual_reports, _list_teachers
from mrna_plum.config import load_config
from pathlib import Path
import duckdb

root = Path('E:/Skrypty/GRN_v2/mRNA-PLUM')
cfg = load_config(root / 'config.yaml')
con = duckdb.connect(str(root / '_data/mrna_plum.duckdb'))

teachers = _list_teachers(con)
print('Teachers:', teachers)

con.close()
