from mrna_plum.reports.export_individual import export_individual_reports
from mrna_plum.config import load_config
from pathlib import Path
import duckdb, traceback

root = Path('E:/Skrypty/GRN_v2/mRNA-PLUM')
cfg = load_config(root / 'config.yaml')
con = duckdb.connect(str(root / '_data/mrna_plum.duckdb'))
try:
    code, out = export_individual_reports(con, cfg)
    print('code:', code)
    if isinstance(out, dict):
        for k, v in out.items():
            print(f'  {k}: {v}')
    else:
        print('out:', out)
except Exception as e:
    traceback.print_exc()
finally:
    con.close()
