from mrna_plum.reports.export_individual import export_individual_reports
from mrna_plum.config import load_config
from pathlib import Path
import duckdb

root = Path('E:/Skrypty/GRN_v2/mRNA-PLUM')
cfg = load_config(root / 'config.yaml')

# Ustaw root w config jak robi cli_export_individual
if isinstance(cfg, dict):
    cfg['root'] = str(root)
else:
    setattr(cfg, 'root', str(root))

con = duckdb.connect(str(root / '_data/mrna_plum.duckdb'))
code, out = export_individual_reports(con, cfg)
print('code:', code, 'out:', out)
con.close()
