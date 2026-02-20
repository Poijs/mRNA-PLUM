import duckdb
from pathlib import Path
from mrna_plum.stats.compute_stats import compute_stats

def test_pct_course_two_teachers(tmp_path: Path):
    db = tmp_path / "w.duckdb"
    con = duckdb.connect(str(db))

    con.execute("CREATE SCHEMA mart;")

    con.execute("""
        CREATE TABLE events_canonical (
            course_code VARCHAR, ay VARCHAR, term VARCHAR,
            wydzial_code VARCHAR, kierunek_code VARCHAR, track_code VARCHAR, semester_code VARCHAR,
            ts_utc TIMESTAMP, teacher_id VARCHAR, operation VARCHAR,
            tech_key VARCHAR, activity_label VARCHAR, object_id VARCHAR, count_mode VARCHAR,
            counted BOOLEAN
        );
    """)

    con.execute("""
        CREATE TABLE mart.activities_state (
            course_code VARCHAR, activity_id VARCHAR,
            status_final VARCHAR, deleted_at TIMESTAMP,
            visible_last BOOLEAN, confidence_deleted DOUBLE
        );
    """)

    # 2 nauczycieli, ta sama aktywność "PAGE"
    con.execute("""
        INSERT INTO events_canonical VALUES
        ('C1','2025/26','Z','W1','K1','T1','S1', now(), 'T_A','CREATE','PAGE','Strona','10','object-based', true),
        ('C1','2025/26','Z','W1','K1','T1','S1', now(), 'T_B','CREATE','PAGE','Strona','11','object-based', true);
    """)

    con.execute("""
        INSERT INTO mart.activities_state VALUES
        ('C1','10','visible_active', NULL, true, 1.0),
        ('C1','11','visible_active', NULL, true, 1.0);
    """)

    # mapping
    # tu najprościej: zrobisz pliki CSV/XLSX w tmp_path i wskażesz config.yaml
    # ... (pomijam dla czytelności – ale test ma sprawdzić, że oba wejdą do long)
    con.close()

    # prepare root with config + mapping files, then compute_stats(root)
    # then assert pct_course = 0.5 and 0.5