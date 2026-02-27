# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['src\\mrna_plum\\cli.py'],
    pathex=['src'],
    binaries=[],
    datas=[],
    hiddenimports=[
        'tzdata',
        'duckdb',
        'mrna_plum.merge.merge_logs',
        'mrna_plum.store.duckdb_store',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='mrna-plum',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,   # ← ZMIANA
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
