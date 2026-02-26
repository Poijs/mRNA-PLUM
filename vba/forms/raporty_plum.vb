' === Component: raporty_plum [UserForm]
' === Exported: 2026-02-26 14:22:22

Option Explicit

Private Sub Label3_Click()

End Sub

Private Sub UserForm_Initialize()

    Me.caption = "mRNA-PLUM — Raporty Nauczycieli Akademickich"

    ' wczytaj ostatnie ścieżki
    Me.txtLogsFolder.text = GetSetting("mRNA-PLUM", "Paths", "LogsFolder", "")
    Me.txtTemplateXlsx.text = ""

    ' KEYS: pamiętaj wybór, a jak brak to domyślnie root\_data\KEYS.xlsx
    Dim defKeys As String
    defKeys = ThisWorkbook.path & "\_data\KEYS.xlsx"

    Me.txtKeysXlsx.text = GetSetting("mRNA-PLUM", "Paths", "KeysXlsx", defKeys)
    Me.txtInputsDir.text = GetSetting("mRNA-PLUM", "Paths", "InputsDir", ThisWorkbook.path & "\_data\inputs")

    ResetProgress
    Me.LabelStatus.caption = "Gotowy."

End Sub


' =========================================================
' WYBÓR FOLDERU LOGÓW
' =========================================================
Private Sub btnPickLogsFolder_Click()

    Dim p As String
    p = modNA_UIInputs.PickFolderDialog( _
            "Wskaż folder nadrzędny z logami (z podfolderami)", _
            Me.txtLogsFolder.text)

    If Len(p) > 0 Then
        Me.txtLogsFolder.text = p
    End If

End Sub


' =========================================================
' WYBÓR TEMPLATE
' =========================================================
Private Sub btnPickTemplate_Click()

    Dim p As String
    p = modNA_UIInputs.PickFileDialog( _
            "Wskaż wzór raportu (Excel)", _
            "Excel", "*.xlsx", "")

    If Len(p) > 0 Then
        Me.txtTemplateXlsx.text = p
    End If

End Sub


' =========================================================
' START PIPELINE
' =========================================================
Private Sub btnStart_Click()

    Dim errMsg As String

    If Not modNA_UIInputs.UI_ValidateInputs(errMsg) Then
        MsgBox errMsg, vbExclamation
        Exit Sub
    End If

    ' Zapamiętaj folder logów
    SaveSetting "mRNA-PLUM", "Paths", "LogsFolder", Me.txtLogsFolder.text
    SaveSetting "mRNA-PLUM", "Paths", "KeysXlsx", Me.txtKeysXlsx.text
    SaveSetting "mRNA-PLUM", "Paths", "InputsDir", Me.txtInputsDir.text

    Me.btnStart.enabled = False
    Me.LabelStatus.caption = "Inicjalizacja..."

    modNA_Launcher.StartPipeline

End Sub


' =========================================================
' ZAMKNIJ
' =========================================================
Private Sub btnClose_Click()
    Unload Me
End Sub


' =========================================================
' PUBLIC — wywoływane z Launchera
' =========================================================

Public Sub SetStatus(ByVal txt As String)
    Me.LabelStatus.caption = txt
End Sub

Public Sub SetProgress(ByVal pct As Double, ByVal txt As String)

    If pct < 0 Then pct = 0
    If pct > 1 Then pct = 1

    Me.LabelBar.Width = Me.FrameBar.Width * pct
    Me.LabelPct.caption = Format(pct, "0%")
    Me.LabelStatus.caption = txt

End Sub

Public Sub ResetProgress()
    Me.LabelBar.Width = 0
    Me.LabelPct.caption = "0%"
End Sub

Public Sub SetRunning(ByVal running As Boolean)
    Me.btnStart.enabled = Not running
End Sub

Private Sub btnPickKeys_Click()

    Dim p As String
    p = modNA_UIInputs.PickFileDialog( _
            "Wskaż plik KEYS.xlsx (reguły parsowania)", _
            "Excel", "*.xlsx", _
            Me.txtKeysXlsx.text)

    If Len(p) > 0 Then
        Me.txtKeysXlsx.text = p
    End If

End Sub
Private Sub btnConvertXlsx_Click()
    On Error GoTo EH

    Dim src As String
    src = Trim$(Me.txtInputsDir.text) ' albo osobny textbox np. txtXlsxSourceFolder

    Dim fso As Object
    Set fso = CreateObject("Scripting.FileSystemObject")

    ' normalizacja (często ratuje przy kopiuj/wklej)
    src = Replace$(src, """", "")
    src = Trim$(src)
    If Right$(src, 1) = "\" Then src = Left$(src, Len(src) - 1)

    If Len(src) = 0 Or Not fso.FolderExists(src) Then
        src = modNA_UIInputs.PickFolderDialog( _
                "Wskaż folder z XLSX do konwersji (z podfolderami)", _
                ThisWorkbook.path)
        If Len(src) = 0 Then Exit Sub
        Me.txtInputsDir.text = src
    End If

    Me.LabelStatus.caption = "Konwersja XLSX › CSV..."
    DoEvents

    modNA_Convert.ConvertXlsxFolderToCsv src, ThisWorkbook.path

    Me.LabelStatus.caption = "Konwersja zakończona."
    Exit Sub

EH:
    Me.LabelStatus.caption = "Błąd konwersji XLSX › CSV."
    MsgBox "Błąd: " & Err.Description, vbExclamation
End Sub
Private Sub btnMergeTeacherIdCsv_Click()
    On Error GoTo EH

    Me.LabelStatus.caption = "Scalanie CSV... wybierz 2 pliki."
    DoEvents

    Call MergeCsv_ByEmail

    Exit Sub
EH:
    Me.LabelStatus.caption = "Błąd scalania CSV."
    MsgBox "Błąd scalania CSV: " & Err.Description, vbExclamation
End Sub


Private Sub btnPickInputsDir_Click()
    Dim p As String
    p = modNA_UIInputs.PickFolderDialog( _
            "Wskaż folder z plikami źródłowymi", _
            Me.txtInputsDir.text)

    If Len(p) > 0 Then
        Me.txtInputsDir.text = p
    End If
End Sub
