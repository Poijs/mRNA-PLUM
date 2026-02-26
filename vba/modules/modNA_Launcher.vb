' === Component: modNA_Launcher [Standard Module]
' === Exported: 2026-02-26 14:22:22

' === modNA_Launcher ===
Option Explicit

' ---------------------------
' Konfiguracja / stałe
' ---------------------------
Private Const PROGRESS_INTERVAL_SEC As Double = 1# / 86400#   ' 1 sek (Date = dni)
Private Const PROGRESS_FILE As String = "_run\progress.jsonl"
Private Const RUN_LOG As String = "_run\run.log"

Private Const OUT_INDY As String = "_out\indywidualne\"
Private Const OUT_PDF As String = "_out\pdf\"
Private Const RUN_DIR As String = "_run\"

' Uwaga: ustaw to na nazwę pliku exe jeśli dystrybuujesz PyInstaller
Private Const PIPE_EXE As String = "mrna-plum.exe"  ' albo np. "mrna_plum_cli.exe"

' ---------------------------
' Stan pipeline (globalny)
' ---------------------------
Private Type TStep
    Name As String
    Cmd As String
End Type

Private gSteps() As TStep
Private gStepIndex As Long
Private gStepStart As Date

Private gWsh As Object          ' WScript.Shell
Private gExec As Object         ' WshScriptExec
Private gNextOnTime As Date
Private gMonitoring As Boolean
Private gRoot As String

' ---------------------------
' PUBLIC: wymagane przez Ciebie
' ---------------------------
Public Function RunPython(ByVal cmdLine As String) As Long
    ' Blokujące oczekiwanie (UWAGA: zamrozi UI na czas działania procesu).
    ' Dobre do krótkich komend / testów, ale nie do długiego pipeline.
    Dim sh As Object
    Set sh = CreateObject("WScript.Shell")

    ' 0 = hidden window, True = wait
    ' cmdLine MUSI zawierać pełne cudzysłowy dla ścieżek ze spacjami.
    RunPython = sh.Run(cmdLine, 0, True)
End Function

' ---------------------------
' PUBLIC: uruchomienie całości z UserForm
' ---------------------------
Public Sub StartPipeline()
    On Error GoTo EH

    gRoot = ThisWorkbook.path
    EnsureFolders
    If dir$(gRoot & "\" & PIPE_EXE) = vbNullString Then
        Err.Raise vbObjectError + 701, "StartPipeline", "Nie znaleziono pliku EXE: " & gRoot & "\" & PIPE_EXE
    End If

    If dir$(gRoot & "\config.yaml") = vbNullString Then
        Err.Raise vbObjectError + 702, "StartPipeline", "Nie znaleziono pliku config.yaml: " & gRoot & "\config.yaml"
    End If
    InitSteps

    ' wyczyść artefakty run
    SafeKillFile gRoot & "\" & RUN_LOG
    SafeKillFile gRoot & "\" & gRootRel(PROGRESS_FILE)
    ClearOkFlags

    AppendLog "=== START PIPELINE: " & Format(Now, "yyyy-mm-dd hh:nn:ss") & " ==="

    Set gWsh = CreateObject("WScript.Shell")
    gWsh.CurrentDirectory = gRoot
    gStepIndex = 0

    ' UI: zablokuj przyciski / ustaw status startowy
    UI_SetRunning True
    UI_SetStatus "Start pipeline..."

    StartNextStep
    Exit Sub

EH:
    PipelineFail "StartPipeline error: " & Err.Number & " - " & Err.Description
End Sub

' ---------------------------
' Pipeline core (asynchroniczny, bez freeze UI)
' ---------------------------
Private Sub InitSteps()
    ReDim gSteps(0 To 5) As TStep

    gSteps(0).Name = "merge-logs"
    gSteps(0).Cmd = StepCmd("merge-logs")

    gSteps(1).Name = "parse-events"
    gSteps(1).Cmd = StepCmd("parse-events")

    gSteps(2).Name = "build-activities-state"
    gSteps(2).Cmd = StepCmd("build-activities-state")

    gSteps(3).Name = "compute-stats"
    gSteps(3).Cmd = StepCmd("compute-stats")

    gSteps(4).Name = "export-excel"
    gSteps(4).Cmd = StepCmd("export-excel")

    gSteps(5).Name = "export-individual"
    gSteps(5).Cmd = StepCmd("export-individual")
End Sub

Private Function StepCmd(ByVal stepName As String) As String
    Dim exePath As String, cfgPath As String, logPath As String
    exePath = Quote(gRoot & "\" & PIPE_EXE)
    cfgPath = Quote(gRoot & "\config.yaml")
    logPath = Quote(gRoot & "\" & RUN_LOG)

    Dim inputsDir As String
    inputsDir = UF_Text(raporty_plum, "txtInputsDir")

    Dim extraArgs As String
    extraArgs = ""

    ' parse-events: KEYS override z UI
    If LCase$(stepName) = "parse-events" Then
        Dim keysXlsx As String
        keysXlsx = UF_Text(raporty_plum, "txtKeysXlsx")
        If Len(keysXlsx) > 0 Then
            extraArgs = extraArgs & " --keys-xlsx " & Quote(keysXlsx)
        End If
    End If

    ' build-activities-state: snapshot-file z UI (KRYTYCZNE) + fallback inputs-dir
    If LCase$(stepName) = "build-activities-state" Then
        Dim snapPath As String
        snapPath = UF_Text(raporty_plum, "txtSnapshotFile")

        If Len(snapPath) > 0 Then
            extraArgs = extraArgs & " --snapshot-file " & Quote(snapPath)
        ElseIf Len(inputsDir) > 0 Then
            extraArgs = extraArgs & " --inputs-dir " & Quote(inputsDir)
        Else
            Err.Raise vbObjectError + 513, "StepCmd", _
                "Brak danych wejściowych dla build-activities-state." & vbCrLf & _
                "Uzupełnij txtSnapshotFile lub txtInputsDir."
        End If
    End If

    ' export-excel / export-individual: inputs-dir
    If LCase$(stepName) = "export-excel" Or LCase$(stepName) = "export-individual" Then
        If Len(inputsDir) > 0 Then
            extraArgs = extraArgs & " --inputs-dir " & Quote(inputsDir)
        End If
    End If
    
    ' >>> DODAJ TEN BLOK W StepCmd (obok innych per-step bloków) <<<
    If LCase$(stepName) = "merge-logs" Then
        If Len(inputsDir) > 0 Then
            extraArgs = extraArgs & " --inputs-dir " & Quote(inputsDir)
        End If
    End If
    
    ' export-individual: out-dir
    If LCase$(stepName) = "export-individual" Then
        Dim outDir As String
        outDir = gRoot & "\" & OUT_INDY
        If Right$(outDir, 1) = "\" Then outDir = Left$(outDir, Len(outDir) - 1)
        extraArgs = extraArgs & " --out-dir " & Quote(outDir)
    End If

    Dim inner As String
    inner = "chcp 65001>nul" & _
            " & cd /d " & Quote(gRoot) & _
            " & " & exePath & " " & stepName & _
            " --root " & Quote(gRoot) & _
            " --config " & cfgPath & _
            extraArgs & _
            " >> " & logPath & " 2>&1"

    ' UWAGA: dokładnie JEDNA para cudzysłowów po /c
    StepCmd = "cmd.exe /c " & Quote(inner)
End Function

Private Sub StartNextStep()
    On Error GoTo EH

    If gStepIndex > UBound(gSteps) Then
        ' Python zakończony -> integracja PDF
        AppendLog "=== PYTHON PIPELINE OK ==="
        UI_SetStatus "Python OK. Sprawdzam pliki indywidualne..."

        If Not HasAnyFiles(gRoot & "\" & OUT_INDY, "*.xlsx") Then
            PipelineFail "Brak plików w " & OUT_INDY & " — nie generuję PDF."
            Exit Sub
        End If

        UI_SetStatus "Generuję PDF (VBA)..."
        AppendLog "=== START PDF ENGINE ==="

                ' === PDF ENGINE (BATCH, bez klikania) ===
        Dim cfg As Object
        Set cfg = CreateObject("Scripting.Dictionary")

        Dim templatePath As String
        On Error Resume Next
        templatePath = Trim$(raporty_plum.txtTemplateXlsx.text)
        On Error GoTo EH

        If Len(templatePath) = 0 Then
            PipelineFail "Brak ścieżki do wzoru raportu. Uzupełnij raporty_plum.txtTemplateXlsx."
            Exit Sub
        End If
        If dir$(templatePath) = vbNullString Then
            PipelineFail "Nie znaleziono wzoru raportu: " & templatePath
            Exit Sub
        End If

        cfg("root") = gRoot
        cfg("in_indywidualne") = gRoot & "\" & OUT_INDY
        cfg("out_pdf") = gRoot & "\" & OUT_PDF
        cfg("run_dir") = gRoot & "\" & RUN_DIR
        cfg("template_path") = templatePath

        ' Nazwy arkuszy zgodne z silnikiem i Twoimi stałymi w modNA_PdfEngine:
        cfg("sheet_dane_pers") = "DANE_PERS"
        cfg("sheet_dane_kursy") = "DANE_KURSY"
        cfg("sheet_report") = "Raport_NA"

        cfg("max_blocks") = 16
        cfg("truncate_overflow") = True

        AppendLog "=== START PDF ENGINE (BATCH) ==="
        Call modPdfEngine.PdfEngine_RunBatch(cfg)
        AppendLog "=== PDF ENGINE OK ==="
        UI_SetStatus "Zakończono poprawnie."
        UI_SetRunning False
        StopMonitor
        Exit Sub
    End If

    Dim stepName As String
    stepName = gSteps(gStepIndex).Name

    UI_SetStatus "Krok " & (gStepIndex + 1) & "/" & (UBound(gSteps) + 1) & ": " & stepName
    AppendLog "--- STEP START: " & stepName & " @ " & Format(Now, "yyyy-mm-dd hh:nn:ss")
    AppendLog "CMD: " & gSteps(gStepIndex).Cmd
    gStepStart = Now

    ' Exec -> nie blokuje UI; monitorujemy Status w OnTime
    Set gExec = gWsh.Exec(gSteps(gStepIndex).Cmd)

    StartMonitor
    Exit Sub

EH:
    PipelineFail "StartNextStep error: " & Err.Number & " - " & Err.Description
End Sub

' ---------------------------
' Monitor progress.jsonl + status procesu
' ---------------------------
Private Sub StartMonitor()
    gMonitoring = True
    ScheduleMonitor Now + PROGRESS_INTERVAL_SEC
End Sub

Private Sub ScheduleMonitor(ByVal whenTime As Date)
    On Error Resume Next
    gNextOnTime = whenTime
    Application.OnTime earliesttime:=gNextOnTime, procedure:="modNA_Launcher.MonitorProgress", schedule:=True
    On Error GoTo 0
End Sub

Public Sub MonitorProgress()
    On Error GoTo EH

    If Not gMonitoring Then Exit Sub

    ' 1) update UI z ostatniej linii progress.jsonl
    UpdateUIFromProgress

    ' 2) sprawdzamy proces
    If Not gExec Is Nothing Then
        ' Status: 0=Running, 1=Finished
        If CLng(gExec.Status) = 1 Then
            Dim exitCode As Long
            exitCode = CLng(gExec.exitCode)

            Dim stepName As String
            stepName = gSteps(gStepIndex).Name

            AppendLog "--- STEP END: " & stepName & _
                      " exit=" & exitCode & _
                      " dur=" & FormatDuration(Now - gStepStart) & _
                      " @ " & Format(Now, "yyyy-mm-dd hh:nn:ss")

            If exitCode <> 0 Then
                Dim tail As String
                tail = ReadLastLines(gRoot & "\" & RUN_LOG, 10)
                PipelineFail "Krok '" & stepName & "' zakończony błędem exit=" & exitCode & vbCrLf & vbCrLf & _
                             "Ostatnie linie logu:" & vbCrLf & tail
                Exit Sub
            End If

            ' zapis <step>.ok
            WriteTextFile gRoot & "\" & RUN_DIR & stepName & ".ok", _
                "ok " & Format(Now, "yyyy-mm-dd hh:nn:ss") & " dur=" & FormatDuration(Now - gStepStart)

            ' kolejny krok
            Set gExec = Nothing
            gStepIndex = gStepIndex + 1
            StartNextStep
            Exit Sub
        End If
    End If

    ' jeśli nadal działa -> planuj kolejny tick
    ScheduleMonitor Now + PROGRESS_INTERVAL_SEC
    Exit Sub

EH:
    PipelineFail "MonitorProgress error: " & Err.Number & " - " & Err.Description
End Sub

Private Sub StopMonitor()
    On Error Resume Next
    gMonitoring = False
    If gNextOnTime <> 0 Then
        Application.OnTime earliesttime:=gNextOnTime, procedure:="modNA_Launcher.MonitorProgress", schedule:=False
    End If
    On Error GoTo 0
End Sub

' ---------------------------
' UI helpers (podłącz do UserForm)
' ---------------------------
Private Sub UI_SetRunning(ByVal running As Boolean)
    On Error Resume Next
    With raporty_plum
        .btnStart.enabled = Not running
    End With
    On Error GoTo 0
End Sub

Private Sub UI_SetStatus(ByVal msg As String)
    On Error Resume Next
    raporty_plum.LabelStatus.caption = msg
    On Error GoTo 0
End Sub

Private Sub UI_SetProgress(ByVal pct As Double, ByVal msg As String)
    On Error Resume Next
    With raporty_plum
        .LabelStatus.caption = msg
        ' ProgressBar jako Label w Frame: LabelBar.Width = Frame.Width * pct
        If pct < 0 Then pct = 0
        If pct > 1 Then pct = 1
        .LabelBar.Width = .FrameBar.Width * pct
        .LabelPct.caption = Format(pct, "0%")
    End With
    On Error GoTo 0
End Sub

' ---------------------------
' Progress.jsonl parsing
' Zakładamy, że python dopisuje linie np:
' {"step":"parse-events","pct":0.34,"msg":"Parsing..."}
' ---------------------------
Private Sub UpdateUIFromProgress()
    Dim p As String
    p = gRoot & "\" & gRootRel(PROGRESS_FILE)
    If dir(p) = vbNullString Then Exit Sub

    Dim line As String
    line = ReadLastNonEmptyLine(p)
    If Len(line) = 0 Then Exit Sub

    Dim stepName As String, msg As String
    Dim pct As Double

    stepName = JsonGetString(line, "step")

    msg = JsonGetString(line, "message")
    If Len(msg) = 0 Then msg = JsonGetString(line, "msg")

    pct = JsonGetNumber(line, "pct")

    Dim cur As Double, tot As Double
    cur = JsonGetNumber(line, "current")
    tot = JsonGetNumber(line, "total")

' pct fallback z current/total tylko jeśli pct nieobecne (u Ciebie JsonGetNumber zwykle zwraca 0)
' więc rozróżniamy: jeśli tot>0 i cur>=0 i pct=0, to i tak policz (bo to poprawne 0..1)
    If tot > 0 Then
        If pct = 0# Then
            pct = cur / tot
        End If
    End If

    If Len(msg) = 0 Then msg = stepName

' Fallback na "percent" (0..100) tylko jeśli nadal nie mamy sensownego pct i percent > 0
    If pct = 0# Then
        Dim pct100 As Double
        pct100 = JsonGetNumber(line, "percent")
        If pct100 > 1# Then
            pct = pct100 / 100#
        ElseIf pct100 > 0# Then
            pct = pct100
        End If
    End If

' clamp 0..1
    If pct < 0# Then pct = 0#
    If pct > 1# Then pct = 1#

    ' UI
    UI_SetProgress pct, msg
End Sub

Private Function JsonGetString(ByVal jsonLine As String, ByVal key As String) As String
    ' minimalistycznie: szukamy "key":"value"
    Dim pat As String, p As Long, q1 As Long, q2 As Long
    pat = """" & key & """:"
    p = InStr(1, jsonLine, pat, vbTextCompare)
    If p = 0 Then Exit Function

    q1 = InStr(p + Len(pat), jsonLine, """")
    If q1 = 0 Then Exit Function
    q2 = InStr(q1 + 1, jsonLine, """")
    If q2 = 0 Then Exit Function

    JsonGetString = Mid$(jsonLine, q1 + 1, q2 - q1 - 1)
End Function

Private Function JsonGetNumber(ByVal jsonLine As String, ByVal key As String) As Double
    Dim pat As String, p As Long, i As Long, ch As String, buf As String
    pat = """" & key & """:"
    p = InStr(1, jsonLine, pat, vbTextCompare)
    If p = 0 Then Exit Function

    i = p + Len(pat)
    ' pomiń spacje
    Do While i <= Len(jsonLine) And Mid$(jsonLine, i, 1) = " "
        i = i + 1
    Loop

    Do While i <= Len(jsonLine)
        ch = Mid$(jsonLine, i, 1)
        If (ch Like "[0-9]") Or ch = "." Or ch = "-" Then
            buf = buf & ch
            i = i + 1
        Else
            Exit Do
        End If
    Loop

    If Len(buf) = 0 Then Exit Function
    JsonGetNumber = CDbl(Replace(buf, ",", "."))
End Function

' ---------------------------
' Logging / pliki / utilsy
' ---------------------------
Private Sub PipelineFail(ByVal message As String)
    AppendLog "!!! PIPELINE FAIL: " & message
    StopMonitor
    UI_SetRunning False
    UI_SetStatus "BŁĄD: zobacz run.log"

    MsgBox message, vbCritical, "mRNA-PLUM Pipeline"
End Sub

Private Sub AppendLog(ByVal s As String)
    WriteTextFile gRoot & "\" & RUN_LOG, s & vbCrLf, True
End Sub

Private Sub WriteTextFile(ByVal fullPath As String, ByVal text As String, Optional ByVal append As Boolean = False)
    Dim fso As Object, ts As Object
    Set fso = CreateObject("Scripting.FileSystemObject")

    EnsureFolderExists fso.GetParentFolderName(fullPath)

    If append And fso.FileExists(fullPath) Then
        Set ts = fso.OpenTextFile(fullPath, 8, True, -1) ' ForAppending, Unicode
    Else
        Set ts = fso.OpenTextFile(fullPath, 2, True, -1) ' ForWriting, Unicode
    End If

    ts.Write text
    ts.Close
End Sub

Private Function ReadLastLines(ByVal fullPath As String, ByVal n As Long) As String
    ' prosto i bezpiecznie: czytamy cały plik tylko dla tail (run.log zwykle nie jest ogromny)
    On Error GoTo EH

    Dim fso As Object, ts As Object, allText As String, arr() As String
    Set fso = CreateObject("Scripting.FileSystemObject")
    If Not fso.FileExists(fullPath) Then Exit Function

    Set ts = fso.OpenTextFile(fullPath, 1, False, -1) ' ForReading, Unicode
    allText = ts.ReadAll
    ts.Close

    arr = Split(Replace(allText, vbCrLf, vbLf), vbLf)

    Dim i As Long, startI As Long, buf As String
    startI = UBound(arr) - n + 1
    If startI < 0 Then startI = 0

    For i = startI To UBound(arr)
        If Len(arr(i)) > 0 Then buf = buf & arr(i) & vbCrLf
    Next

    ReadLastLines = buf
    Exit Function

EH:
    ReadLastLines = "(nie udało się odczytać tail logu: " & Err.Description & ")"
End Function

Private Function ReadLastNonEmptyLine(ByVal fullPath As String) As String
    On Error GoTo EH

    Dim fso As Object, ts As Object, allText As String, arr() As String
    Set fso = CreateObject("Scripting.FileSystemObject")
    If Not fso.FileExists(fullPath) Then Exit Function

    Set ts = fso.OpenTextFile(fullPath, 1, False, -1)
    allText = ts.ReadAll
    ts.Close

    arr = Split(Replace(allText, vbCrLf, vbLf), vbLf)

    Dim i As Long
    For i = UBound(arr) To 0 Step -1
        If Len(Trim$(arr(i))) > 0 Then
            ReadLastNonEmptyLine = Trim$(arr(i))
            Exit Function
        End If
    Next i
    Exit Function

EH:
    ' ignoruj
End Function

Private Function HasAnyFiles(ByVal folderPath As String, ByVal pattern As String) As Boolean
    Dim p As String
    p = folderPath
    If Right$(p, 1) <> "\" Then p = p & "\"
    HasAnyFiles = (dir(p & pattern) <> vbNullString)
End Function

Private Sub EnsureFolders()
    EnsureFolderExists gRoot & "\" & RUN_DIR
    EnsureFolderExists gRoot & "\" & OUT_INDY
    EnsureFolderExists gRoot & "\" & OUT_PDF
End Sub

Private Sub EnsureFolderExists(ByVal folderPath As String)
    Dim fso As Object
    Set fso = CreateObject("Scripting.FileSystemObject")
    If Len(folderPath) = 0 Then Exit Sub
    If Not fso.FolderExists(folderPath) Then fso.CreateFolder folderPath
End Sub

Private Sub SafeKillFile(ByVal fullPath As String)
    On Error Resume Next
    If Len(dir(fullPath)) > 0 Then Kill fullPath
    On Error GoTo 0
End Sub

Private Sub ClearOkFlags()
    On Error Resume Next
    Dim f As String
    f = dir(gRoot & "\" & RUN_DIR & "*.ok")
    Do While Len(f) > 0
        Kill gRoot & "\" & RUN_DIR & f
        f = dir()
    Loop
    On Error GoTo 0
End Sub

Private Function FormatDuration(ByVal dtDays As Double) As String
    Dim totalSec As Long
    totalSec = CLng(dtDays * 86400#)
    FormatDuration = (totalSec \ 60) & "m " & (totalSec Mod 60) & "s"
End Function

Private Function Quote(ByVal s As String) As String
    Quote = """" & s & """"
End Function

Private Function gRootRel(ByVal rel As String) As String
    gRootRel = rel
    If Left$(gRootRel, 1) = "\" Then gRootRel = Mid$(gRootRel, 2)
End Function
Private Function UF_Text(ByVal formObj As Object, ByVal ctrlName As String) As String
    ' Bezpiecznie pobiera .Text z kontrolki UserForm po nazwie.
    ' Zwraca "" jeśli kontrolka nie istnieje.
    On Error GoTo EH
    Dim ctl As Object
    Set ctl = CallByName(formObj, ctrlName, VbGet)
    UF_Text = Trim$(CStr(CallByName(ctl, "Text", VbGet)))
    Exit Function
EH:
    UF_Text = ""
End Function