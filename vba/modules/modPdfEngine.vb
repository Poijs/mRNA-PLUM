' === Component: modPdfEngine [Standard Module]
' === Exported: 2026-02-26 14:22:22

Option Explicit

' ============================================================
' modPdfEngine — stable PDF engine for mRNA-PLUM (late binding)
' ============================================================

' ---------- Logging ----------
Private mLogFile As Integer
Private mLogPath As String

' ============================================================
' PUBLIC API
' ============================================================
Public Sub PdfEngine_RunBatch(ByVal cfg As Object)
    On Error GoTo EH

    Dim t0 As Double: t0 = Timer

    ' Validate cfg
    Dim root As String: root = NzStr(cfg("root"))
    Dim inFolder As String: inFolder = NzStr(cfg("in_indywidualne"))
    Dim outPdf As String: outPdf = NzStr(cfg("out_pdf"))
    Dim runDir As String: runDir = NzStr(cfg("run_dir"))
    Dim templatePath As String: templatePath = NzStr(cfg("template_path"))

    Dim sheetPers As String: sheetPers = NzStr(cfg("sheet_dane_pers"))
    Dim sheetKursy As String: sheetKursy = NzStr(cfg("sheet_dane_kursy"))
    Dim sheetReport As String: sheetReport = NzStr(cfg("sheet_report"))

    Dim maxBlocks As Long: maxBlocks = CLng(cfg("max_blocks"))
    Dim truncateOverflow As Boolean: truncateOverflow = CBool(cfg("truncate_overflow"))

    If Right$(inFolder, 1) <> "\" Then inFolder = inFolder & "\"
    If Right$(outPdf, 1) <> "\" Then outPdf = outPdf & "\"
    If Right$(runDir, 1) <> "\" Then runDir = runDir & "\"

    EnsureFolder runDir
    EnsureFolder outPdf

    OpenLog runDir, "pdf_batch"

    LogLine "START PdfEngine_RunBatch"
    LogLine "root=" & root
    LogLine "in_indywidualne=" & inFolder
    LogLine "out_pdf=" & outPdf
    LogLine "template_path=" & templatePath

    If dir(templatePath) = vbNullString Then
        Err.Raise vbObjectError + 100, "PdfEngine_RunBatch", "Nie znaleziono template: " & templatePath
    End If

    ' Stability settings
    Dim prevCalc As XlCalculation
    Dim prevSU As Boolean, prevEE As Boolean, prevDA As Boolean
    prevCalc = Application.Calculation
    prevSU = Application.ScreenUpdating
    prevEE = Application.EnableEvents
    prevDA = Application.DisplayAlerts

    Application.ScreenUpdating = False
    Application.EnableEvents = False
    Application.DisplayAlerts = False
    Application.Calculation = xlCalculationManual

    Dim f As String
    f = dir(inFolder & "*.xlsx")
    If Len(f) = 0 Then
        LogLine "Brak plików XLSX w: " & inFolder
        GoTo CleanUp
    End If

    Dim countOk As Long, countFail As Long, countAll As Long

    Do While Len(f) > 0
        countAll = countAll + 1
        Dim srcPath As String: srcPath = inFolder & f

        On Error GoTo OneFail
        ProcessOneTeacherFile srcPath, templatePath, outPdf, sheetPers, sheetKursy, sheetReport, maxBlocks, truncateOverflow
        countOk = countOk + 1
        LogLine "OK: " & srcPath
        On Error GoTo EH

NextFile:
    f = dir()
    DoEvents
    GoTo ContinueLoop

OneFail:
    countFail = countFail + 1
    LogLine "FAIL: " & srcPath & " | Err=" & Err.Number & " | " & Err.Description
    Err.Clear
    On Error GoTo EH
    Resume NextFile

ContinueLoop:
Loop

CleanUp:
    Application.Calculation = prevCalc
    Application.DisplayAlerts = prevDA
    Application.EnableEvents = prevEE
    Application.ScreenUpdating = prevSU

    LogLine "DONE: all=" & countAll & ", ok=" & countOk & ", fail=" & countFail & ", sec=" & Format$(Timer - t0, "0.0")
    CloseLog
    Exit Sub

EH:
    LogLine "FATAL: Err=" & Err.Number & " | " & Err.Description
    CloseLog
    Err.Raise Err.Number, "PdfEngine_RunBatch", Err.Description
End Sub

' ============================================================
' CORE — one teacher file
' ============================================================
Private Sub ProcessOneTeacherFile( _
    ByVal srcPath As String, _
    ByVal templatePath As String, _
    ByVal outPdfFolder As String, _
    ByVal sheetPers As String, _
    ByVal sheetKursy As String, _
    ByVal sheetReport As String, _
    ByVal maxBlocks As Long, _
    ByVal truncateOverflow As Boolean _
)
    On Error GoTo EH

    Dim wbData As Workbook, wbTpl As Workbook
    Dim wsPers As Worksheet, wsKursy As Worksheet, wsReport As Worksheet

    LogLine "----"
    LogLine "Process: " & srcPath

    Set wbData = Workbooks.Open(fileName:=srcPath, ReadOnly:=True, UpdateLinks:=0, AddToMru:=False)

    Set wsPers = GetSheetSafe(wbData, sheetPers)
    Set wsKursy = GetSheetSafe(wbData, sheetKursy)
    If wsKursy Is Nothing Then Err.Raise vbObjectError + 200, "ProcessOneTeacherFile", "Brak arkusza DANE_KURSY w: " & srcPath

    Set wbTpl = Workbooks.Open(fileName:=templatePath, ReadOnly:=True, UpdateLinks:=0, AddToMru:=False)

    Set wsReport = ResolveReportSheet(wbTpl, sheetReport)
    If wsReport Is Nothing Then Err.Raise vbObjectError + 201, "ProcessOneTeacherFile", "Nie znaleziono arkusza raportu w template."

    ' 1) Fill named ranges (metryczka + KPI)
    If Not wsPers Is Nothing Then
        FillNamedRangesFromDanePers wbTpl, wsPers
    Else
        LogLine "WARN: Brak DANE_PERS w źródle: " & srcPath
    End If

    ' 2) Load courses
    Dim courses As Collection
    Set courses = LoadCourses(wsKursy)

    Dim nCourses As Long
    nCourses = courses.Count

    If nCourses > maxBlocks Then
        Dim msg As String
        msg = "Nadmiar kursów: " & nCourses & " > " & maxBlocks & " w " & srcPath
        If truncateOverflow Then
            LogLine "WARN: " & msg & " | TRUNCATE -> " & maxBlocks
            nCourses = maxBlocks
        Else
            Err.Raise vbObjectError + 202, "ProcessOneTeacherFile", msg
        End If
    End If

    ' 3) Fill course blocks 1..N
    Dim i As Long
    For i = 1 To nCourses
        FillCourseBlock wsReport, i, courses(i)
    Next i

    ' 4) Clear unused blocks
    TrimUnusedBlocks wsReport, nCourses, maxBlocks

    ' 5) Setup page breaks (manual)
    SetupPageBreaks wsReport, nCourses, maxBlocks

    ' 6) Print area to last block
    SetPrintAreaToLastBlock wsReport, nCourses, maxBlocks

    ' 7) Export PDF
    Dim outPdfPath As String
    outPdfPath = outPdfFolder & BuildPdfFileName(wbTpl, wbData, wsPers, srcPath) & ".pdf"

    ExportReportToPdf wbTpl, wsReport, outPdfPath

    ' Close without saving (template always)
    SafeClose wbTpl
    SafeClose wbData
    Exit Sub

EH:
    LogLine "ERROR ProcessOneTeacherFile: Err=" & Err.Number & " | " & Err.Description
    SafeClose wbTpl
    SafeClose wbData
    Err.Raise Err.Number, "ProcessOneTeacherFile", Err.Description
End Sub

' ============================================================
' REQUIRED FUNCTIONS (per spec)
' ============================================================

Public Sub FillNamedRangesFromDanePers(ByVal templateWb As Workbook, ByVal danePersWs As Worksheet)
    On Error GoTo EH

    Dim LastRow As Long
    LastRow = LastUsedRow(danePersWs, 1)
    If LastRow < 1 Then Exit Sub

    Dim r As Long
    Dim startRow As Long: startRow = 1

    ' jeśli A1 wygląda jak nagłówek "Name" -> start od 2
    If LCase$(Trim$(CStr(danePersWs.Cells(1, 1).value))) = "name" Then startRow = 2

    For r = startRow To LastRow
        Dim nm As String, v As Variant
        nm = Trim$(CStr(danePersWs.Cells(r, 1).value))
        v = danePersWs.Cells(r, 2).value

        If Len(nm) > 0 Then
            If NameExistsInWorkbook(templateWb, nm) Then
                On Error Resume Next
                templateWb.names(nm).RefersToRange.value = v
                If Err.Number <> 0 Then
                    LogLine "WARN: nie dało się ustawić NamedRange '" & nm & "' (" & Err.Number & "): " & Err.Description
                    Err.Clear
                End If
                On Error GoTo EH
            Else
                LogLine "WARN: brak NamedRange w template: " & nm
            End If
        End If
    Next r

    Exit Sub
EH:
    Err.Raise Err.Number, "FillNamedRangesFromDanePers", Err.Description
End Sub

Public Function LoadCourses(ByVal daneKursyWs As Worksheet) As Collection
    On Error GoTo EH

    Dim col As New Collection

    Dim LastRow As Long, lastCol As Long
    LastRow = LastUsedRow(daneKursyWs, 1)
    lastCol = LastUsedCol(daneKursyWs, 1)
    If LastRow < 2 Or lastCol < 1 Then
        Set LoadCourses = col
        Exit Function
    End If

    Dim headers() As Variant
    headers = daneKursyWs.Range(daneKursyWs.Cells(1, 1), daneKursyWs.Cells(1, lastCol)).Value2

    Dim data() As Variant
    data = daneKursyWs.Range(daneKursyWs.Cells(2, 1), daneKursyWs.Cells(LastRow, lastCol)).Value2

    Dim r As Long, c As Long
    For r = 1 To UBound(data, 1)
        Dim d As Object ' Scripting.Dictionary
        Set d = CreateObject("Scripting.Dictionary")
        d.CompareMode = 1 ' TextCompare

        For c = 1 To lastCol
            Dim h As String
            h = Trim$(CStr(headers(1, c)))
            If Len(h) = 0 Then h = "col_" & c
            d(h) = data(r, c)
        Next c

        col.Add d
    Next r

    Set LoadCourses = col
    Exit Function

EH:
    Err.Raise Err.Number, "LoadCourses", Err.Description
End Function

Public Sub FillCourseBlock(ByVal wsReport As Worksheet, ByVal blockNo As Long, ByVal courseRow As Object)
    ' courseRow: Scripting.Dictionary header->value
    On Error GoTo EH

    Dim topNm As String
    topNm = "nr_blk_" & Format$(blockNo, "00") & "_top"

    Dim topCell As Range
    Set topCell = GetNamedRangeCell(wsReport.Parent, topNm)
    If topCell Is Nothing Then
        LogLine "WARN: brak NamedRange top dla bloku: " & topNm
        Exit Sub
    End If

    ' --- MAPOWANIE BLOKU ---
    ' Najstabilniejsze podejście: tabela map w template (opcjonalna):
    ' Sheet: "MAPA_BLOKU"
    ' kol A: FieldName (nagłówek z DANE_KURSY)
    ' kol B: RowOffset (0=ten sam wiersz co top)
    ' kol C: ColOffset (0=ta sama kolumna co top)
    ' Jeśli nie ma MAPA_BLOKU -> stosujemy "default minimal" (tytuł kursu itp.) + log ostrzegawczy.

    Dim map As Object
    Set map = TryLoadBlockMap(wsReport.Parent)

    If map Is Nothing Then
        ' Default minimal: wpisz tytuł kursu w 2 wierszach od top,
        ' a ID kursu obok (bardzo bezpieczne, bo nie rozwala layoutu).
        ' Dostosujesz po potwierdzeniu offsetów albo dodaniu MAPA_BLOKU.
        LogLine "WARN: brak MAPA_BLOKU w template — używam default minimal dla bloku " & blockNo

        Dim vTitle As Variant, vId As Variant
        vTitle = PickCourseValue(courseRow, Array("pełna nazwa e-kursu", "pelna nazwa e-kursu", "full_name", "fullname", "nazwa", "course_name", "nazwa kursu"))
        vId = PickCourseValue(courseRow, Array("id kursu", "course_id", "id", "courseid"))

        ' Wstaw: topCell.Offset(0,0) -> tytuł
        topCell.Offset(0, 0).value = vTitle
        ' ID np. w komórce obok (kolumna +10) — bezpieczne, ale może wymagać korekty
        topCell.Offset(0, 10).value = vId

        Exit Sub
    End If

    ' Map exists: iterate entries field->(rOff,cOff)
    Dim k As Variant
    For Each k In map.keys
        Dim rc As Variant
        rc = map(k) ' array(0)=rOff, array(1)=cOff

        Dim val As Variant
        val = GetCourseValueByHeader(courseRow, CStr(k))

        With topCell.Offset(CLng(rc(0)), CLng(rc(1)))
            .value = val
        End With
    Next k

    Exit Sub

EH:
    Err.Raise Err.Number, "FillCourseBlock", Err.Description
End Sub

Public Sub SetupPageBreaks(ByVal wsReport As Worksheet, ByVal nCourses As Long, ByVal maxBlocks As Long)
    On Error GoTo EH

    ' Reset existing manual breaks
    On Error Resume Next
    wsReport.ResetAllPageBreaks
    On Error GoTo EH

    ' Wg ustaleń: HPageBreak przed blokami #2,#5,#8,#11,#14 (ale tylko jeśli istnieją w zakresie 1..maxBlocks)
    Dim breaks As Variant
    breaks = Array(2, 5, 8, 11, 14)

    Dim i As Long
    For i = LBound(breaks) To UBound(breaks)
        Dim b As Long: b = CLng(breaks(i))
        If b >= 1 And b <= maxBlocks Then
            ' Jeśli realnie mamy mniej kursów niż b, to i tak break jest OK (template ma stałe strony),
            ' ale możesz też warunkować: If nCourses >= b Then ...
            Dim nm As String
            nm = "nr_blk_" & Format$(b, "00") & "_top"
            Dim topCell As Range
            Set topCell = GetNamedRangeCell(wsReport.Parent, nm)
            If Not topCell Is Nothing Then
                wsReport.HPageBreaks.Add Before:=topCell
            Else
                LogLine "WARN: SetupPageBreaks brak named range: " & nm
            End If
        End If
    Next i

    Exit Sub
EH:
    Err.Raise Err.Number, "SetupPageBreaks", Err.Description
End Sub

Public Sub TrimUnusedBlocks(ByVal wsReport As Worksheet, ByVal nCourses As Long, ByVal maxBlocks As Long)
    On Error GoTo EH

    If nCourses < 0 Then nCourses = 0
    If nCourses >= maxBlocks Then Exit Sub

    Dim blockHeight As Long
    blockHeight = DetectBlockHeight(wsReport, maxBlocks)
    If blockHeight <= 0 Then
        LogLine "WARN: Nie wykryto wysokości bloku — TrimUnusedBlocks pominięte"
        Exit Sub
    End If

    Dim b As Long
    For b = nCourses + 1 To maxBlocks
        Dim topNm As String: topNm = "nr_blk_" & Format$(b, "00") & "_top"
        Dim topCell As Range: Set topCell = GetNamedRangeCell(wsReport.Parent, topNm)
        If Not topCell Is Nothing Then
            Dim rng As Range
            ' Czyścimy "obszar bloku": od top w dół blockHeight-1 wierszy.
            ' Szerokość: używamy UsedRange.Columns, ale ograniczamy do sensownego obszaru strony.
            Set rng = BlockRangeByHeuristics(wsReport, topCell, blockHeight)
            rng.ClearContents
        End If
    Next b

    Exit Sub
EH:
    Err.Raise Err.Number, "TrimUnusedBlocks", Err.Description
End Sub

Public Sub SetPrintAreaToLastBlock(ByVal wsReport As Worksheet, ByVal nCourses As Long, ByVal maxBlocks As Long)
    On Error GoTo EH

    If nCourses <= 0 Then
        ' jeżeli brak kursów — drukuj stronę 1 (metryczka + KPI) -> ustawimy UsedRange
        wsReport.PageSetup.PrintArea = wsReport.UsedRange.Address
        Exit Sub
    End If

    If nCourses > maxBlocks Then nCourses = maxBlocks

    Dim blockHeight As Long
    blockHeight = DetectBlockHeight(wsReport, maxBlocks)
    If blockHeight <= 0 Then
        wsReport.PageSetup.PrintArea = wsReport.UsedRange.Address
        Exit Sub
    End If

    Dim lastTopNm As String: lastTopNm = "nr_blk_" & Format$(nCourses, "00") & "_top"
    Dim lastTop As Range: Set lastTop = GetNamedRangeCell(wsReport.Parent, lastTopNm)

    If lastTop Is Nothing Then
        wsReport.PageSetup.PrintArea = wsReport.UsedRange.Address
        Exit Sub
    End If

    Dim LastRow As Long
    LastRow = lastTop.row + blockHeight - 1

    ' Kolumny do wydruku: od 1 do ostatniej użytej w nagłówku / usedrange
    Dim lastCol As Long
    lastCol = wsReport.UsedRange.Column + wsReport.UsedRange.Columns.Count - 1
    If lastCol < 1 Then lastCol = 1

    Dim rng As Range
    Set rng = wsReport.Range(wsReport.Cells(1, 1), wsReport.Cells(LastRow, lastCol))
    wsReport.PageSetup.PrintArea = rng.Address

    Exit Sub
EH:
    Err.Raise Err.Number, "SetPrintAreaToLastBlock", Err.Description
End Sub

Public Sub ExportReportToPdf(ByVal templateWb As Workbook, ByVal wsReport As Worksheet, ByVal outPdfPath As String)
    On Error GoTo EH

    EnsureFolder ParentFolder(outPdfPath)

    ' Stabilne ustawienia PageSetup wg ustaleń
    With wsReport.PageSetup
        .Zoom = False
        .FitToPagesWide = 1
        .FitToPagesTall = False
    End With

    ' Export
    wsReport.ExportAsFixedFormat _
        Type:=xlTypePDF, _
        fileName:=outPdfPath, _
        Quality:=xlQualityStandard, _
        IncludeDocProperties:=True, _
        IgnorePrintAreas:=False, _
        OpenAfterPublish:=False

    LogLine "PDF: " & outPdfPath
    Exit Sub

EH:
    Err.Raise Err.Number, "ExportReportToPdf", Err.Description
End Sub

' ============================================================
' Helpers — report/template/data
' ============================================================

Private Function ResolveReportSheet(ByVal wb As Workbook, ByVal sheetName As String) As Worksheet
    On Error Resume Next
    If Len(sheetName) > 0 Then
        Set ResolveReportSheet = wb.Worksheets(sheetName)
    Else
        Set ResolveReportSheet = wb.Worksheets(1)
    End If
    On Error GoTo 0
End Function

Private Function GetSheetSafe(ByVal wb As Workbook, ByVal sheetName As String) As Worksheet
    On Error Resume Next
    Set GetSheetSafe = wb.Worksheets(sheetName)
    On Error GoTo 0
End Function

Private Sub SafeClose(ByVal wb As Workbook)
    On Error Resume Next
    If Not wb Is Nothing Then wb.Close SaveChanges:=False
    On Error GoTo 0
End Sub

Private Function BuildPdfFileName(ByVal wbTpl As Workbook, ByVal wbData As Workbook, ByVal wsPers As Worksheet, ByVal srcPath As String) As String
    ' Prefer: NazwiskoImie + BAZUS ID (z DANE_PERS jeśli są NamedRanges na te dane)
    On Error GoTo EH

    Dim base As String
    base = FileBaseName(srcPath)

    Dim nazw As String, bazus As String
    nazw = ""
    bazus = ""

    ' Jeżeli w DANE_PERS są klucze, np. nr_meta_NazwiskoImie / nr_meta_BazusID
    ' to to już trafia do NamedRanges w template. Ale do nazwy PDF możemy też czytać prosto z DANE_PERS:
    If Not wsPers Is Nothing Then
        nazw = FindPersValue(wsPers, Array("NazwiskoImie", "Nazwisko Imię", "NAZWISKOIMIE", "nr_meta_NazwiskoImie"))
        bazus = FindPersValue(wsPers, Array("BAZUS ID", "Bazus ID", "BAZUSID", "nr_meta_BazusID"))
    End If

    If Len(Trim$(nazw)) > 0 Then base = Trim$(nazw)
    If Len(Trim$(bazus)) > 0 Then base = base & "_" & Trim$(bazus)

    base = SanitizeFileName(base)
    If Len(base) > 180 Then base = Left$(base, 180)

    BuildPdfFileName = base
    Exit Function

EH:
    BuildPdfFileName = SanitizeFileName(FileBaseName(srcPath))
End Function

Private Function FindPersValue(ByVal ws As Worksheet, ByVal keys As Variant) As String
    On Error GoTo EH
    Dim LastRow As Long: LastRow = LastUsedRow(ws, 1)
    If LastRow < 1 Then Exit Function

    Dim startRow As Long: startRow = 1
    If LCase$(Trim$(CStr(ws.Cells(1, 1).value))) = "name" Then startRow = 2

    Dim r As Long, i As Long
    For r = startRow To LastRow
        Dim nm As String: nm = Trim$(CStr(ws.Cells(r, 1).value))
        If Len(nm) > 0 Then
            For i = LBound(keys) To UBound(keys)
                If LCase$(nm) = LCase$(CStr(keys(i))) Then
                    FindPersValue = Trim$(CStr(ws.Cells(r, 2).value))
                    Exit Function
                End If
            Next i
        End If
    Next r
    Exit Function
EH:
    FindPersValue = ""
End Function

' ============================================================
' Block mapping via MAPA_BLOKU (recommended)
' ============================================================

Private Function TryLoadBlockMap(ByVal wb As Workbook) As Object
    ' Returns Scripting.Dictionary: key=FieldName (DANE_KURSY header), value=array(rOff,cOff)
    ' Expected sheet: MAPA_BLOKU
    ' A: FieldName, B: RowOffset, C: ColOffset
    On Error GoTo EH

    Dim ws As Worksheet
    On Error Resume Next
    Set ws = wb.Worksheets("MAPA_BLOKU")
    On Error GoTo 0
    If ws Is Nothing Then Exit Function

    Dim LastRow As Long: LastRow = LastUsedRow(ws, 1)
    If LastRow < 2 Then Exit Function

    Dim d As Object: Set d = CreateObject("Scripting.Dictionary")
    d.CompareMode = 1 ' TextCompare

    Dim r As Long
    For r = 2 To LastRow
        Dim fieldName As String
        fieldName = Trim$(CStr(ws.Cells(r, 1).value))
        If Len(fieldName) > 0 Then
            Dim ro As Long, co As Long
            ro = CLng(val(ws.Cells(r, 2).value))
            co = CLng(val(ws.Cells(r, 3).value))
            d(fieldName) = Array(ro, co)
        End If
    Next r

    If d.Count = 0 Then Exit Function
    Set TryLoadBlockMap = d
    Exit Function

EH:
    LogLine "WARN: MAPA_BLOKU read error: " & Err.Number & " | " & Err.Description
End Function

Private Function GetCourseValueByHeader(ByVal courseRow As Object, ByVal headerName As String) As Variant
    ' direct header match
    If courseRow.Exists(headerName) Then
        GetCourseValueByHeader = courseRow(headerName)
    Else
        ' fuzzy (normalized)
        GetCourseValueByHeader = GetCourseValueFuzzy(courseRow, headerName)
    End If
End Function

Private Function GetCourseValueFuzzy(ByVal courseRow As Object, ByVal headerName As String) As Variant
    On Error GoTo EH

    Dim target As String
    target = NormalizeKey(headerName)

    Dim k As Variant
    For Each k In courseRow.keys
        If NormalizeKey(CStr(k)) = target Then
            GetCourseValueFuzzy = courseRow(k)
            Exit Function
        End If
    Next k

    GetCourseValueFuzzy = vbNullString
    Exit Function

EH:
    GetCourseValueFuzzy = vbNullString
End Function

Private Function PickCourseValue(ByVal courseRow As Object, ByVal candidates As Variant) As Variant
    Dim i As Long
    For i = LBound(candidates) To UBound(candidates)
        Dim v As Variant
        v = GetCourseValueByHeader(courseRow, CStr(candidates(i)))
        If Len(Trim$(CStr(v))) > 0 Then
            PickCourseValue = v
            Exit Function
        End If
    Next i
    PickCourseValue = vbNullString
End Function

' ============================================================
' Block geometry heuristics
' ============================================================

Private Function DetectBlockHeight(ByVal wsReport As Worksheet, ByVal maxBlocks As Long) As Long
    ' detect from successive tops: min positive delta between blk_i and blk_{i+1}
    On Error GoTo EH

    Dim minDelta As Long: minDelta = 0
    Dim i As Long
    For i = 1 To maxBlocks - 1
        Dim a As Range, b As Range
        Set a = GetNamedRangeCell(wsReport.Parent, "nr_blk_" & Format$(i, "00") & "_top")
        Set b = GetNamedRangeCell(wsReport.Parent, "nr_blk_" & Format$(i + 1, "00") & "_top")
        If Not a Is Nothing And Not b Is Nothing Then
            Dim d As Long: d = b.row - a.row
            If d > 0 Then
                If minDelta = 0 Or d < minDelta Then minDelta = d
            End If
        End If
    Next i

    DetectBlockHeight = minDelta
    Exit Function

EH:
    DetectBlockHeight = 0
End Function

Private Function BlockRangeByHeuristics(ByVal ws As Worksheet, ByVal topCell As Range, ByVal blockHeight As Long) As Range
    ' width: we take usedrange last col, but at least up to topCell.Column
    Dim lastCol As Long
    lastCol = ws.UsedRange.Column + ws.UsedRange.Columns.Count - 1
    If lastCol < topCell.Column Then lastCol = topCell.Column

    Set BlockRangeByHeuristics = ws.Range(ws.Cells(topCell.row, 1), ws.Cells(topCell.row + blockHeight - 1, lastCol))
End Function

' ============================================================
' Named range utilities
' ============================================================

Private Function NameExistsInWorkbook(ByVal wb As Workbook, ByVal nameText As String) As Boolean
    On Error GoTo EH
    Dim nm As Name
    Set nm = wb.names(nameText)
    NameExistsInWorkbook = True
    Exit Function
EH:
    NameExistsInWorkbook = False
End Function

Private Function GetNamedRangeCell(ByVal wb As Workbook, ByVal nameText As String) As Range
    On Error GoTo EH
    Dim nm As Name
    Set nm = wb.names(nameText)
    Set GetNamedRangeCell = nm.RefersToRange.Cells(1, 1)
    Exit Function
EH:
    Set GetNamedRangeCell = Nothing
End Function

' ============================================================
' File / folder / log
' ============================================================

Private Sub EnsureFolder(ByVal path As String)
    On Error Resume Next
    If Len(path) = 0 Then Exit Sub
    If Right$(path, 1) = "\" Then path = Left$(path, Len(path) - 1)
    If Len(dir(path, vbDirectory)) = 0 Then MkDir path
    On Error GoTo 0
End Sub

Private Function ParentFolder(ByVal fullPath As String) As String
    Dim p As Long: p = InStrRev(fullPath, "\")
    If p > 0 Then ParentFolder = Left$(fullPath, p) Else ParentFolder = ""
End Function

Private Function FileBaseName(ByVal fullPath As String) As String
    Dim s As String: s = fullPath
    Dim p As Long: p = InStrRev(s, "\")
    If p > 0 Then s = Mid$(s, p + 1)
    Dim d As Long: d = InStrRev(s, ".")
    If d > 0 Then s = Left$(s, d - 1)
    FileBaseName = s
End Function

Private Function SanitizeFileName(ByVal s As String) As String
    Dim bad As Variant, i As Long
    bad = Array("\", "/", ":", "*", "?", """", "<", ">", "|")
    For i = LBound(bad) To UBound(bad)
        s = Replace$(s, CStr(bad(i)), "_")
    Next i
    s = Replace$(s, vbCr, " ")
    s = Replace$(s, vbLf, " ")
    s = Trim$(s)
    SanitizeFileName = s
End Function

Private Sub OpenLog(ByVal runDir As String, ByVal prefix As String)
    Dim ts As String
    ts = Format$(Now, "yyyy-mm-dd_hh-nn-ss")
    mLogPath = runDir & prefix & "_" & ts & ".log"

    mLogFile = FreeFile
    Open mLogPath For Output As #mLogFile
    Print #mLogFile, "LOG " & Now
End Sub

Private Sub CloseLog()
    On Error Resume Next
    If mLogFile <> 0 Then Close #mLogFile
    mLogFile = 0
    On Error GoTo 0
End Sub

Private Sub LogLine(ByVal s As String)
    On Error Resume Next
    If mLogFile <> 0 Then Print #mLogFile, Format$(Now, "hh:nn:ss") & " | " & s
    On Error GoTo 0
End Sub

' ============================================================
' Worksheet scanning
' ============================================================

Private Function LastUsedRow(ByVal ws As Worksheet, ByVal col As Long) As Long
    On Error GoTo EH
    LastUsedRow = ws.Cells(ws.Rows.Count, col).End(xlUp).row
    Exit Function
EH:
    LastUsedRow = 0
End Function

Private Function LastUsedCol(ByVal ws As Worksheet, ByVal row As Long) As Long
    On Error GoTo EH
    LastUsedCol = ws.Cells(row, ws.Columns.Count).End(xlToLeft).Column
    Exit Function
EH:
    LastUsedCol = 0
End Function

' ============================================================
' Text normalization (simple, stable)
' ============================================================

Private Function NormalizeKey(ByVal s As String) As String
    s = LCase$(Trim$(s))
    s = ReplacePolish(s)
    s = Replace$(s, " ", "")
    s = Replace$(s, "_", "")
    s = Replace$(s, "-", "")
    s = Replace$(s, "/", "")
    s = Replace$(s, ":", "")
    s = Replace$(s, ".", "")
    s = Replace$(s, ",", "")
    NormalizeKey = s
End Function

Private Function ReplacePolish(ByVal s As String) As String
    s = Replace$(s, "ą", "a")
    s = Replace$(s, "ć", "c")
    s = Replace$(s, "ę", "e")
    s = Replace$(s, "ł", "l")
    s = Replace$(s, "ń", "n")
    s = Replace$(s, "ó", "o")
    s = Replace$(s, "ś", "s")
    s = Replace$(s, "ż", "z")
    s = Replace$(s, "ź", "z")
    ReplacePolish = s
End Function

Private Function NzStr(ByVal v As Variant) As String
    If IsError(v) Then NzStr = "" Else NzStr = CStr(v)
End Function
