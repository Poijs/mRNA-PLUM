' === Component: modNA_PdfEngine [Standard Module]
' === Exported: 2026-02-26 14:22:22

Option Explicit

' =========================
' KONFIG
' =========================
Private Const SHEET_KURSY As String = "DANE_KURSY"
Private Const SHEET_PERS  As String = "DANE_PERS"

Private Const REPORT_SHEET As String = "Raport_NA" ' <-- potwierdziłeś

Private Const MAX_BLOCKS As Long = 16

' PDF / wydruk
Private Const MARGIN_CM As Double = 1.5
Private Const PERCENT_FORMAT As String = "0,0%"

Private mLogFile As Integer
Private mLogPath As String

' =========================
' START
' =========================
Public Sub RaportyNAPDF()
    On Error GoTo EH
    Application.ScreenUpdating = False
    Application.EnableEvents = False
    Application.Calculation = xlCalculationManual

    Dim wzorPath As String, srcFolder As String, outFolder As String

    wzorPath = PickReportTemplate()
    If Len(wzorPath) = 0 Then GoTo CleanUp

    srcFolder = PickSourceFolder()
    If Len(srcFolder) = 0 Then GoTo CleanUp
    If Right$(srcFolder, 1) <> "\" Then srcFolder = srcFolder & "\"

    outFolder = srcFolder & "raporty_NA_pdf\"
    EnsureFolder outFolder

    Dim f As String
    f = dir$(srcFolder & "*.xlsx")

    If Len(f) = 0 Then
        MsgBox "Brak plików .xlsx w folderze.", vbExclamation
        GoTo CleanUp
    End If

    Do While Len(f) > 0
        ProcessOneSourceFile srcFolder & f, wzorPath, outFolder
        f = dir$
    Loop

    MsgBox "Zakończone. PDF-y zapisane w: " & outFolder, vbInformation

CleanUp:
    Application.ScreenUpdating = True
    Application.EnableEvents = True
    Application.Calculation = xlCalculationAutomatic
    Exit Sub

EH:
    MsgBox "Błąd: " & Err.Number & " - " & Err.Description, vbCritical
    Resume CleanUp
End Sub

' =========================
' 1 PLIK -> 1 PDF
' =========================
Private Sub ProcessOneSourceFile(ByVal srcPath As String, ByVal wzorPath As String, ByVal outFolder As String)
    On Error GoTo EH

    Dim wbSrc As Workbook, wbTpl As Workbook, wbOut As Workbook
    Dim wsK As Worksheet, wsP As Worksheet, wsOut As Worksheet

    Dim runFolder As String, logPath As String
    runFolder = ParentFolderWithBackslash(outFolder) & "_run\"
    EnsureFolder runFolder

    logPath = runFolder & "pdf_" & Format$(Now, "yyyymmdd_hhnnss") & "_" & SanitizeFileName(Replace$(dir$(srcPath), ".xlsx", "")) & ".log"
    LogOpen logPath
    LogLine "START src=" & srcPath

    ' --- open source (read-only) ---
    Set wbSrc = Workbooks.Open(srcPath, ReadOnly:=True, UpdateLinks:=0, AddToMru:=False)
    Set wsK = GetWsOrNothing(wbSrc, SHEET_KURSY)
    Set wsP = GetWsOrNothing(wbSrc, SHEET_PERS)

    If wsK Is Nothing Then
        LogLine "ERROR: missing sheet: " & SHEET_KURSY
        GoTo CloseAndExit
    End If

    ' --- open template (read-only) and SaveCopyAs ---
    Set wbTpl = Workbooks.Open(wzorPath, ReadOnly:=True, UpdateLinks:=0, AddToMru:=False)

    Dim tmpPath As String
    tmpPath = outFolder & "~tmp_" & Format$(Now, "yyyymmdd_hhnnss") & "_" & SanitizeFileName(Replace$(wbSrc.Name, ".xlsx", "")) & ".xlsx"

    Application.DisplayAlerts = False
    wbTpl.SaveCopyAs tmpPath
    Application.DisplayAlerts = True

    wbTpl.Close SaveChanges:=False
    Set wbTpl = Nothing

    ' --- open working copy (writable) ---
    Set wbOut = Workbooks.Open(tmpPath, ReadOnly:=False, UpdateLinks:=0, AddToMru:=False)
    Set wsOut = GetWsOrNothing(wbOut, REPORT_SHEET)
    If wsOut Is Nothing Then
        LogLine "ERROR: missing report sheet: " & REPORT_SHEET
        GoTo CloseAndExit
    End If

    ' 1) DANE_PERS -> NamedRanges
    If Not wsP Is Nothing Then
        FillNamedRangesFromDanePers wbOut, wsP
    Else
        LogLine "WARN: missing sheet: " & SHEET_PERS
    End If

    ' 2) Kursy -> 16 bloków
    FillAllCourseBlocks wbOut, wsOut, wsK, MAX_BLOCKS

    ' 3) Layout + Print settings
    ApplyLayout wsOut

    ' 4) PDF name (prefer NamedRanges, fallback to source filename)
    Dim nauczyciel As String, bazusID As String, base As String
    nauczyciel = CStr(GetNameValueOrEmpty(wbOut, "nr_meta_Nauczyciel"))
    bazusID = CStr(GetNameValueOrEmpty(wbOut, "nr_meta_BazusID"))

    base = Replace$(wbSrc.Name, ".xlsx", "")
    If Len(Trim$(nauczyciel)) > 0 Then base = Trim$(nauczyciel)
    If Len(Trim$(bazusID)) > 0 Then base = base & "_" & Trim$(bazusID)

    base = SanitizeFileName(base)
    If Len(base) > 180 Then base = Left$(base, 180)

    Dim outPdfPath As String
    outPdfPath = outFolder & base & ".pdf"
    LogLine "Export PDF -> " & outPdfPath

    wsOut.ExportAsFixedFormat Type:=xlTypePDF, _
                              fileName:=outPdfPath, _
                              Quality:=xlQualityStandard, _
                              IncludeDocProperties:=True, _
                              IgnorePrintAreas:=False, _
                              OpenAfterPublish:=False

    LogLine "OK"

CloseAndExit:
    On Error Resume Next
    If Not wbOut Is Nothing Then wbOut.Close SaveChanges:=False
    If Len(tmpPath) > 0 Then Kill tmpPath
    If Not wbTpl Is Nothing Then wbTpl.Close SaveChanges:=False
    If Not wbSrc Is Nothing Then wbSrc.Close SaveChanges:=False
    LogClose
    On Error GoTo 0
    Exit Sub

EH:
    LogLine "FAIL Err=" & Err.Number & " | " & Err.Description
    Resume CloseAndExit
End Sub

' =========================
' BLOKI KURSÓW
' =========================
Private Sub FillAllCourseBlocks(ByVal wbOut As Workbook, ByVal wsOut As Worksheet, ByVal wsK As Worksheet, ByVal maxBlocks As Long)
    On Error GoTo EH

    Dim courses As Collection
    Set courses = LoadDistinctCourses(wsK) ' kolekcja stringów (display name)

    Dim nAll As Long: nAll = courses.Count
    Dim nUse As Long: nUse = nAll
    If nUse > maxBlocks Then
        LogLine "WARN: overflow courses " & nUse & " -> truncate to " & maxBlocks
        nUse = maxBlocks
    End If

    ' słownik metryk: key = courseKey|activityKey -> Array(count, pct)
    Dim metrics As Object
    Set metrics = LoadCourseMetrics(wsK)

    Dim b As Long
    For b = 1 To maxBlocks
        If b <= nUse Then
            Dim courseName As String
            courseName = CStr(courses(b))

            FillOneCourseBlock wbOut, wsOut, b, courseName, metrics
        Else
            ClearOneCourseBlock wbOut, wsOut, b
        End If
    Next b

    ' pagebreaks: #2,#5,#8,#11,#14 (wg PROMPT 9)
    SetupPageBreaks_ByTopAnchors wbOut, wsOut, maxBlocks

    ' print area: do końca bloku nUse
    SetPrintAreaToLastUsedBlock_ByTopAnchors wbOut, wsOut, nUse, maxBlocks

    Exit Sub

EH:
    Err.Raise Err.Number, "FillAllCourseBlocks", Err.Description
End Sub

Private Sub FillOneCourseBlock(ByVal wb As Workbook, ByVal ws As Worksheet, ByVal blockNo As Long, ByVal courseName As String, ByVal metrics As Object)
    On Error GoTo EH

    Dim sNo As String: sNo = Format$(blockNo, "00")

    ' --- nagłówek bloku przez NamedRanges ---
    SafeSetNameValue wb, "nr_blk_" & sNo & "_top", blockNo
    SafeSetNameValue wb, "nr_blk_" & sNo & "_CourseName", courseName

    ' ID / liczby: jeśli masz je w DANE_KURSY (np. specjalne wiersze),
    ' to najprościej dostarczyć je jako NamedRanges w DANE_PERS.
    ' Ale jeżeli masz w DANE_KURSY kolumny dla ID/studentów/nauczycieli,
    ' dopisz tu mapowanie. Na razie zostawiamy puste (albo "-").
    SafeSetNameValue wb, "nr_blk_" & sNo & "_CourseID", ""
    SafeSetNameValue wb, "nr_blk_" & sNo & "_Studenci", ""
    SafeSetNameValue wb, "nr_blk_" & sNo & "_Nauczyciele", ""
    SafeSetNameValue wb, "nr_blk_" & sNo & "_Aktywni", ""

    ' --- zakres bloku: bierzemy od nr_blk_XX_top do tuż przed kolejnym top (albo wysokość jak blok 1) ---
    Dim rngBlock As Range
    Set rngBlock = GetBlockRangeByTopAnchors(wb, ws, blockNo, MAX_BLOCKS)

    ' --- aktywności: wypełnij po etykietach w obrębie bloku (działa dla układu lewa/prawa tabela) ---
    FillActivitiesInsideBlock rngBlock, courseName, metrics

    Exit Sub

EH:
    Err.Raise Err.Number, "FillOneCourseBlock", Err.Description
End Sub

Private Sub ClearOneCourseBlock(ByVal wb As Workbook, ByVal ws As Worksheet, ByVal blockNo As Long)
    Dim sNo As String: sNo = Format$(blockNo, "00")

    SafeSetNameValue wb, "nr_blk_" & sNo & "_CourseName", ""
    SafeSetNameValue wb, "nr_blk_" & sNo & "_CourseID", ""
    SafeSetNameValue wb, "nr_blk_" & sNo & "_Studenci", ""
    SafeSetNameValue wb, "nr_blk_" & sNo & "_Nauczyciele", ""
    SafeSetNameValue wb, "nr_blk_" & sNo & "_Aktywni", ""

    ' wyczyść wartości liczbowe/procenty w tabelach aktywności w obrębie bloku
    On Error Resume Next
    Dim rngBlock As Range
    Set rngBlock = GetBlockRangeByTopAnchors(wb, ws, blockNo, MAX_BLOCKS)
    On Error GoTo 0
    If rngBlock Is Nothing Then Exit Sub

    ClearActivityValuesInsideBlock rngBlock
End Sub

' -------------------------
' Aktywności w bloku: znajdź etykiety i wpisz wartości do komórek obok
' -------------------------
Private Sub FillActivitiesInsideBlock(ByVal rngBlock As Range, ByVal courseName As String, ByVal metrics As Object)
    ' Założenie: w wierszu aktywności jest komórka z etykietą (np. "Rozdziały w książce"),
    ' a po prawej w tym samym wierszu są kolumny: Ilość, % w skali kursu.
    ' Działa także gdy blok ma DWIE tabele (lewa i prawa), bo etykiety są w obu miejscach.

    Dim activities As Variant
    activities = Array( _
        "Rozdziały w książce", "Strony lekcji", "Strona", "Tekst i media", _
        "Wpisy do bazy danych", "Pojęcia w słowniku", "Adres URL", _
        "Pliki i foldery", "H5P", "Wpisy w Wiki", _
        "Utworzone pytania", "Ocenione zadań", "Ocenione zadan", "Ocenione zadania", _
        "Spotkania MS Teams", "Opinia zwrotna", "Głosowanie", _
        "Wiadomości na czacie", "Wpisy na forum" _
    )

    Dim i As Long
    For i = LBound(activities) To UBound(activities)
        Dim actLabel As String: actLabel = CStr(activities(i))

        Dim c As Range
        For Each c In rngBlock.Cells
            If NormalizeFuzzy(CStr(c.value)) = NormalizeFuzzy(actLabel) Then
                ' komórka etykiety może być scalona
                Dim labArea As Range: Set labArea = MergeAreaOrSelf(c)
                Dim rowNum As Long: rowNum = labArea.row

                ' znajdź w tym wierszu “Ilość” i “% w skali kursu” w ramach tej tabeli
                ' heurystyka: bierzemy pierwsze dwie NIEPuste komórki na prawo od etykiety,
                ' które nie są tekstem-nagłówkiem.
                Dim tgtCount As Range, tgtPct As Range
                Set tgtCount = FindNextValueCellRight(labArea)
                If Not tgtCount Is Nothing Then Set tgtPct = FindNextValueCellRight(tgtCount.MergeArea)

                Dim key As String
                key = NormalizeFuzzy(courseName) & "|" & NormalizeFuzzy(actLabel)

                If metrics.Exists(key) Then
                    Dim arr As Variant: arr = metrics(key)
                    PutValueOrDash tgtCount, arr(0), False
                    PutValueOrDash tgtPct, arr(1), True
                Else
                    PutValueOrDash tgtCount, Empty, False
                    PutValueOrDash tgtPct, Empty, True
                End If
            End If
        Next c
    Next i
End Sub

Private Sub ClearActivityValuesInsideBlock(ByVal rngBlock As Range)
    ' Czyścimy tylko komórki, które wyglądają jak pola Ilość/% (czyli nie teksty).
    Dim c As Range
    For Each c In rngBlock.Cells
        If c.MergeCells Then
            If c.Address <> c.MergeArea.Cells(1, 1).Address Then GoTo NextC
        End If

        Dim t As String: t = Trim$(CStr(c.value))
        If Len(t) = 0 Then GoTo NextC

        ' jeśli to tekst etykiety lub nagłówek - pomijamy
        If Not IsNumericLike(t) And InStr(1, t, "%", vbTextCompare) = 0 Then GoTo NextC

        c.MergeArea.ClearContents
NextC:
    Next c
End Sub

' =========================
' DANE: kursy i metryki
' =========================
Private Function LoadDistinctCourses(ByVal wsK As Worksheet) As Collection
    Dim col As New Collection
    Dim dict As Object: Set dict = CreateObject("Scripting.Dictionary")
    dict.CompareMode = vbTextCompare

    Dim lastR As Long: lastR = LastRow(wsK)
    Dim r As Long
    For r = 2 To lastR
        Dim courseName As String
        courseName = Trim$(CStr(wsK.Cells(r, 1).value))
        If Len(courseName) > 0 Then
            Dim key As String: key = NormalizeFuzzy(courseName)
            If Not dict.Exists(key) Then
                dict.Add key, True
                col.Add courseName
            End If
        End If
    Next r

    Set LoadDistinctCourses = col
End Function

Private Function LoadCourseMetrics(ByVal wsK As Worksheet) As Object
    Dim dict As Object: Set dict = CreateObject("Scripting.Dictionary")
    dict.CompareMode = vbTextCompare

    Dim lastR As Long: lastR = LastRow(wsK)
    Dim r As Long
    For r = 2 To lastR
        Dim courseName As String, actLabel As String
        courseName = Trim$(CStr(wsK.Cells(r, 1).value))
        actLabel = Trim$(CStr(wsK.Cells(r, 2).value))
        If Len(courseName) = 0 Or Len(actLabel) = 0 Then GoTo NextR

        Dim cnt As Variant, pct As Variant
        cnt = wsK.Cells(r, 3).value
        pct = wsK.Cells(r, 4).value

        Dim key As String
        key = NormalizeFuzzy(courseName) & "|" & NormalizeFuzzy(actLabel)

        dict(key) = Array(cnt, pct)
NextR:
    Next r

    Set LoadCourseMetrics = dict
End Function

' =========================
' PageBreaks + PrintArea oparte o nr_blk_XX_top
' =========================
Private Sub SetupPageBreaks_ByTopAnchors(ByVal wb As Workbook, ByVal ws As Worksheet, ByVal maxBlocks As Long)
    On Error Resume Next
    ws.ResetAllPageBreaks
    On Error GoTo 0

    Dim breaks As Variant
    breaks = Array(2, 5, 8, 11, 14)

    Dim i As Long, b As Long
    For i = LBound(breaks) To UBound(breaks)
        b = CLng(breaks(i))
        If b >= 1 And b <= maxBlocks Then
            Dim topCell As Range
            Set topCell = GetNameRangeOrNothing(wb, "nr_blk_" & Format$(b, "00") & "_top")
            If Not topCell Is Nothing Then
                ws.HPageBreaks.Add Before:=ws.Rows(topCell.row)
            End If
        End If
    Next i
End Sub

Private Sub SetPrintAreaToLastUsedBlock_ByTopAnchors(ByVal wb As Workbook, ByVal ws As Worksheet, ByVal nCourses As Long, ByVal maxBlocks As Long)
    If nCourses < 1 Then
        ws.PageSetup.PrintArea = ws.UsedRange.Address
        Exit Sub
    End If
    If nCourses > maxBlocks Then nCourses = maxBlocks

    Dim topLast As Range
    Set topLast = GetNameRangeOrNothing(wb, "nr_blk_" & Format$(nCourses, "00") & "_top")
    If topLast Is Nothing Then
        ws.PageSetup.PrintArea = ws.UsedRange.Address
        Exit Sub
    End If

    ' wysokość bloku: różnica top(2) - top(1) (stały układ)
    Dim h As Long: h = GetBlockHeightFromAnchors(wb)
    If h <= 0 Then
        ws.PageSetup.PrintArea = ws.UsedRange.Address
        Exit Sub
    End If

    Dim LastRow As Long
    LastRow = topLast.row + h - 1

    Dim lastCol As Long
    lastCol = ws.UsedRange.Column + ws.UsedRange.Columns.Count - 1

    ws.PageSetup.PrintArea = ws.Range(ws.Cells(1, 1), ws.Cells(LastRow, lastCol)).Address
End Sub

Private Function GetBlockRangeByTopAnchors(ByVal wb As Workbook, ByVal ws As Worksheet, ByVal blockNo As Long, ByVal maxBlocks As Long) As Range
    Dim topCell As Range
    Set topCell = GetNameRangeOrNothing(wb, "nr_blk_" & Format$(blockNo, "00") & "_top")
    If topCell Is Nothing Then Exit Function

    Dim h As Long: h = GetBlockHeightFromAnchors(wb)
    If h <= 0 Then Exit Function

    ' szerokość bierzemy z UsedRange kolumn (praktycznie cały blok jest w tym zakresie)
    Dim w As Long
    w = ws.UsedRange.Columns.Count
    If w < 1 Then w = 30

    Set GetBlockRangeByTopAnchors = ws.Range(topCell, topCell.Offset(h - 1, w - 1))
End Function

Private Function GetBlockHeightFromAnchors(ByVal wb As Workbook) As Long
    Dim t1 As Range, t2 As Range
    Set t1 = GetNameRangeOrNothing(wb, "nr_blk_01_top")
    Set t2 = GetNameRangeOrNothing(wb, "nr_blk_02_top")
    If t1 Is Nothing Or t2 Is Nothing Then Exit Function
    GetBlockHeightFromAnchors = t2.row - t1.row
End Function

' =========================
' NamedRanges: DANE_PERS -> workbook.Names
' =========================
Private Sub FillNamedRangesFromDanePers(ByVal templateWb As Workbook, ByVal danePersWs As Worksheet)
    On Error GoTo EH

    Dim lastR As Long: lastR = LastRow(danePersWs)
    If lastR < 1 Then Exit Sub

    Dim startRow As Long: startRow = 1
    If LCase$(Trim$(CStr(danePersWs.Cells(1, 1).value))) = "name" Then startRow = 2

    Dim r As Long
    For r = startRow To lastR
        Dim nm As String: nm = Trim$(CStr(danePersWs.Cells(r, 1).value))
        If Len(nm) = 0 Then GoTo NextR

        Dim v As Variant: v = danePersWs.Cells(r, 2).value
        If NameExists(templateWb, nm) Then
            On Error Resume Next
            templateWb.names(nm).RefersToRange.value = v
            If Err.Number <> 0 Then
                LogLine "WARN: NamedRange set failed '" & nm & "' Err=" & Err.Number
                Err.Clear
            End If
            On Error GoTo EH
        End If
NextR:
    Next r
    Exit Sub

EH:
    Err.Raise Err.Number, "FillNamedRangesFromDanePers", Err.Description
End Sub

Private Sub SafeSetNameValue(ByVal wb As Workbook, ByVal nm As String, ByVal v As Variant)
    If Not NameExists(wb, nm) Then Exit Sub
    On Error Resume Next
    wb.names(nm).RefersToRange.value = v
    On Error GoTo 0
End Sub

Private Function GetNameValueOrEmpty(ByVal wb As Workbook, ByVal nm As String) As Variant
    On Error GoTo EH
    If Not NameExists(wb, nm) Then Exit Function
    GetNameValueOrEmpty = wb.names(nm).RefersToRange.value
    Exit Function
EH:
    GetNameValueOrEmpty = Empty
End Function

Private Function NameExists(ByVal wb As Workbook, ByVal nameText As String) As Boolean
    On Error GoTo EH
    Dim n As Name
    Set n = wb.names(nameText)
    NameExists = True
    Exit Function
EH:
    NameExists = False
End Function

Private Function GetNameRangeOrNothing(ByVal wb As Workbook, ByVal nm As String) As Range
    On Error GoTo EH
    If Not NameExists(wb, nm) Then Exit Function
    Set GetNameRangeOrNothing = wb.names(nm).RefersToRange
    Exit Function
EH:
End Function

' =========================
' Layout
' =========================
Private Sub ApplyLayout(ByVal ws As Worksheet)
    With ws.PageSetup
        .PaperSize = xlPaperA4
        .Orientation = xlPortrait
        .LeftMargin = Application.CentimetersToPoints(MARGIN_CM)
        .RightMargin = Application.CentimetersToPoints(MARGIN_CM)
        .TopMargin = Application.CentimetersToPoints(MARGIN_CM)
        .BottomMargin = Application.CentimetersToPoints(MARGIN_CM)
        .Zoom = False
        .FitToPagesWide = 1
        .FitToPagesTall = False
    End With
    ws.UsedRange.WrapText = True
End Sub

' =========================
' PutValue / helpers
' =========================
Private Sub PutValueOrDash(ByVal tgt As Range, ByVal v As Variant, ByVal isPercent As Boolean)
    If tgt Is Nothing Then Exit Sub

    Dim area As Range
    Set area = tgt.MergeArea

    If IsEmpty(v) Or v = "" Then
        area.NumberFormat = "General"
        area.Cells(1, 1).value = "-"
        Exit Sub
    End If

    If isPercent Then
        Dim x As Double, s As String

        If IsNumeric(v) Then
            x = CDbl(v)
        Else
            s = Trim$(CStr(v))
            s = Replace$(s, "%", "")
            s = Replace$(s, Chr$(160), " ")
            s = Replace$(s, " ", "")

            If Application.DecimalSeparator = "," Then
                s = Replace$(s, ".", ",")
            Else
                s = Replace$(s, ",", ".")
            End If

            If Not IsNumeric(s) Then
                area.NumberFormat = "General"
                area.Cells(1, 1).value = "-"
                Exit Sub
            End If
            x = CDbl(s)
        End If

        If x > 1# Then x = x / 100#
        area.Cells(1, 1).value = x
        area.NumberFormat = PERCENT_FORMAT
    Else
        If IsNumeric(v) Then
            area.Cells(1, 1).value = CDbl(v)
            area.NumberFormat = "General"
        Else
            area.NumberFormat = "General"
            area.Cells(1, 1).value = "-"
        End If
    End If
End Sub

Private Function FindNextValueCellRight(ByVal fromArea As Range) As Range
    ' Szuka pierwszej "sensownej" komórki na prawo od etykiety, w tym samym wierszu.
    ' Obsługa scaleń: przechodzimy po kolumnach od końca mergeArea.
    Dim ws As Worksheet: Set ws = fromArea.Worksheet
    Dim r As Long: r = fromArea.row
    Dim startCol As Long: startCol = fromArea.Column + fromArea.Columns.Count

    Dim c As Long
    For c = startCol To ws.Columns.Count
        Dim cc As Range: Set cc = ws.Cells(r, c)
        Dim ma As Range: Set ma = MergeAreaOrSelf(cc)

        ' pomijamy jeśli to ewidentny tekst-nagłówek
        Dim t As String: t = Trim$(CStr(ma.Cells(1, 1).value))
        If NormalizeFuzzy(t) Like "*rodzaj udostepnionej*" Then GoTo NextC
        If NormalizeFuzzy(t) Like "*ilosc*" Then GoTo NextC
        If InStr(1, t, "%", vbTextCompare) > 0 And InStr(1, NormalizeFuzzy(t), "skali", vbTextCompare) > 0 Then GoTo NextC

        Set FindNextValueCellRight = ma
        Exit Function
NextC:
    Next c
End Function

Private Function MergeAreaOrSelf(ByVal c As Range) As Range
    If c.MergeCells Then
        Set MergeAreaOrSelf = c.MergeArea
    Else
        Set MergeAreaOrSelf = c
    End If
End Function

Private Function IsNumericLike(ByVal s As String) As Boolean
    Dim t As String
    t = Replace$(s, "%", "")
    t = Replace$(t, " ", "")
    t = Replace$(t, Chr$(160), "")
    If Len(t) = 0 Then Exit Function
    If Application.DecimalSeparator = "," Then
        t = Replace$(t, ".", ",")
    Else
        t = Replace$(t, ",", ".")
    End If
    IsNumericLike = IsNumeric(t)
End Function

Private Function NormalizeFuzzy(ByVal s As String) As String
    s = Replace$(s, vbCr, " ")
    s = Replace$(s, vbLf, " ")
    s = Replace$(s, Chr$(160), " ")
    s = Trim$(Application.WorksheetFunction.Trim(s))
    s = LCase$(s)
    s = ReplacePolish(s)
    NormalizeFuzzy = s
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

' =========================
' Tools: sheets, picker, folder, lastrow, sanitize
' =========================
Private Function GetWsOrNothing(ByVal wb As Workbook, ByVal wsName As String) As Worksheet
    On Error Resume Next
    Set GetWsOrNothing = wb.Worksheets(wsName)
    On Error GoTo 0
End Function

Private Function PickReportTemplate() As String
    Dim fd As FileDialog
    Set fd = Application.FileDialog(msoFileDialogFilePicker)
    With fd
        .title = "Wskaż plik wzoru raportu (.xlsx)"
        .Filters.Clear
        .Filters.Add "Excel", "*.xlsx"
        .AllowMultiSelect = False
        If .Show = -1 Then PickReportTemplate = .SelectedItems(1)
    End With
End Function

Private Function PickSourceFolder() As String
    Dim fd As FileDialog
    Set fd = Application.FileDialog(msoFileDialogFolderPicker)
    With fd
        .title = "Wybierz folder z plikami źródłowymi (.xlsx)"
        If .Show = -1 Then PickSourceFolder = .SelectedItems(1)
    End With
End Function

Private Sub EnsureFolder(ByVal path As String)
    Dim fso As Object
    If Len(path) = 0 Then Exit Sub
    If Right$(path, 1) = "\" Then path = Left$(path, Len(path) - 1)
    Set fso = CreateObject("Scripting.FileSystemObject")
    If Not fso.FolderExists(path) Then fso.CreateFolder path
End Sub

Private Function SanitizeFileName(ByVal s As String) As String
    Dim bad: bad = Array("/", "\", ":", "*", "?", """", "<", ">", "|")
    Dim i As Long
    For i = LBound(bad) To UBound(bad)
        s = Replace$(s, bad(i), "-")
    Next i
    SanitizeFileName = s
End Function

Private Function LastRow(ByVal ws As Worksheet) As Long
    LastRow = ws.Cells(ws.Rows.Count, 1).End(xlUp).row
End Function

Private Function ParentFolderWithBackslash(ByVal folderPath As String) As String
    Dim p As String: p = folderPath
    If Right$(p, 1) = "\" Then p = Left$(p, Len(p) - 1)
    Dim i As Long: i = InStrRev(p, "\")
    If i > 0 Then ParentFolderWithBackslash = Left$(p, i)
End Function

' =========================
' Logging
' =========================


Private Sub LogOpen(ByVal path As String)
    mLogPath = path
    mLogFile = FreeFile
    Open mLogPath For Output As #mLogFile
    Print #mLogFile, "LOG " & Now
End Sub

Private Sub LogLine(ByVal s As String)
    On Error Resume Next
    If mLogFile <> 0 Then Print #mLogFile, Format$(Now, "hh:nn:ss") & " | " & s
    On Error GoTo 0
End Sub

Private Sub LogClose()
    On Error Resume Next
    If mLogFile <> 0 Then Close #mLogFile
    mLogFile = 0
    On Error GoTo 0
End Sub
