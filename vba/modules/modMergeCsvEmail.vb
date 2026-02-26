' === Component: modMergeCsvEmail [Standard Module]
' === Exported: 2026-02-26 14:22:22

Option Explicit

' ============================
' CSV merge: id + dane osobowe
' email = klucz łączenia
' wynik: dane_do_raportu.csv w folderze pierwszego pliku
' ============================

Public Sub MergeCsv_ByEmail()
    On Error GoTo EH

    Dim csv1Path As String, csv2Path As String
    csv1Path = PickCsvFile("Wybierz PIERWSZY plik CSV (Pełna nazwa, E-mail, ...)")
    If Len(csv1Path) = 0 Then Exit Sub

    csv2Path = PickCsvFile("Wybierz DRUGI plik CSV (id,email)")
    If Len(csv2Path) = 0 Then Exit Sub

    Dim outPath As String
    outPath = ParentFolder(csv1Path) & "dane_do_raportu.csv"

    ' 1) wczytaj mapę email -> id z csv2
    Dim mapEmailToId As Object
    Set mapEmailToId = CreateObject("Scripting.Dictionary")
    mapEmailToId.CompareMode = 1 ' TextCompare

    Dim csv2Text As String
    csv2Text = ReadTextUtf8(csv2Path)

    Dim lines2 As Variant
    lines2 = SplitLines(csv2Text)
    If UBound(lines2) < 0 Then Err.Raise vbObjectError + 1, , "Drugi plik CSV jest pusty."

    Dim hdr2 As Variant
    Dim delim2 As String
    delim2 = DetectDelimiter(CStr(lines2(0)))
    hdr2 = ParseCsvLineEx(CStr(lines2(0)), delim2)

    Dim idx2_id As Long, idx2_email As Long
    idx2_id = FindHeaderIndex(hdr2, "id")
    idx2_email = FindHeaderIndex(hdr2, "email")

    If idx2_id < 0 Or idx2_email < 0 Then
        Err.Raise vbObjectError + 2, , "Drugi CSV musi mieć nagłówki: id,email"
    End If

    Dim i As Long
    For i = 1 To UBound(lines2)
        If Len(Trim$(CStr(lines2(i)))) = 0 Then GoTo Next2

        Dim row2 As Variant
        row2 = ParseCsvLineEx(CStr(lines2(i)), delim2)

        Dim em2 As String, id2 As String
        em2 = NormalizeEmail(GetFieldSafe(row2, idx2_email))
        id2 = Trim$(GetFieldSafe(row2, idx2_id))

        If Len(em2) > 0 Then
            ' jeśli duplikaty emaili -> ostatni wygrywa (możesz zmienić na pierwszy)
            mapEmailToId(em2) = id2
        End If
Next2:
    Next i

    ' 2) czytaj csv1 i zapisuj wynik z dodanym "id" na początku
    Dim csv1Text As String
    csv1Text = ReadTextUtf8(csv1Path)

    Dim lines1 As Variant
    lines1 = SplitLines(csv1Text)
    If UBound(lines1) < 0 Then Err.Raise vbObjectError + 3, , "Pierwszy plik CSV jest pusty."

    Dim hdr1 As Variant
    Dim delim1 As String
    delim1 = DetectDelimiter(CStr(lines1(0)))
    hdr1 = ParseCsvLineEx(CStr(lines1(0)), delim1)

    Dim idx1_email As Long
    ' w Twoim nagłówku jest "E-mail" (z myślnikiem)
    idx1_email = FindHeaderIndex(hdr1, "E-mail")
    If idx1_email < 0 Then idx1_email = FindHeaderIndex(hdr1, "Email")
    If idx1_email < 0 Then idx1_email = FindHeaderIndex(hdr1, "E-mail ") ' awaryjnie

    If idx1_email < 0 Then
        Err.Raise vbObjectError + 4, , "Pierwszy CSV musi mieć kolumnę nagłówka: E-mail"
    End If

    Dim sb As String
    sb = ""

    ' nagłówek wyjściowy: id + oryginalne nagłówki
    sb = sb & CsvJoinWithLeadingId("id", hdr1) & vbCrLf

    Dim notFound As Long, total As Long
    For i = 1 To UBound(lines1)
        Dim ln As String
        ln = CStr(lines1(i))
        If Len(Trim$(ln)) = 0 Then GoTo Next1

        total = total + 1

        Dim row1 As Variant
        row1 = ParseCsvLineEx(ln, delim1)

        Dim em1 As String
        em1 = NormalizeEmail(GetFieldSafe(row1, idx1_email))

        Dim idOut As String
        If Len(em1) > 0 And mapEmailToId.Exists(em1) Then
            idOut = CStr(mapEmailToId(em1))
        Else
            idOut = "" ' brak dopasowania -> puste id
            notFound = notFound + 1
        End If

        sb = sb & CsvJoinWithLeadingId(idOut, row1) & vbCrLf
Next1:
    Next i

    WriteTextUtf8 outPath, sb

    MsgBox "Zapisano: " & outPath & vbCrLf & _
           "Wiersze: " & total & vbCrLf & _
           "Brak dopasowania id: " & notFound, vbInformation
    Exit Sub

EH:
    MsgBox "Błąd: " & Err.Number & vbCrLf & Err.Description, vbCritical
End Sub

' ----------------------------
' UI: wybór pliku
' ----------------------------
Private Function PickCsvFile(ByVal title As String) As String
    On Error GoTo EH
    Dim fd As Object
    Set fd = Application.FileDialog(3) ' msoFileDialogFilePicker

    With fd
        .title = title
        .AllowMultiSelect = False
        .Filters.Clear
        .Filters.Add "CSV", "*.csv"
        .Filters.Add "Wszystkie pliki", "*.*"
        If .Show <> -1 Then Exit Function
        PickCsvFile = .SelectedItems(1)
    End With
    Exit Function
EH:
    PickCsvFile = ""
End Function

' ----------------------------
' UTF-8 read/write (ADODB.Stream)
' ----------------------------
Private Function ReadTextUtf8(ByVal path As String) As String
    Dim stm As Object
    Set stm = CreateObject("ADODB.Stream")
    With stm
        .Type = 2 ' adTypeText
        .Charset = "utf-8"
        .Open
        .LoadFromFile path
        ReadTextUtf8 = .ReadText(-1)
        .Close
    End With
End Function

Private Sub WriteTextUtf8(ByVal path As String, ByVal textData As String)
    Dim stm As Object
    Set stm = CreateObject("ADODB.Stream")
    With stm
        .Type = 2 ' adTypeText
        .Charset = "utf-8"
        .Open
        .WriteText textData
        .SaveToFile path, 2 ' adSaveCreateOverWrite
        .Close
    End With
End Sub

' ----------------------------
' CSV parsing (quotes-aware)
' ----------------------------
Private Function ParseCsvLineEx(ByVal line As String, ByVal delim As String) As Variant
    Dim res() As String
    Dim i As Long, ch As String
    Dim cur As String
    Dim inQ As Boolean

    ReDim res(0 To 0)
    cur = ""
    inQ = False

    i = 1
    Do While i <= Len(line)
        ch = Mid$(line, i, 1)

        If inQ Then
            If ch = """" Then
                If i < Len(line) And Mid$(line, i + 1, 1) = """" Then
                    cur = cur & """"
                    i = i + 1
                Else
                    inQ = False
                End If
            Else
                cur = cur & ch
            End If
        Else
            If ch = delim Then
                AppendField res, cur
                cur = ""
            ElseIf ch = """" Then
                inQ = True
            Else
                cur = cur & ch
            End If
        End If

        i = i + 1
    Loop

    AppendField res, cur
    ParseCsvLineEx = res
End Function

Private Sub AppendField(ByRef arr() As String, ByVal v As String)
    Dim n As Long
    n = UBound(arr)
    If n = 0 And Len(arr(0)) = 0 Then
        arr(0) = v
    Else
        ReDim Preserve arr(0 To n + 1)
        arr(n + 1) = v
    End If
End Sub

Private Function CsvJoinWithLeadingId(ByVal idVal As String, ByVal row As Variant) As String
    Dim i As Long
    Dim s As String
    s = CsvEscape(idVal)

    For i = LBound(row) To UBound(row)
        s = s & "," & CsvEscape(CStr(row(i)))
    Next i
    CsvJoinWithLeadingId = s
End Function

Private Function CsvEscape(ByVal v As String) As String
    Dim mustQ As Boolean
    mustQ = (InStr(1, v, ",", vbBinaryCompare) > 0) Or _
            (InStr(1, v, """", vbBinaryCompare) > 0) Or _
            (InStr(1, v, vbCr, vbBinaryCompare) > 0) Or _
            (InStr(1, v, vbLf, vbBinaryCompare) > 0)

    If InStr(1, v, """", vbBinaryCompare) > 0 Then
        v = Replace$(v, """", """""")
    End If

    If mustQ Then
        CsvEscape = """" & v & """"
    Else
        CsvEscape = v
    End If
End Function

' ----------------------------
' Helpers
' ----------------------------
Private Function FindHeaderIndex(ByVal headers As Variant, ByVal headerName As String) As Long
    Dim i As Long
    FindHeaderIndex = -1
    For i = LBound(headers) To UBound(headers)
        If NormalizeHeader(CStr(headers(i))) = NormalizeHeader(headerName) Then
            FindHeaderIndex = i
            Exit Function
        End If
    Next i
End Function

Private Function NormalizeHeader(ByVal s As String) As String
    s = Trim$(s)
    s = Replace$(s, Chr$(160), " ")
    s = LCase$(s)
    NormalizeHeader = s
End Function

Private Function NormalizeEmail(ByVal s As String) As String
    s = Trim$(s)
    s = Replace$(s, Chr$(160), " ")
    s = LCase$(s)
    NormalizeEmail = s
End Function

Private Function GetFieldSafe(ByVal row As Variant, ByVal idx As Long) As String
    On Error GoTo EH
    If idx < LBound(row) Or idx > UBound(row) Then Exit Function
    GetFieldSafe = CStr(row(idx))
    Exit Function
EH:
    GetFieldSafe = ""
End Function

Private Function ParentFolder(ByVal fullPath As String) As String
    Dim p As Long
    p = InStrRev(fullPath, "\")
    If p > 0 Then
        ParentFolder = Left$(fullPath, p)
    Else
        ParentFolder = ""
    End If
End Function

Private Function SplitLines(ByVal s As String) As Variant
    ' normalizacja zakończeń linii
    s = Replace$(s, vbCrLf, vbLf)
    s = Replace$(s, vbCr, vbLf)
    If Len(s) = 0 Then
        SplitLines = Array()
    Else
        SplitLines = Split(s, vbLf)
    End If
End Function
Private Function DetectDelimiter(ByVal line As String) As String
    ' prosto: który znak częściej występuje w nagłówku
    Dim cComma As Long, cSemi As Long
    cComma = Len(line) - Len(Replace$(line, ",", ""))
    cSemi = Len(line) - Len(Replace$(line, ";", ""))
    If cSemi > cComma Then DetectDelimiter = ";" Else DetectDelimiter = ","
End Function