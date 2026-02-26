' === Component: modExportVBA [Standard Module]
' === Exported: 2026-02-26 14:22:22

' === modExportVBA.bas (rozszerzenie: TXT + osobne .vb do folderu) ===
Option Explicit

Private Const vbext_ct_StdModule As Long = 1
Private Const vbext_ct_ClassModule As Long = 2
Private Const vbext_ct_MSForm As Long = 3
Private Const vbext_ct_Document As Long = 100

' --- WIDOCZNE W ALT+F8 (bez parametrów) ---
Public Sub ExportAllVBAtoSingleTxt_UI()
    ExportAllVBAtoSingleTxt_Worker ""
End Sub

Public Sub ExportAllVBAtoSingleTxt_ToFolderDesktop()
    Dim desk As String
    desk = CreateObject("WScript.Shell").SpecialFolders("Desktop")

    Dim outPath As String
    outPath = desk & "\" & SafeStr(ThisWorkbookOrDoc.VBProject.Name) & "_VBA_" & Format(Now, "yyyymmdd_HHNNSS") & ".txt"
    ExportAllVBAtoSingleTxt_Worker outPath
End Sub

' NOWE: folder + osobne .vb (i dodatkowo 1 zbiorczy .txt w tym samym folderze)
Public Sub ExportAllVBA_ToFolder_WithSeparateVB_UI()
    ExportAllVBA_ToFolder_WithSeparateVB_Worker ""
End Sub
' --- /WIDOCZNE ---


' =========================
' 1) Eksport do jednego TXT
' =========================
Private Sub ExportAllVBAtoSingleTxt_Worker(Optional ByVal outPath As String = "")
    On Error GoTo blad

    Dim proj As Object, comps As Object, c As Object
    Dim hostName As String, projName As String, whenStr As String
    hostName = Application.Name
    Set proj = ThisWorkbookOrDoc.VBProject
    Set comps = proj.VBComponents
    projName = SafeStr(proj.Name)
    whenStr = Format(Now, "yyyy-mm-dd HH:nn:ss")

    If Len(outPath) = 0 Then
        outPath = PickSaveTxt(projName & "_VBA_" & Format(Now, "yyyymmdd_HHNNSS") & ".txt")
        If Len(outPath) = 0 Then Exit Sub
    End If

    Dim sb As String
    sb = "# VBA EXPORT" & vbCrLf & _
         "# Host: " & hostName & vbCrLf & _
         "# Project: " & projName & vbCrLf & _
         "# Date: " & whenStr & vbCrLf & _
         "# Components: " & comps.Count & vbCrLf & _
         String(80, "-") & vbCrLf

    For Each c In comps
        sb = sb & ComponentBlock(c) & vbCrLf
    Next c

    SaveTextUTF8 outPath, sb
    MsgBox "Zapisano eksport VBA do:" & vbCrLf & outPath, vbInformation
    Exit Sub

blad:
    HandleVBAccessError "Błąd eksportu (TXT)", Err
End Sub


' ==========================================
' 2) NOWE: Eksport do folderu + osobne .vb
' ==========================================
Private Sub ExportAllVBA_ToFolder_WithSeparateVB_Worker(Optional ByVal rootOutDir As String = "")
    On Error GoTo blad

    Dim proj As Object, comps As Object, c As Object
    Set proj = ThisWorkbookOrDoc.VBProject
    Set comps = proj.VBComponents

    Dim hostBase As String
    hostBase = GetHostFileBaseName() ' nazwa "głównego pliku" (bez rozszerzenia)
    If Len(hostBase) = 0 Then hostBase = SafeStr(proj.Name)

    If Len(rootOutDir) = 0 Then
        rootOutDir = PickFolder("Wybierz lokalizację docelową (katalog nadrzędny):")
        If Len(rootOutDir) = 0 Then Exit Sub
    End If

    Dim stamp As String: stamp = Format(Now, "yyyymmdd_HHNNSS")
    Dim outFolder As String
    outFolder = rootOutDir & "\" & CleanFileName(hostBase) & "_VBA_" & stamp

    EnsureFolderExists outFolder
    EnsureFolderExists outFolder & "\Modules"

    ' (A) Zapis zbiorczego TXT do tego samego folderu
    Dim txtPath As String
    txtPath = outFolder & "\" & CleanFileName(hostBase) & "_VBA_" & stamp & ".txt"
    ExportAllVBAtoSingleTxt_Worker txtPath

    ' (B) Zapis każdego komponentu do osobnego .vb
    Dim savedCount As Long: savedCount = 0
    For Each c In comps
        Dim codeLines As Long: codeLines = c.CodeModule.CountOfLines

        Dim code As String
        If codeLines > 0 Then
            code = c.CodeModule.lines(1, codeLines)
        Else
            code = "" ' puste moduły też zapisujemy, żeby było widać że istnieją
        End If

        Dim header As String
        header = "' === Component: " & c.Name & " [" & ComponentTypeName(c.Type) & "]" & vbCrLf & _
                 "' === Exported: " & Format(Now, "yyyy-mm-dd HH:nn:ss") & vbCrLf & vbCrLf

        Dim vbPath As String
        vbPath = outFolder & "\Modules\" & CleanFileName(c.Name) & ".vb"

        SaveTextUTF8 vbPath, header & code
        savedCount = savedCount + 1
    Next c

    MsgBox "Zapisano folder eksportu:" & vbCrLf & outFolder & vbCrLf & vbCrLf & _
           "• TXT: " & txtPath & vbCrLf & _
           "• Pliki .vb: " & savedCount & " szt. w \Modules\", vbInformation
    Exit Sub

blad:
    HandleVBAccessError "Błąd eksportu (folder + .vb)", Err
End Sub


' === Helper: zwraca obiekt-kontener projektu (Excel: ThisWorkbook, Word: ActiveDocument)
Private Function ThisWorkbookOrDoc() As Object
    Dim o As Object

    On Error Resume Next
    Set o = CallByName(Application, "ThisWorkbook", VbGet)
    On Error GoTo 0
    If Not o Is Nothing Then
        Set ThisWorkbookOrDoc = o
        Exit Function
    End If

    On Error Resume Next
    Set o = CallByName(Application, "ActiveDocument", VbGet)
    On Error GoTo 0
    If Not o Is Nothing Then
        Set ThisWorkbookOrDoc = o
        Exit Function
    End If

    Err.Raise 5, , "Nie rozpoznano hosta (Excel/Word) – nie można uzyskać kontenera VBProject."
End Function


Private Function ComponentBlock(ByVal comp As Object) As String
    Dim kind As String: kind = ComponentTypeName(comp.Type)
    Dim codeLines As Long: codeLines = comp.CodeModule.CountOfLines
    Dim code As String: If codeLines > 0 Then code = comp.CodeModule.lines(1, codeLines)

    ComponentBlock = _
        String(80, "=") & vbCrLf & _
        "=== Component: " & comp.Name & "  [" & kind & "]" & vbCrLf & _
        String(80, "=") & vbCrLf & _
        code & vbCrLf & _
        String(80, "-") & vbCrLf
End Function

Private Function ComponentTypeName(ByVal t As Long) As String
    Select Case t
        Case vbext_ct_StdModule:   ComponentTypeName = "Standard Module"
        Case vbext_ct_ClassModule: ComponentTypeName = "Class Module"
        Case vbext_ct_MSForm:      ComponentTypeName = "UserForm"
        Case vbext_ct_Document:    ComponentTypeName = "Document/Sheet Module"
        Case Else:                 ComponentTypeName = "Unknown(" & t & ")"
    End Select
End Function


' ===== UI pickery =====
Private Function PickSaveTxt(ByVal suggestName As String) As String
    On Error GoTo blad
    With Application.FileDialog(msoFileDialogSaveAs)
        .title = "Gdzie zapisać eksport VBA jako TXT?"
        .InitialFileName = suggestName
        .Filters.Clear
        .Filters.Add "Pliki tekstowe (*.txt)", "*.txt"
        If .Show Then PickSaveTxt = .SelectedItems(1)
    End With
    Exit Function
blad:
    PickSaveTxt = ""
End Function

Private Function PickFolder(ByVal title As String) As String
    On Error GoTo blad
    With Application.FileDialog(msoFileDialogFolderPicker)
        .title = title
        If .Show Then PickFolder = .SelectedItems(1)
    End With
    Exit Function
blad:
    PickFolder = ""
End Function


' ===== Zapis UTF-8 =====
Private Sub SaveTextUTF8(ByVal path As String, ByVal textData As String)
    Dim stm As Object: Set stm = CreateObject("ADODB.Stream")
    With stm
        .Type = 2           ' adTypeText
        .Charset = "utf-8"
        .Open
        .WriteText textData
        .SaveToFile path, 2 ' adSaveCreateOverWrite
        .Close
    End With
End Sub


' ===== Foldery/ścieżki =====
Private Sub EnsureFolderExists(ByVal folderPath As String)
    If Len(dir$(folderPath, vbDirectory)) = 0 Then
        MkDirRecursive folderPath
    End If
End Sub

Private Sub MkDirRecursive(ByVal folderPath As String)
    Dim fso As Object: Set fso = CreateObject("Scripting.FileSystemObject")
    If fso.FolderExists(folderPath) Then Exit Sub
    fso.CreateFolder folderPath
End Sub

Private Function GetHostFileBaseName() As String
    ' Excel: ThisWorkbook.Name, Word: ActiveDocument.Name
    On Error Resume Next

    Dim nm As String
    nm = ""
    nm = CallByName(ThisWorkbookOrDoc, "Name", VbGet)

    On Error GoTo 0
    If Len(nm) = 0 Then Exit Function

    Dim p As Long: p = InStrRev(nm, ".")
    If p > 1 Then
        GetHostFileBaseName = Left$(nm, p - 1)
    Else
        GetHostFileBaseName = nm
    End If
End Function

Private Function CleanFileName(ByVal s As String) As String
    ' usuwa znaki niedozwolone w nazwach plików Windows
    Dim bad As Variant, i As Long
    bad = Array("\", "/", ":", "*", "?", """", "<", ">", "|")
    CleanFileName = s
    For i = LBound(bad) To UBound(bad)
        CleanFileName = Replace(CleanFileName, bad(i), "_")
    Next i
    CleanFileName = Trim$(CleanFileName)
    If Len(CleanFileName) = 0 Then CleanFileName = "NONAME"
End Function

Private Function SafeStr(ByVal s As String) As String
    SafeStr = Replace(Replace(s, vbCr, " "), vbLf, " ")
End Function


' ===== Obsługa błędów dostępu do VBProject =====
Private Sub HandleVBAccessError(ByVal caption As String, ByVal e As ErrObject)
    If e.Number = 1004 Or e.Number = 70 Then
        MsgBox "Brak dostępu do projektu VBA." & vbCrLf & _
               "Włącz: Plik › Opcje › Centrum zaufania › Ustawienia… › Ustawienia makr ›" & vbCrLf & _
               "„Ufaj dostępowi do modelu obiektowego projektu VBA”.", vbExclamation, caption
    Else
        MsgBox caption & ":" & vbCrLf & e.Number & " - " & e.Description, vbCritical
    End If
End Sub



