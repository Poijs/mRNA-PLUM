' === Component: modNA_Convert [Standard Module]
' === Exported: 2026-02-26 14:22:22

Option Explicit

Public Sub ConvertXlsxFolderToCsv(ByVal sourceFolder As String, ByVal rootPath As String)

    On Error GoTo EH

    sourceFolder = Trim$(Replace(sourceFolder, "/", "\"))
    If Right$(sourceFolder, 1) = "\" Then sourceFolder = Left$(sourceFolder, Len(sourceFolder) - 1)

    If Len(sourceFolder) = 0 Then
        MsgBox "Nie wskazano folderu.", vbExclamation
        Exit Sub
    End If

    Dim fso As Object
    Set fso = CreateObject("Scripting.FileSystemObject")

    If Not fso.FolderExists(sourceFolder) Then
        MsgBox "Folder nie istnieje: " & sourceFolder, vbExclamation
        Exit Sub
    End If

    Dim targetFolder As String
    targetFolder = rootPath & "\_data\converted_csv"
    EnsureFolder targetFolder

    Application.ScreenUpdating = False
    Application.DisplayAlerts = False

    ProcessFolderRecursive sourceFolder, targetFolder

    Application.DisplayAlerts = True
    Application.ScreenUpdating = True

    MsgBox "Konwersja zakończona.", vbInformation
    Exit Sub

EH:
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    MsgBox "Błąd konwersji: " & Err.Description, vbCritical
End Sub


Private Sub ProcessFolderRecursive(ByVal folderPath As String, ByVal targetFolder As String)

    Dim fso As Object
    Set fso = CreateObject("Scripting.FileSystemObject")

    Dim f As Object
    Dim subF As Object

    For Each f In fso.GetFolder(folderPath).files
        If LCase(fso.GetExtensionName(f.Name)) = "xlsx" Then
            ConvertOneFile f.path, targetFolder
        End If
    Next f

    For Each subF In fso.GetFolder(folderPath).SubFolders
        ProcessFolderRecursive subF.path, targetFolder
    Next subF

End Sub


Private Sub ConvertOneFile(ByVal fullPath As String, ByVal targetFolder As String)

    Dim wb As Workbook
    Dim csvPath As String

    Set wb = Workbooks.Open(fullPath, ReadOnly:=True)

    csvPath = targetFolder & "\" & _
              Replace(wb.Name, ".xlsx", ".csv")

    wb.SaveAs fileName:=csvPath, _
              FileFormat:=xlCSVUTF8

    wb.Close False

End Sub


Private Sub EnsureFolder(ByVal folderPath As String)
    Dim fso As Object
    Set fso = CreateObject("Scripting.FileSystemObject")
    If Not fso.FolderExists(folderPath) Then
        fso.CreateFolder folderPath
    End If
End Sub
