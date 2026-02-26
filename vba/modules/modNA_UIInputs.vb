' === Component: modNA_UIInputs [Standard Module]
' === Exported: 2026-02-26 14:22:22

Option Explicit

Private Const KEYS_REL As String = "_data\KEYS.xlsx"

Public Sub UI_LoadDefaults()
    On Error Resume Next
    With raporty_plum
        .txtLogsFolder.text = GetSetting("mRNA-PLUM", "Paths", "LogsFolder", "")
        .txtTemplateXlsx.text = ""
        .txtKeysXlsx.text = GetSetting("mRNA-PLUM", "Paths", "KeysXlsx", ThisWorkbook.path & "\_data\KEYS.xlsx")
    End With
    On Error GoTo 0
End Sub

Public Sub UI_SaveDefaults()
    On Error Resume Next
    SaveSetting "mRNA-PLUM", "Paths", "LogsFolder", raporty_plum.txtLogsFolder.text
    SaveSetting "mRNA-PLUM", "Paths", "KeysXlsx", raporty_plum.txtKeysXlsx.text
    On Error GoTo 0
End Sub

Public Function UI_ValidateInputs(ByRef errMsg As String) As Boolean

    Dim logsFolder As String, templateXlsx As String, keysXlsx As String
    logsFolder = Trim$(raporty_plum.txtLogsFolder.text)
    templateXlsx = Trim$(raporty_plum.txtTemplateXlsx.text)
    keysXlsx = Trim$(raporty_plum.txtKeysXlsx.text)

    If Len(logsFolder) = 0 Then errMsg = "Nie wskazano folderu z logami.": Exit Function
    If Not FolderExists(logsFolder) Then errMsg = "Folder logów nie istnieje: " & logsFolder: Exit Function

    If Len(keysXlsx) = 0 Then errMsg = "Nie wskazano pliku KEYS.xlsx.": Exit Function
    If Not FileExists(keysXlsx) Then errMsg = "Plik KEYS.xlsx nie istnieje: " & keysXlsx: Exit Function

    If Len(templateXlsx) = 0 Then errMsg = "Nie wskazano wzoru raportu (template).": Exit Function
    If Not FileExists(templateXlsx) Then errMsg = "Wzór raportu nie istnieje: " & templateXlsx: Exit Function

    UI_ValidateInputs = True

End Function

Public Function BuildRuntimeConfigYaml(ByVal rootPath As String) As String
    Dim cfgPath As String
    cfgPath = rootPath & "\_run\config.runtime.yaml"

    Dim logsFolder As String, templateXlsx As String, keysXlsx As String
    logsFolder = NormalizePath(raporty_plum.txtLogsFolder.text)
    templateXlsx = NormalizePath(raporty_plum.txtTemplateXlsx.text)
    keysXlsx = rootPath & "\" & KEYS_REL

    EnsureFolder rootPath & "\_run"
    EnsureFolder rootPath & "\_out"
    EnsureFolder rootPath & "\_out\indywidualne"
    EnsureFolder rootPath & "\_out\pdf"

    Dim y As String
    y = ""
    y = y & "root: """ & YamlEscape(rootPath) & """" & vbCrLf

    y = y & "data:" & vbCrLf
    y = y & "  logs_dir: """ & YamlEscape(logsFolder) & """" & vbCrLf
    y = y & "  logs_recursive: true" & vbCrLf ' <— ważne: folder + podfoldery

    y = y & "run:" & vbCrLf
    y = y & "  dir: """ & YamlEscape(rootPath & "\_run") & """" & vbCrLf

    y = y & "out:" & vbCrLf
    y = y & "  indywidualne_dir: """ & YamlEscape(rootPath & "\_out\indywidualne") & """" & vbCrLf
    y = y & "  pdf_dir: """ & YamlEscape(rootPath & "\_out\pdf") & """" & vbCrLf

    y = y & "parse_events:" & vbCrLf
    y = y & "  keys_xlsx: """ & YamlEscape(keysXlsx) & """" & vbCrLf
    y = y & "  keys_sheet: ""KEYS""" & vbCrLf

    y = y & "pdf:" & vbCrLf
    y = y & "  template_xlsx: """ & YamlEscape(templateXlsx) & """" & vbCrLf

    WriteTextFileUtf8 cfgPath, y
    BuildRuntimeConfigYaml = cfgPath
End Function

' --- dialogi i utils (zostają z poprzedniej wersji) ---
Public Function PickFolderDialog(ByVal title As String, Optional ByVal initialPath As String = "") As String
    On Error GoTo EH
    Dim fd As Object
    Set fd = Application.FileDialog(4)
    fd.title = title
    If Len(initialPath) > 0 Then fd.InitialFileName = EnsureTrailingBackslash(initialPath)
    fd.AllowMultiSelect = False
    If fd.Show <> -1 Then Exit Function
    PickFolderDialog = CStr(fd.SelectedItems(1))
    Exit Function
EH:
    PickFolderDialog = ""
End Function

Public Function PickFileDialog(ByVal title As String, ByVal filterDesc As String, ByVal filterPattern As String, Optional ByVal initialPath As String = "") As String
    On Error GoTo EH
    Dim fd As Object
    Set fd = Application.FileDialog(3)
    fd.title = title
    fd.AllowMultiSelect = False
    fd.Filters.Clear
    fd.Filters.Add filterDesc, filterPattern
    If Len(initialPath) > 0 Then fd.InitialFileName = initialPath
    If fd.Show <> -1 Then Exit Function
    PickFileDialog = CStr(fd.SelectedItems(1))
    Exit Function
EH:
    PickFileDialog = ""
End Function

Private Function FileExists(ByVal p As String) As Boolean: FileExists = (Len(dir(p)) > 0): End Function
Private Function FolderExists(ByVal p As String) As Boolean
    On Error Resume Next
    FolderExists = (Len(dir(p, vbDirectory)) > 0)
    On Error GoTo 0
End Function

Private Sub EnsureFolder(ByVal folderPath As String)
    Dim fso As Object
    Set fso = CreateObject("Scripting.FileSystemObject")
    If Not fso.FolderExists(folderPath) Then fso.CreateFolder folderPath
End Sub

Private Function EnsureTrailingBackslash(ByVal p As String) As String
    If Len(p) = 0 Then EnsureTrailingBackslash = "": Exit Function
    If Right$(p, 1) = "\" Then EnsureTrailingBackslash = p Else EnsureTrailingBackslash = p & "\"
End Function

Private Function NormalizePath(ByVal p As String) As String
    NormalizePath = Replace(Trim$(p), "/", "\")
End Function

Private Function YamlEscape(ByVal s As String) As String
    YamlEscape = Replace(s, """", "\""")
End Function

Private Sub WriteTextFileUtf8(ByVal fullPath As String, ByVal text As String)
    Dim stm As Object
    Set stm = CreateObject("ADODB.Stream")
    stm.Type = 2
    stm.Charset = "utf-8"
    stm.Open
    stm.WriteText text
    stm.Position = 0
    stm.SaveToFile fullPath, 2
    stm.Close
End Sub
