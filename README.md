# DATA_MANAGEMENT
prjet


Sub FormatAndCleanRatingChanges()
    Dim ws As Worksheet
    Dim lastRow As Long, lastCol As Long
    Dim rng As Range, cell As Range
    Dim delRange As Range
    Dim i As Long, j As Long
    Dim ratingCol As Range

    ' Set the active worksheet
    Set ws = ActiveSheet
    
    ' Find the last used row and column
    lastRow = ws.Cells(ws.Rows.Count, "A").End(xlUp).Row
    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column

    ' Apply formatting to header row
    With ws.Rows(1)
        .Font.Bold = True
        .Interior.Color = RGB(0, 176, 80) ' Green background
        .Font.Color = RGB(255, 255, 255) ' White text
    End With

    ' Apply alternating row shading (light green for readability)
    For i = 2 To lastRow
        If i Mod 2 = 0 Then ' Even rows
            ws.Rows(i).Interior.Color = RGB(230, 255, 230) ' Light green
        End If
    Next i

    ' Apply conditional formatting for rating changes
    ' Assume rating changes start from column 3 onwards
    For i = 2 To lastRow
        For j = 3 To lastCol ' Assuming columns 3+ contain rating changes
            Set cell = ws.Cells(i, j)
            
            If InStr(cell.Value, "→") > 0 Then ' If cell contains "→", it's a rating change
                If Left(cell.Value, 1) > Right(cell.Value, 1) Then
                    ' Downgrade (red text)
                    cell.Font.Color = RGB(192, 0, 0)
                    cell.Font.Bold = True
                Else
                    ' Upgrade (green text)
                    cell.Font.Color = RGB(0, 176, 80)
                    cell.Font.Bold = True
                End If
            End If
        Next j
    Next i

    ' Auto-fit columns
    ws.Cells.EntireColumn.AutoFit

    ' Add borders to the table
    Set rng = ws.Range(ws.Cells(1, 1), ws.Cells(lastRow, lastCol))
    With rng.Borders
        .LineStyle = xlContinuous
        .Weight = xlThin
    End With

    ' Identify rows containing "nan" and mark them for deletion
    For Each cell In rng
        If LCase(cell.Value) = "nan" Then
            If delRange Is Nothing Then
                Set delRange = cell.EntireRow
            Else
                Set delRange = Union(delRange, cell.EntireRow)
            End If
        End If
    Next cell
    
    ' Delete identified rows
    If Not delRange Is Nothing Then
        delRange.Delete Shift:=xlUp
    End If

    ' Cleanup
    Set rng = Nothing
    Set delRange = Nothing
    Set ws = Nothing

    MsgBox "Table formatted, cleaned, and colored!", vbInformation, "Success"
End Sub