Sub Envoyer_Email()
    Dim OutlookApp As Object
    Dim OutlookMail As Object
    Dim wordEditor As Object
    Dim wb As Workbook
    Dim wsR As Worksheet
    Dim lastRow As Long
    Dim destinataire As String
    Dim Sujet As String
    Dim Corps As String
    Dim jour As Integer, mois As Integer, annee As Integer

    ' Set references
    Set wb = ThisWorkbook
    Set wsR = wb.Worksheets("ratings") ' Your ratings sheet
    lastRow = wsR.Cells(wsR.Rows.Count, 1).End(xlUp).Row

    ' Exit if Mail_Show flag is False
    If Mail_Show = False Then Exit Sub

    ' Dates
    jour = Day(Now)
    mois = Month(Now)
    annee = Year(Now)

    ' Email fields
    destinataire = "david.favier@bnpparibas.com; olivier.boucillotte@bnpparibas.com"
    Sujet = "Rating changes as of " & jour & "/" & mois & "/" & annee
    Corps = "Dear Pricing Team," & Chr(10) & Chr(10) & _
            "Would you please consider the price challenges from the following file?" & Chr(10) & Chr(10) & _
            "Thank you for your continuous support."

    ' Create Outlook email
    Set OutlookApp = CreateObject("Outlook.Application")
    Set OutlookMail = OutlookApp.CreateItem(0)

    ' Copy the Excel table as image
    wsR.Range("A1:E" & lastRow).CopyPicture Appearance:=xlScreen, Format:=xlPicture

    With OutlookMail
        .To = destinataire
        .Subject = Sujet
        .Display ' Must display before accessing WordEditor
        Set wordEditor = .GetInspector.WordEditor

        ' Insert message + image
        wordEditor.Application.Selection.TypeText Corps & vbCrLf & vbCrLf
        wordEditor.Application.Selection.TypeParagraph
        wordEditor.Application.Selection.Paste
        .Send
    End With

    ' Cleanup
    Set OutlookMail = Nothing
    Set OutlookApp = Nothing
    MsgBox "L'e-mail avec les pièces jointes a été envoyé avec succès.", vbInformation
End Sub