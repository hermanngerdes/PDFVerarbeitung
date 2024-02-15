# Version 1.0 15.12.20
# - Artikel wird nur geschrieben, wenn die EAN ein gültiger Artikel ist.
# - Wiegeartikel wird berücksichtigt: Wenn Wiegeartikel, dann wird kg-Spalte, sonst die Stück-Spalte gefüllt.

import os
import shutil
import pyodbc
from datetime import datetime

from PyPDF2 import PdfReader
from Datenbank import db_open
import configparser

from datetime import date

def pause(Bemerkung):
    programPause = input(Bemerkung)

def Slashkontrolle(pfad):
    if pfad[-2:] != "//":
        return pfad + "//"
    else:
        return pfad


# Verbindung zur Datenbank aufbauen

# ini.read("C://Users//User//PycharmProjects//PDFFields//PDFImport.ini")
ini = configparser.ConfigParser(delimiters='=')
ini.read("PDFImport.ini")
laden = ini["PDFImport"]["Laden"]
vorlauf = ini["PDFImport"]["Vorlauf"]
print(f"Vorlauf: {vorlauf} Stunden")
ablage = ini["PDFImport"]["Ablagepfad"]
importpfad = ini["PDFImport"]["ImportPfad"]
protokollpfad = ini["PDFImport"]["Protokollpfad"]

importpfad = Slashkontrolle(importpfad)

ablage = Slashkontrolle(ablage)

protokollpfad = Slashkontrolle(protokollpfad)
conn = db_open('bo', 'PDFImport.ini')

crs = conn.cursor()

updcrs = conn.cursor()
# pdf_file_name = fd.askopenfilename(title = "Select file PDF").
# Alle PDF-Dateien im Ordner untersuchen

Dateien = os.listdir(importpfad)

with open(protokollpfad + str(date.today()) + '.txt', 'w') as f2:
    for pdf_file_name in Dateien:
        # start_time = time.time()
        # print (os.path.splitext(pdf_file_name)[1])

        #print (protokollpfad + str(date.today()) + '.csv')

        if (os.path.splitext(pdf_file_name)[1]) == '.pdf':
            f = PdfReader(importpfad + pdf_file_name)
            fields = f.get_fields()
            if not(fields is None):
                fdfinfo = dict((k, v.get('/V', '')) for k, v in fields.items())
                dateipfad = (os.path.splitext(pdf_file_name)[0])
                pfad, datei = os.path.split(dateipfad)
                kdnr = ""
                if 'Kundennummer' in fdfinfo:
                    kdnr = fdfinfo['Kundennummer']
                    stmt = "Select Adressid, Nachname From Adresse where Adressid = '%s'" % kdnr
                    # print stmt
                    crs.execute(stmt)
                    row = crs.fetchone()
                    if row:
                        if "Kundenname" in fdfinfo:
                            print(f"Kunde: {kdnr:>12} Namensabgleich: {fdfinfo['Kundenname']} | {row[1]} ")
                    else:
                        f2.write(f"Keinen passenden Kunden für Kundennummer: {kdnr} in Datei: {datei}.pdf gefunden.\n")
                        kdnr = ""

                if kdnr > "":
                    f2.write(f"PDF-Datei: {datei} ")
                    # gibts eine gültige bestellid für diesen Kunden?
                    stmt = "Select b.Bestellid From dBestellung b " \
                           "Left Join (select Bestellid, Rowid From dLiBesonderheit where isNULL(PLZ,'') = 'Pause'" \
                           "and getdate() between von and isNULL(bis, getdate())) l " \
                           "On l.Bestellid = b.Bestellid " \
                           "where b.Adressid = '%s' and l.Rowid is NULL " \
                           "And getdate() between b.von and isNULL(b.bis, getdate())" % kdnr
                    crs.execute(stmt)
                    try:
                        row = crs.fetchone()
                    except pyodbc.Error as e:
                        print(f"Fehler bei Befehl \n{stmt}")

                    bid = 0
                    if not (row[0] is None):
                        bid = row[0]
                    if bid > 0:
                        # Liefertag gefunden
                        pass
                    else:
                        # Bestellid anlegen
                        stmt = "Insert into dBestellung(Ladenid, Bestellid, Adressid, von, vonKW, TourNr)" \
                               "(Select '%s',(Select max(Bestellid) + 1 From dBestellung), '%s'," \
                               " datediff(dd, datepart(dw, getdate()) - 1, getdate())," \
                               " dbo.fn_KW(datediff(dd, datepart(dw, getdate()) - 1, getdate())), 0)" % (laden, kdnr)
                        # stmt = "Insert into AdressTyp(AdressTyp, Bezeichnung) Values('T','Hermann')"
                        print("Anlage einer neuen Bestellung: " + stmt)
                        updcrs.execute(stmt)
                        updcrs.commit()

                    # Es werden die Bestellids eines Kunden untersucht. Gibt es noch eine gültige, passende Bestellid
                    # in dieser Woche (bd), wird diese gewählt. Gibts erst eine Bestellid in der nächsten Woche (bn),
                    # wird die genommen.
                    # Zusätzlich ermittelt die SQL-Abfrage den passenden Montag:
                    # Wenn bd.Ltmin vorhanden, liegt der Termin in der Woche, in der der Liefertermin gefunden wurde,
                    # sonst in der nächsten.

                    stmt = "Select min(b.Bestellid) Bestellid, " \
                           " dateadd(dd, Case When min(isNULL(bd.LtMin, -1)) >= 0 Then 1 else 8 End" \
                           " - datepart(dw, dateadd(hh, {vl}, getdate()))," \
                           " cast(dateadd(hh, {vl}, getdate()) as date)) Montag" \
                           " From dBestellung b" \
                           " Left Join (Select TourNr, Bestellid From dLiBesonderheit" \
                           "  where getdate() between von and isNULL(bis, getdate())" \
                           "  and plz <> 'Pause') l On l.Bestellid = b.Bestellid " \
                           " Join dTourbeschreibung t On t.TourNr = isNULL(l.TourNr, b.TourNr)" \
                           " Left Join (Select Rowid, Bestellid From dLiBesonderheit" \
                           "  where getdate() between von and isNULL(bis, getdate()) and plz = 'Pause') p " \
                           "  On p.Bestellid = b.Bestellid" \
                           " Left Join (Select min(t.Liefertag - datepart(dw, dateadd(hh, {vl}, getdate()))) LtMin," \
                           "  min(t.Liefertag) Lt, b.Adressid From dBestellung b" \
                           "  Left Join (Select TourNr, Bestellid" \
                           "  From dLiBesonderheit where getdate() between von and isNULL(bis, getdate())" \
                           "  and plz <> 'Pause') l On l.Bestellid = b.Bestellid" \
                           "  Left Join (Select Rowid, Bestellid From dLiBesonderheit" \
                           "  where getdate() between von and isNULL(bis, getdate()) and plz = 'Pause') p" \
                           "  On p.Bestellid = b.Bestellid" \
                           "  Join dTourbeschreibung t On t.TourNr = isNULL(l.TourNr, b.TourNr)" \
                           "  where getdate() between b.von and isNULL(b.bis, getdate()) And p.Rowid is NULL" \
                           "  And t.Liefertag - datepart(dw, dateadd(hh, {vl}, getdate())) >= 0" \
                           "  And b.LadenId = '{ld}'" \
                           "  Group by b.Adressid) bd On bd.Adressid = b.Adressid" \
                           " Left Join (Select min(t.Liefertag - datepart(dw, dateadd(hh, {vl}, getdate()))) LtMin," \
                           "  min(t.Liefertag) Lt, b.adressid From dBestellung b" \
                           "  Left Join (Select TourNr, Bestellid From dLiBesonderheit" \
                           "  where getdate() between von and isNULL(bis, getdate()) and plz <> 'Pause') l" \
                           "  On l.Bestellid = b.Bestellid" \
                           "  Left Join (Select Rowid, Bestellid From dLiBesonderheit" \
                           "  where getdate() between von and isNULL(bis, getdate()) and plz = 'Pause') p " \
                           "  On p.Bestellid = b.Bestellid" \
                           "  Join dTourbeschreibung t On t.TourNr = isNULL(l.TourNr, b.TourNr)" \
                           "  where getdate() between b.von and isNULL(b.bis, getdate()) And p.Rowid is NULL" \
                           "  And b.LadenId = '{ld}'" \
                           "  Group by b.Adressid) bn On bn.Adressid = b.Adressid" \
                           " where getdate() between b.von and isNULL(b.bis, getdate()) " \
                           " And isnull(bd.LtMin, bn.LtMin) = t.Liefertag - datepart(dw, dateadd(hh, {vl}, getdate()))" \
                           " And b.LadenId = '{ld}' And b.Adressid = '{kd}' and p.Rowid is NULL" \
                           " Group by b.Adressid" \
                        .format(vl=vorlauf, kd=kdnr, ld=laden)
                    try:
                        crs.execute(stmt)
                    except pyodbc.Error:
                        print(f"Fehlerhafter Befehl:\n{stmt}")

                    # Bei mehreren gültigen Bestellungen ermitteln der kleinsten Bestellid, die auf einem passenden
                    # Liefertag liegt.

                    row = crs.fetchone()
                    bid = row[0]
                    montag = row[1].strftime("%d.%m.%Y")
                    f2.write(f". Eingelesen für {montag}\n")
                    for key in fdfinfo.keys():
                        if len(fdfinfo[key]) > 0:
                            if key[0:3] == 'EAN':
                                stmt = "Insert into dBestellpos(Bestellid, von, vonKW, bis, bisKW, EAN, Menge," \
                                       " Stueck, Kg, Periode)" \
                                       " Select {bid}, '{montag}', dbo.fn_KW('{montag}')," \
                                       " dateadd(dd,6,'{montag}'), dbo.fn_KW('{montag}'), {ean}, '{menge}'," \
                                       " Case When Wiegeartikel = 0" \
                                       " then cast(Replace('{menge}',',','.') as Real) Else 0 End," \
                                       " Case When Wiegeartikel > 0" \
                                       " then cast(Replace('{menge}',',','.') as Real) Else 0 End , 1" \
                                       " From boart..Artikel where EAN = {ean}" \
                                       .format(montag=montag, bid=bid, ean=key[3:], menge=fdfinfo[key])
                                # print("einfügen " + stmt)
                                try:
                                    updcrs.execute(stmt)
                                    updcrs.commit()
                                except pyodbc.Error as e:
                                    print(e)
                                f2.write(f"Kunde:{kdnr:>14} EAN:{key[3:]:>15} Menge:{fdfinfo[key]:>8}"
                                         f" BestellId: {bid:>8}"
                                         f"{' Artikel nicht gefunden.' if updcrs.rowcount == 0 else ''}\n")
                    stmt = "Insert into boart..dChange(Bestellid, Montag)" \
                           "Values(%s, '%s')" \
                           % (str(bid), montag)
                    try:
                        updcrs.execute(stmt)
                        updcrs.commit()
                    except pyodbc.Error as e:
                        print(e)
                # Ablegen der Datei in Ablageordner
                if len(ablage) > 0:
                    shutil.move(importpfad + pdf_file_name, ablage + pdf_file_name)
pause("Ende des Programms")
crs.close()
conn.close()


