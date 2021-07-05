import mysql.connector
import json
import os
import re
import xlrd
import math
import datetime
import csv

# for debugging purposes
from inspect import getmembers
from pprint import pprint

    #Email Sending
from email.mime.text import MIMEText
from email.header import Header
import smtplib
    # Recipients
to = '<>'
#to = ['<>','<>']

    # create a log
import logging
logging.basicConfig(filename='logFile2.log', filemode='w', level=logging.INFO, format='%(asctime)s >>   %(message)s', datefmt='%d.%m.%y %H:%M')

errorsOccured = False

config = {
    'user': '',
    'password': '',
    'host': '0.0.0.0',
    'database': 'NAME',
    'raise_on_warnings': True,
}

jobList = {
'ea_ablehnung': {
    'fileType': 'xlsx',
    'filePath': r'\\PATH.xlsx',
    'workSheet': 'EA_Ablehnung',
    'lastUpdated': None,
    'lastUpdatedEntry': 'tbl_mbfd_iza_loeschwunsch',
    'truncStmt': 'TRUNCATE TABLE iza_mbfd.tbl_mbfd_iza_loeschwunsch;',
    'insertStmt': """INSERT INTO
                iza_mbfd.tbl_mbfd_iza_loeschwunsch
            (KVZ_ID,
             ABLEHNUNGSGRUND,
             ABLEHNUNG_AM,
             GESPERRT_BIS,
             KOMMENTAR,
             EINGETRAGEN_AM,
             EINGETRAGEN_VON
             )
             VALUES
             (%s, %s, %s, %s, %s, %s, %s);"""
},

    'ea_aufnahme': {
        'fileType': 'xlsx',
        'filePath': r'\\PATH.xlsx',
        'workSheet': 'EA_Aufnahme',
        'lastUpdated': None,
        'lastUpdatedEntry': 'tbl_mbfd_iza_loeschwunschkorrektur',
        'truncStmt': 'TRUNCATE TABLE iza_mbfd.tbl_mbfd_iza_loeschwunschkorrektur;',
        'insertStmt': """INSERT INTO
                    iza_mbfd.tbl_mbfd_iza_loeschwunschkorrektur
                (KVZ_ID,
                 EINTRAGUNG_DATUM,
                 AUFNAHME_EA_DATUM_SOLL,
                 GRUND,
                 ANFRAGER,
                 KOMMENTAR
                 )
                 VALUES
                 (%s, %s, %s, %s, %s, %s);"""
    }
}



def loadDbCredentials():
    credFileName = '%userprofile%\\izadb_credentials.json'
    credFileName = os.path.expandvars(credFileName)

    credFile = open(credFileName, 'r')
    credJson = json.load(credFile)
    credFile.close()

    config['user'] = credJson['user']
    config['password'] = credJson['password']


def loadFile(job):
    if ('dateAppended' in job and job['dateAppended']):
        filenamesByDate = dict()
        fileDir = os.path.dirname(job['filePath'])
        for dirpath, dirnames, filenames in os.walk(fileDir):
            break
        for file in filenames:
            year = month = day = 0
            match = re.search(job['lastUpdatedPattern'], file)
            try:
                year = int(match.group('year'))
                month = int(match.group('month'))
                day = int(match.group('day'))
            except:
                break;
            fileDate = datetime.date(year, month, day)
            filenamesByDate[fileDate] = file
        latest = max(filenamesByDate)
        job['lastUpdated'] = latest
        job['filePath'] = os.path.join(fileDir, filenamesByDate[latest])
    else:
        job['lastUpdated'] = datetime.date.fromtimestamp(os.path.getmtime(job['filePath']))
    if (job['fileType'] in ('xlsx', 'xls')):
        # print(job['filePath'])
        data = loadExcelFile(job['filePath'], job['workSheet'])
    elif (job['fileType'] == 'csv'):
        data = loadCsvFile(job['filePath'])
    return data


def cleanInvalidValues(value):  # necessary, as long as the Kollo-bundesweit contains errors
    replacements = {
        '³': 'ü',
        '÷': 'ö',
        'õ': 'ä',
        '▀': 'ß'
    }
    for orig, repl in replacements.items():
        value = re.sub( orig, repl, value)
    return value


def loadExcelFile(filePath, workSheetName):
    workBook = xlrd.open_workbook(filePath)
    workSheet = workBook.sheet_by_name(workSheetName)
    numRows = workSheet.nrows
    numCols = workSheet.ncols
    tableData = []
    for i in range(1, numRows):
        formattedRow = []
        for j in range(0, numCols):
            cell = workSheet.cell(i, j)
            cellValue = cell.value
            if (isinstance(cellValue, str)):
                cellValue = cleanInvalidValues(cellValue)
            if ('' == cellValue):
                cellValue = None
            if (cell.ctype == 3):
                dateTuple = xlrd.xldate_as_tuple(cellValue, workBook.datemode)
                cellValue = str(dateTuple[0]) + '-' + str(dateTuple[1]) + '-' + str(dateTuple[2])
            formattedRow.append(cellValue)
        tableData.append(formattedRow)
    return tableData



def loadCsvFile(filePath):
    csv.field_size_limit(1000000)
    data = list()
    with open(filePath, 'r')as file:
        fileReader = csv.reader(file, delimiter=';')
        for row in fileReader:
            data.append(row)
    return data[1:]


def SendPerEmail():

    file= open('logFile2.log', 'r')
    subj = 'KDFM-Importer Report'
    frm = 'KDFM-Importer<>'

    mail = MIMEText(file.read())
    mail['Subject'] = Header(subj, 'utf-8')
    mail['From'] = frm
    mail['To'] = to #",".join(to)  # with list

    try:
        s = smtplib.SMTP('0.0.0.0', 587, None)
        s.sendmail(frm, to, mail.as_string())
        s.quit()
    except BaseException as err:
        print(str(err))



loadDbCredentials()
# print(config)

connection = mysql.connector.connect(**config)
cursor = connection.cursor()
try:
    for job in jobList:
        print(datetime.datetime.now())
        print("Starting job", job)
        logging.info(f"Starting job  {job}")
        print("Loading file...")
        logging.info("Loading file...")
        data = list()
        data = loadFile(jobList[job])
        print("Deleting table content...")
        logging.info("Deleting table content...")
        cursor.execute(jobList[job]['truncStmt'])
        print("Table is now empty.")
        logging.info("Table is now empty.")
        # Too many inserts lead to errors, so data will be split into chunks
        print(len(data), "rows found")
        logging.info(f"{len(data)} rows found")
        chunkSize = 5000  # chunksize may vary, depending on your machine. If value is too high, operation will crash
        numChunks = math.ceil(len(data) / chunkSize)
        for i in range(0, numChunks):
            print("Chunk", i + 1, "of", numChunks)
            logging.info(f"Chunk {i + 1} of {numChunks}")
            lowerBound = i * chunkSize
            upperBound = (i + 1) * chunkSize - 1
            if (upperBound > len(data)):
                upperBound = len(data) - 1
            print("Processing rows", lowerBound, "to", upperBound)
            logging.info(f"Processing rows  {lowerBound} to  {upperBound}")
            dataChunk = data[lowerBound:upperBound + 1]
            # pprint (dataChunk[0:2])
            cursor.executemany(jobList[job]['insertStmt'],dataChunk)
            warnings = cursor.fetchwarnings()
            if (None != warnings):
                errorsOccured = True
            rowsAffected = cursor.rowcount
            print("Insert for", job, "executed,", rowsAffected, "rows affected, the following warnings were raised:", warnings)
            logging.info(f"Insert for {job} executed { rowsAffected} rows affected, the following warnings were raised: {warnings}")
        lastUpdated = jobList[job]['lastUpdated']
        print("Last updated:", lastUpdated)
        logging.info(f"Last updated: { lastUpdated}")
        cursor.execute('UPDATE iza_mbfd.tbl_datenstand_input SET STAND = "' + str(lastUpdated) + '" WHERE TABELLE = "' + jobList[job]['lastUpdatedEntry'] + '"')
        print("Job", job, "finished\r\n")
        logging.info(f"Job {job} finished\n")
        if (errorsOccured):
            print('### ERRORS HAVE OCCURED. CHECK LOG ABOVE TO SEE WHICH ###')
    connection.close()

except BaseException as err :
    print(str(err))
    logging.error(str(err))
    SendPerEmail()

