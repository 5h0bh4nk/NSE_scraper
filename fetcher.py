
import pandas as pd
import mysql.connector
from datetime import date, timedelta
import calendar

def get_month_code(month):
    monthcode = ''
    if month == '01':
        monthcode = 'JAN'
    elif month == '02':
        monthcode = 'FEB'
    elif month == '03':
        monthcode = 'MAR'
    elif month == '04':
        monthcode = 'APR'
    elif month == '05':
        monthcode = 'MAY'
    elif month == '06':
        monthcode = 'JUN'
    elif month == '07':
        monthcode = 'JUL'
    elif month == '08':
        monthcode = 'AUG'
    elif month == '09':
        monthcode = 'SEP'
    elif month == '10':
        monthcode = 'OCT'
    elif month == '11':
        monthcode = 'NOV'
    elif month == '12':
        monthcode = 'DEC'
    return monthcode

# extract csv data from url
equity_security = pd.read_csv('https://archives.nseindia.com/content/equities/EQUITY_L.csv')
print(equity_security.head())

#connect to sql and create database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="shubh123"
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE IF NOT EXISTS nse")

#connect to database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="shubh123",
    database="nse"
)

mycursor = mydb.cursor()

# create table

mycursor.execute("CREATE TABLE IF NOT EXISTS equity_security (SYMBOL VARCHAR(255), NAME VARCHAR(255),SERIES VARCHAR(255), ISIN VARCHAR(255), FACE_VALUE INT, PRIMARY KEY (SYMBOL, ISIN));")

mycursor.execute("CREATE TABLE IF NOT EXISTS bhavcopy (SYMBOL VARCHAR(255), SERIES VARCHAR(255), DATE1 DATE,OPEN FLOAT, HIGH FLOAT, LOW FLOAT, CLOSE FLOAT, LAST FLOAT, PREVCLOSE FLOAT, TOTTRDQTY FLOAT, TOTTRDVAL FLOAT, TOTALTRADES INT, ISIN VARCHAR(255), PRIMARY KEY (SYMBOL, ISIN, DATE1, TOTALTRADES))")

#insert data into table

for i, row in equity_security.iterrows():
    sql = "INSERT INTO equity_security (SYMBOL, NAME, SERIES, ISIN, FACE_VALUE) VALUES (%s, %s, %s, %s, %s)"
    val = (row['SYMBOL'], row['NAME OF COMPANY'], row[' SERIES'], row[' ISIN NUMBER'], row[' FACE VALUE'])
    mycursor.execute(sql, val)
    mydb.commit()


today = date.today()

for i in range(30):
    date = today - timedelta(days = i)
    workday = calendar.day_name[date.weekday()]
    day = str(date.day)
    if len(day) == 1:
        day = '0'+day
    month = date.strftime("%m")
    monthcode = get_month_code(month)
    year = date.strftime("%Y")

    if workday == 'Saturday' or workday == 'Sunday':
        continue
    print('EXTRACTING : https://archives.nseindia.com/content/historical/EQUITIES/'+year+'/'+monthcode+'/cm'+day+str(date.strftime("%b")).upper()+'2022bhav.csv.zip')
    bhavcopy = pd.read_csv('https://archives.nseindia.com/content/historical/EQUITIES/'+year+'/'+monthcode+'/cm'+day+str(date.strftime("%b")).upper()+'2022bhav.csv.zip')

    for i, row in bhavcopy.iterrows():
        sql = "INSERT INTO bhavcopy (SYMBOL, SERIES, DATE1, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE, TOTTRDQTY, TOTTRDVAL, TOTALTRADES, ISIN) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (row['SYMBOL'], row['SERIES'], date, row['OPEN'], row['HIGH'], row['LOW'], row['CLOSE'], row['LAST'], row['PREVCLOSE'], row['TOTTRDQTY'], row['TOTTRDVAL'], row['TOTALTRADES'], row['ISIN'])
        mycursor.execute(sql, val)
        mydb.commit()

print(mycursor.rowcount, "record inserted.")