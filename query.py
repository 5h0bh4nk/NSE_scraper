import mysql.connector
from datetime import date, timedelta
import calendar
import csv

def write_to_csv(result, filename, writerow):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(writerow)
        for row in result:
            writer.writerow(row)

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="shubh123",
  database="nse"
)

mycursor = mydb.cursor()


# 1. Writing a SQL query to fetch the top 25 gainers of the day sorted in the order of their gains. Gains is defined as [(close - open) / open] for the day concerned as per point 2 above.
today = '2022-12-23'
mycursor.execute("SELECT SYMBOL, SERIES, ((CLOSE-OPEN)/OPEN) FROM bhavcopy WHERE DATE1='"+today+"' ORDER BY ((CLOSE-OPEN)/OPEN) DESC LIMIT 25;")
result = mycursor.fetchall()

# output data to csv
write_to_csv(result, 'top_25_gainers.csv', ['SYMBOL', 'SERIES', 'GAINS'])



# 2. Getting datewise top 25 gainers for last 30 days as per point 4 above.
for i in range(30):
    today = date.today()
    date = today - timedelta(days = i)
    weekday = calendar.day_name[date.weekday()]
    if weekday == 'Saturday' or weekday == 'Sunday':
        continue
    mycursor.execute("SELECT SYMBOL, SERIES, ((CLOSE-OPEN)/OPEN) FROM bhavcopy WHERE DATE1='"+str(date)+"' ORDER BY ((CLOSE-OPEN)/OPEN) DESC LIMIT 25;")
    result = mycursor.fetchall()

    # wtite to csv
    write_to_csv(result, 'top_25_gainers_'+str(date)+'.csv', ['SYMBOL', 'SERIES', 'GAINS'])



# 3. Getting a single list of top 25 gainers using the open of the oldest day and close of the latest day of those 30 days as per point 4.
today = '2022-12-23'
month_ago = '2022-11-25'
mycursor.execute("SELECT C1.SYMBOL, C1.SERIES, ((C2.CLOSE-C1.OPEN)/C1.OPEN) FROM bhavcopy C1, bhavcopy C2 WHERE C1.DATE1='"+month_ago+"' AND C2.DATE1='"+today+"' ORDER BY ((C2.CLOSE-C1.OPEN)/C1.OPEN) DESC LIMIT 25;")

result = mycursor.fetchall()

# output to csv
write_to_csv(result, 'top_25_gainers_for_month.csv', ['SYMBOL', 'SERIES', 'GAINS'])
