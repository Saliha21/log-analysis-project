#!/usr/bin/env python3

import psycopg2

DBname="news" #database name

select1=""" SELECT title , count(log.id) as num 
          FROM articles, log 
          WHERE log.path = CONCAT ('/article/' , articles.slug) 
          GROUP BY articles.title 
          ORDER BY num DESC 
          LIMIT 3;"""


select2="""SELECT authors.name, count(log.id) as num 
         FROM articles , authors ,log 
         WHERE articles.author = authors.id AND log.path = CONCAT ('/article/' , articles.slug) AND log.status LIKE '%200%' 
         GROUP BY authors.name 
         ORDER BY num DESC """



select3="""SELECT errorlogs.date, round(100.0*errorcount/num,2) as percent
            FROM logs, errorlogs
            WHERE logs.date = errorlogs.date
            AND errorcount > num/100;"""



#connection with database
def conn_data(select_sql):
  db = psycopg2.connect(database=DBname)
  cur= db.cursor()
  cur.execute(select_sql)  
  result=cur.fetchall()
  db.close()
  return result

query1=conn_data(select1) 


def Question1():
	print(" 1- What are the most popular three articles of all time?")
	print(" ")
	Question1=query1
	for title, num in Question1:
		print(" {} -- {} views".format(title, num))


query2=conn_data(select2)


def Question2():
	print(" ")
	print(" 2- Who are the most popular article authors of all time?")
	print(" ")
	Question2=query2
	for name, num in Question2:
		print(" {} -- {} views".format(name, num))

	

query3=conn_data(select3)


def Question3():
	print(" ")
	print(" 3- On which days did more than 1% of requests lead to errors?")
	print(" ")
	Question3=query3
	for i in Question3:
		print("\t" + str(i[0]) + " - "  + str(i[1]) +  " % " + "errors")
        print(" ")


 

if __name__ == '__main__':
	Question1()
	Question2()
	Question3()







