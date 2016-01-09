#!/usr/bin/python
import sys, re
import json
import MySQLdb
import datetime,time
import flightUtil

level0=''
level1='  '
level2='     '
level3='          '
flightPrefix = ''
flightSuffix = ''
cnx = ''

def createSelectQuery(flight):
   selectQuery = " SELECT price "\
                 " FROM flight "\
                 " WHERE name='%s'" \
                 " ORDER BY id DESC "\
                 " LIMIT 1"%flight
   return selectQuery

def createInsertQuery(flight,price):
   insertQuery = " INSERT INTO flight "\
                 " (name,price) "\
                 " VALUES (%s,%s)"
   return insertQuery

def insertFlightUpdatedPrice(cur,flight,price):
   insertQuery = createInsertQuery(flight,price)
   cur.execute(insertQuery,(flight,price))
   if cur.rowcount!=1: 
      print 'Insertion Failed'
      sys.exit(1)
   cur.execute('COMMIT')

def checkAndInsertFlightPrice(flight,price):
   global cnx  
   cur = cnx.cursor()
   selectQuery = createSelectQuery(flight)
   cur.execute(selectQuery)	
   data = cur.fetchone()
   if cur.rowcount==0:
      insertFlightUpdatedPrice(cur,flight,price)
      print 'New Flight: %s '%flight 
   elif data[0]!=int(price):
      insertFlightUpdatedPrice(cur,flight,price)
      print 'Price Changed for Flight: %s '%flight 
   else:
       print 'Price Un-Changed for Flight: %s '%flight 

def printFlightsInScedule(key,value):
   global flightPrefix
   global flightSuffix
   for key1,value1 in value.items():
      if re.match(flightPrefix,key1) is not None and key1.endswith(flightSuffix): 
         checkAndInsertFlightPrice(key1,value1['O']['ADT']['tf'])

if __name__=="__main__":
   sys.stdout = open('/home/abhinav/c++/python/log.out','a+')
   sys.stderr = open('/home/abhinav/c++/python/log.err','a+')
   print datetime.datetime.fromtimestamp(\
         time.time()).strftime('%Y-%m-%d %H:%M:%S')
   cnx = flightUtil.createConnection()
   string = flightUtil.fetchSanitizedInput()
   parsed_json = json.loads(string)
   flightPrefix = parsed_json['requestParams']['origin']+\
                  parsed_json['requestParams']['destination']
   flightSuffix = ''.join(reversed(parsed_json['requestParams']['flight_depart_date'].split('/')))
   print flightPrefix, flightSuffix
   cityNames = parsed_json['cityName']
   print cityNames
   flightSchedules = parsed_json['resultData']
   for i in range(len(flightSchedules)):
      print '' ''
      flights = int(flightSchedules[i]['isFlights'])
      print 'totalFlights: %s'%flights
      fairDetails = flightSchedules[i]['fareDetails']
   
      for key1,value1 in fairDetails.items():
         if type(value1) is dict:
            printFlightsInScedule(key1,value1)
         elif type(value1) is list:
            print key1, ':', len(value1)
         else:
            print key1, ':', value1

sys.exit(0)
