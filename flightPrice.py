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
unchangedFlightsPrefix = 'Unchanged Flights :'
changedFlightsPrefix = 'Changed Flights :'
newFlightsPrefix = 'New Flights :'

unchangedFlights = ''
changedFlights = ''
newFlights = ''

def cityQuery():
   return "SELECT code FROM cityWithCode"

def cityPairList():
   cur = cnx.cursor()
   cur.execute(cityQuery())
   dataDuplicate = cur.fetchall()
   print dataDuplicate
   data = set(dataDuplicate)
   print data
   return [(city1[0],city2[0]) for city1 in data for city2 in data if city1[0]!=city2[0]]
   

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
   global cnx, changedFlights, unchangedFlights, newFlights 
   cur = cnx.cursor()
   selectQuery = createSelectQuery(flight)
   cur.execute(selectQuery)	
   data = cur.fetchone()
   if cur.rowcount==0:
      insertFlightUpdatedPrice(cur,flight,price)
      newFlights += ' '+flight
   elif data[0]!=int(price):
      insertFlightUpdatedPrice(cur,flight,price)
      changedFlights += ' '+flight
   else:
      unchangedFlights += ' '+flight

def printFlightsInScedule(key,value):
   global flightPrefix
   global flightSuffix
   for key1,value1 in value.items():
      if re.match(flightPrefix,key1) is not None and key1.endswith(flightSuffix): 
         checkAndInsertFlightPrice(key1,value1['O']['ADT']['tf'])
#months = [31,30,31]
months = [31,30.31]
year = '2015'
currMonth = 10
if __name__=="__main__":
   sys.stdout = open('/home/abhinav/c++/python/log.out','a+')
   sys.stderr = open('/home/abhinav/c++/python/log.err','a+')

   print datetime.datetime.fromtimestamp(\
         time.time()).strftime('%Y-%m-%d %H:%M:%S')
   cnx = flightUtil.createConnection()
   cityPairs = cityPairList()
   #print cityPairs
   try:
      sys.stdout.close()
      sys.stderr.close()
   except Exception,e:
      print str(e)
   m = currMonth - 1
   for month in months:
      m = m + 1
      for day in range(1,month):
         d = day 
         if day/10==0:
           d = '0'+str(day)
         date = year+str(m)+str(d)
      for pair in cityPairs:
         sys.stdout = open('/home/abhinav/c++/python/log/%s%s.out'%(pair[0],pair[1]),'a+')
         sys.stderr = open('/home/abhinav/c++/python/log/%s%s.err'%(pair[0],pair[1]),'a+')
         print datetime.datetime.fromtimestamp(\
               time.time()).strftime('%Y-%m-%d %H:%M:%S')
         print pair[0],pair[1],date
         string = flightUtil.fetchSanitizedInput(pair[0],pair[1],date)
         try:
            parsed_json = json.loads(string)
            flightPrefix = parsed_json['requestParams']['origin']+\
                           parsed_json['requestParams']['destination']
            flightSuffix = \
               ''.join(reversed(parsed_json['requestParams']['flight_depart_date'].split('/')))
            cityNames = parsed_json['cityName']
            flightSchedules = parsed_json['resultData']
         except Exception,e:
            print 'Error occured',str(e)
            try:
               sys.stdout.close()
               sys.stderr.close()
            except Exception,e:
               print 'Error occured',str(e)
            continuei;
         flightPattern = pair[0]+pair[1]+date
         for i in range(len(flightSchedules)):
            query = ''
            for j in flightSchedules[i]['fltSchedule'][flightPattern]:
               flight = j['ID']
               for k in j['OD']:
                  for l in k['FS']: 
         for i in range(len(flightSchedules)):
            try:
               changedFlights = changedFlightsPrefix
               unchangedFlights = unchangedFlightsPrefix
               newFlights = newFlightsPrefix
               
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
               if changedFlights!=changedFlightsPrefix:
                  print changedFlights
               if unchangedFlights!=unchangedFlightsPrefix:
                  print unchangedFlights
               if newFlights!=newFlightsPrefix:
                  print newFlights
            except Exception, e:
               print 'Error occured',str(e)
               try:
                  sys.stdout.close()
                  sys.stderr.close()
               except Exception,e:
                  print 'Error occured',str(e)
         try :
            sys.stdout.close()
            sys.stderr.close()
         except Exception, e:
            print 'Error occured in closing ',str(e)
            sys.exit(1)
print 'exiting'
sys.exit(0)
