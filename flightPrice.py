#!/usr/bin/python
import sys, re, gc, time, pdb
import json
import MySQLdb
import datetime,time
import flightUtil

flightPrefix = ''
flightSuffix = ''
#cnx = ''
unchangedFlightsPrefix = 'Unchanged Flights :'
changedFlightsPrefix = 'Changed Flights :'
newFlightsPrefix = 'New Flights :'

unchangedFlights = ''
changedFlights = ''
newFlights = ''

def cityQuery():
   return "SELECT code FROM cityWithCode"

def cityPairList():
   cnx = flightUtil.connection()
   cur = cnx.cursor()
   cur.execute(cityQuery())
   dataDuplicate = cur.fetchall()
   data = set(dataDuplicate)
   cnx.close()
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

def resetTheAvailability():
   c = flightUtil.connection()
   cur = c.cursor()
   cur.execute(flightUtil.resetFlightAvailabilityQuery)

months = [32,31,32,31,32,31]
year = '2016'
currMonth = 1
if __name__=="__main__":
   sys.stdout = open('/home/abhinav/c++/python/logDev.out','a+')
   sys.stderr = open('/home/abhinav/c++/python/logDev.err','a+')
   #pdb.set_trace()
   #cnx = flightUtil.connection()
   resetTheAvailability()
   cityPairs = cityPairList()
   m = currMonth - 1
   gc.set_debug(gc.DEBUG_LEAK)
   for month in months:
      m = m + 1
      for day in range(1,month):
         def formatInTwoDigit(num):
            if num/10==0:
               num = '0'+str(num)
            return str(num)
         date = formatInTwoDigit(day)+'/'+formatInTwoDigit(m)+'/'+year
         print date
         for pair in cityPairs:
            print pair
            try :
               string = flightUtil.fetchSanitizedInput(pair[0],pair[1],date)
               parsed_json = json.loads(string)
               print "Json parse successfuly"
            except Exception,e:

               print 'Error1 Occurred :'+str(e)
               print pair
               print string
               print parsed_json
               print 'Error1 Occurred :'+str(e)
               continue;

            flightPrefix = parsed_json['requestParams']['origin']+\
                           parsed_json['requestParams']['destination']
            flightSuffix = \
               ''.join(reversed(parsed_json['requestParams']['flight_depart_date'].split('/')))
            cityNames = parsed_json['cityName']
            flightSchedules = parsed_json['resultData']
            print 'Flight Schedules Parsed'
            print 'Total Flight Schedules: %s'%str(len(flightSchedules))
            for i in range(len(flightSchedules)):
               try:
                  pat = flightUtil.flightSearchPattern(pair[0],pair[1],date)
                  pat = pat.replace('%','')
                  try:
                     flights = flightSchedules[i]['fltSchedule'][pat]
                     
                     flightPrices = flightSchedules[i]['fareDetails'][pat]
                     print 'fareDetails have been parsed'
                  except Exception,e:
                     print 'Error2 Occurred :'+str(e)
                     #pdb.set_trace()
                     print pair
                     #print flightSchedules[i]
                     print 'Error2 Occurred :'+str(e)
                     continue
                  iterate = len(flightPrices)
                  print 'Total Flights: %s'%str(iterate)
                  for j in range(iterate):
                     flight = flights[j]
                     flightId = flight['ID']
                     for fl in flight['OD']:
                        timings = ''
                        price = flightPrices[flightId]['O']['ADT']['tf']

                        for t in fl['FS']:
                           timings += t['dd']+' '+t['ad']+' '
                        args = [flightId,price,timings,pair[0],pair[1]]
                        print 'Executing Procedure'
                        flightUtil.executeProcedureAndReturn(\
                           'maybeInsertFlightDetails',\
                           args)
                        print 'Executed Procedure'
               except Exception, e:
                  print 'Error3 Occured :',str(e)
                  sys.exit(1)
            gc.collect()
   sys.stdout.close()
   sys.stderr.close()

print 'exiting'
sys.exit(0)
