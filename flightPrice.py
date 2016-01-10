#!/usr/bin/python
import sys, re, gc, time, signal, os, pdb
import json
import MySQLdb
import datetime,time
import multiprocessing, Queue
import flightUtil
import traceback


#isTracingEnabled = os.environ['TRACE']
isTracingEnabled = True
flightPrefix = ''
flightSuffix = ''
unchangedFlightsPrefix = 'Unchanged Flights :'
changedFlightsPrefix = 'Changed Flights :'
newFlightsPrefix = 'New Flights :'

unchangedFlights = ''
changedFlights = ''
newFlights = ''

retryQueue = Queue.Queue() 

def cityQuery():
   return "SELECT code FROM cityWithCode WHERE isAvailable=1 order by code"

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

def logError(traceBack,information,typ):
   return #$
   c = flightUtil.connection()
   cur = c.cursor()
   query = flightUtil.errorLoggingQuery%(traceBack,information,typ)
   print query
   cur.execute(query)
   cur.fetchall()
   cur.execute(flightUtil.commit)
     

def resetTheAvailability():
   c = flightUtil.connection()
   cur = c.cursor()
   cur.execute(flightUtil.resetFlightAvailabilityQuery)
   cur.fetchall()
   cur.execute(flightUtil.commit)
   cur.fetchall()
   cur.execute(flightUtil.resetDirectFlightsDetailsAvailabilityQuery)
   cur.fetchall()
   cur.execute(flightUtil.commit)
   cur.fetchall()
   cur.execute(flightUtil.resetOneStopFlightsDetailsAvailabilityQuery)
   cur.fetchall()
   cur.execute(flightUtil.commit)
   cur.fetchall()
   cur.execute(flightUtil.resetTwoStopFlightsDetailsAvailabilityQuery)
   cur.fetchall()
   cur.execute(flightUtil.commit)
   cur.fetchall()


def fetchParsedJson(origin,destination,date):
   successOuter = False
   retry = 3
   parsed_json = ''

   error,string = flightUtil.fetchSanitizedInput(origin,destination,date)
   if not error:
      try :
         parsed_json = json.loads(string)
      except Exception,e:
         print 'Error :: Invalid Json '+str(e)
         logError(str(e),str(date+' '+origin+' '+destination),1)
   return parsed_json



def parseFlightScheduleAndStoreDetails(flightSchedules,pattern,route,date):
   for i in range(len(flightSchedules)):
      try:
         flights = ''
         flightPrices = ''
         try:
            flights = flightSchedules[i]['fltSchedule'][pattern]
            if isTracingEnabled:
               print 'Flight Schedule %d Parsed'%i
            flightPrices = flightSchedules[i]['fareDetails'][pattern]
            if isTracingEnabled:
               print 'Fare Details of Flight Schedule %d Parsed'%i
         except Exception,e:
            if flights=='':
               print 'Error :: Invalid or Absent fltSchedules '+str(e)
               print flightSchedule[i]
               continue
         iterate = len(flights)
         if flightPrices=='':
            print 'Flight Schedules without fareDetails'
         if isTracingEnabled:
            print 'Total Flights: %s'%str(iterate)
         for j in range(iterate):
            isStoredSuccessfuly = False
            try:
               flight = flights[j]
               flightId = flight['ID']
               if isTracingEnabled:
                  print 'Flight id %s'%flightId
               if len(flight['OD'])!=1:
                  print 'Multiple OD entries'
                  continue
               for fl in flight['OD']:
                  timings = ''
                  price = -1
                  if flightPrices!='':
                     price = int(flightPrices[flightId]['O']['ADT']['tf'])
                     if isTracingEnabled:
                        print 'Fare of Flight parsed'
                  for t in fl['FS']:
                     timings += t['dd']+' '+t['ad']+' '
                  args = \
                     [flightId,price,timings,route.getSource(),route.getDestination()]
                  if isTracingEnabled:
                     print 'Flight Timimgs Parsed'
                     print 'Executing Procedure with arguments :',args
                  if flightPrices=='':
                     print 'Entered the Mysterious Flights'
                     continue
                  print flightUtil.executeProcedureAndReturn(\
                     'maybeInsertFlightDetails',\
                     args)
                  isStoredSuccessfuly = True

                  if isTracingEnabled:
                    print 'Executed Procedure Successfuly'
            except Exception, e:
               if isStoredSuccessfuly:
                  print 'Error parsing Flight Id/Fare/Timings'+str(e)
               else:
                  print 'Error Procedure Execution'+str(e)

      except Exception, e:
         print 'Unexpected Error '+str(e)
         return False
   gc.collect()
   return True

def retryFlight(retryNow):
   retry = retryNow.getRoute()
   parsed_json = fetchParsedJson(\
                    route.getSource(),\
                    route.getDestination(),\
                    retryNow.getDate())
   if parsed_json=='':
      logError(\
         'RetryError',\
         retryNow.retryInformation(),\
         5)
      return
   try:
      flightPrefix = parsed_json['requestParams']['origin']+\
                     parsed_json['requestParams']['destination']
      flightSuffix = \
         ''.join(reversed(\
                 parsed_json['requestParams']['flight_depart_date'].split('/')))
      cityNames = parsed_json['cityName']
      flightSchedules = parsed_json['resultData']
      #print 'Flight Schedules Parsed'
      #print 'Total Flight Schedules: %s'%str(len(flightSchedules))
   except Exception,e:
      logError(\
         'RetryError '+str(e),\
         retryNow.retryInformation(),\
         5)
      return
   pattern = flightUtil.flightSearchPattern(\
                                            route.getSource(),\
                                            route.getDestination(),\
                                            date)
   pattern = pattern.replace('%','')
   if parseFlightScheduleAndStoreDetails(flightSchedules,pattern,route,date):
      logError(\
         'RetryError ',\
         retryNow.retryInformation(),\
         5)

def maybeInsertFlightHistoryForDate(date):
   retryQueue = Queue.Queue()
   signal.signal(signal.SIGINT,flightUtil.handler)
   gc.set_debug(gc.DEBUG_LEAK)
   print date
   cityPairs = cityPairList()
   for pair in cityPairs:
      route = flightUtil.Route(pair[0],pair[1])
      print 'New Request'
      print date,route.getSource(),route.getDestination()
      parsed_json = fetchParsedJson(\
                       route.getSource(),\
                       route.getDestination(),\
                       date)
      if parsed_json=='':
         #retryQueue.put(\
         #   flightUtil.RetryFlight(\
         #                         route,\
         #                         date))
         continue
      if isTracingEnabled:
         print 'Flight Schedules Parsed'
      try:
         flightPrefix = parsed_json['requestParams']['origin']+\
                        parsed_json['requestParams']['destination']

         flightSuffix = \
            ''.join(reversed(\
                    parsed_json['requestParams']['flight_depart_date'].split('/')))
         cityNames = parsed_json['cityName']
         flightSchedules = parsed_json['resultData']
         if isTracingEnabled:
             print 'Result Data Parsed'
             print 'Total Flight Schedules: %s'%str(len(flightSchedules))
      except Exception,e:
         print 'Error :: Insufficient Data in Parsed Json'+str(e)
         #$ print parsed_json
         #logError(str(e),'',4)
         #retryQueue.put(\
         #   flightUtil.RetryFlight(\
         #                         route,\
         #                         date))
         continue
      pattern = flightUtil.flightSearchPattern(\
                                               route.getSource(),\
                                               route.getDestination(),\
                                               date)
      pattern = pattern.replace('%','')
      parseFlightScheduleAndStoreDetails(flightSchedules,pattern,route,date)
      #$ retryQueue.put(\
      #$    flightUtil.RetryFlight(\
      #$                          route,\
      #$                          date))
   #while not retryQueue.empty():
   #    retryFlight(retryQueue.get())
   sys.exit(0) 

allMonths = [32,30,32,31,32,31,32,32,31,32,31,32]

year = '2015'
currMonth = 10
endMonth = 10
if __name__=="__main__":
   year = sys.argv[1]
   startMonth = int(sys.argv[2])
   endMonth = int(sys.argv[3])
   today = int(time.strftime('%d'))
   currMonth = int(time.strftime('%m'))
   isResetNeeded = ''
   if len(sys.argv)==5:
      isResetNeeded = sys.argv[4]
   sys.stdout = \
      open('/home/abhinav/c++/python/logDev'+year+str(startMonth)+'.out','w+')
   sys.stderr = \
      open('/home/abhinav/c++/python/logDev'+year+str(startMonth)+'.err','w+')
   if isResetNeeded!='':
      print "Going to Reset Availability"
      resetTheAvailability()
      print "Reset Availability"
   q = Queue.Queue()
   gc.set_debug(gc.DEBUG_LEAK)
   def formatInTwoDigit(num):
      if num/10==0:
         num = '0'+str(num)
      return str(num)
   months = allMonths[startMonth-1:endMonth]
   dates = \
      [(j,i+1) for i in range(startMonth-1,endMonth) for j in range(1,allMonths[i]) if ((i+1)==currMonth and j>=today) or (i+1)!=currMonth]
   for date in dates:
      iterator = 0
      while iterator < len(date):
         if q.qsize() < 2:
            d = formatInTwoDigit(date[0])+'/'+formatInTwoDigit(date[1])+'/'+year
            pid = multiprocessing.Process(target=maybeInsertFlightHistoryForDate,args=(d,),name=(str(date)))
            q.put(pid)
            pid.start()
            iterator = iterator+1
         else:
            prevSize = q.qsize()
            for i in range(prevSize):
                pid = q.get()
                if pid.is_alive():
                   q.put(pid)
            time.sleep(5)
   sys.stdout.close()
   sys.stderr.close()

print 'exiting'
sys.exit(0)
