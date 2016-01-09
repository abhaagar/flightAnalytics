#!/usr/bin/python
import sys, re, gc, time, signal, pdb
import json
import MySQLdb
import datetime,time
import multiprocessing, Queue
import flightUtil
import traceback

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
   cur.execute(flightUtil.resetStopAvailabilityQuery)
   cur.fetchall()
   cur.execute(flightUtil.commit)
   cur.fetchall()
   cur.execute(flightUtil.resetStop1AvailabilityQuery)
   cur.fetchall()
   cur.execute(flightUtil.commit)
   cur.fetchall()
   cur.execute(flightUtil.resetStop2AvailabilityQuery)
   cur.fetchall()
   cur.execute(flightUtil.commit)
   cur.fetchall()


#def fetchParsedJson(origin,destination,date):
#   successOuter = False
#   retry = 3
#   parsed_json = ''
#   while not successOuter:
#      try :
#         error,string = flightUtil.fetchSanitizedInput(origin,destination,date)
#         if not error:
#            parsed_json = json.loads(string)
#            successOuter = True
#            break
#            #print "Json parse successfuly"
#         else:
#            raise Exception(string)
#      except Exception,e:
#         print 'Error1 Occurred :'+str(e)
#         print date,origin,destination 
#         print traceback.format_exc()
#         print string #$
#         print 'Error1 Occurred :'+str(e)
#         if retry:
#            retry = retry - 1
#            print 'Will be Retried'
#         else:
#            logError(str(e),str(date+' '+origin+' '+destination),1)
#            break
#   if retry<3 and retry>0:
#      print 'Retrying Payed Off'
#   return parsed_json


def fetchParsedJson(origin,destination,date):
   try :
      error,string = flightUtil.fetchSanitizedInput(origin,destination,date)
      if not error:
         parsed_json = json.loads(string)
         #print "Json parse successfuly"
      else:
         raise Exception(string)
   except Exception,e:
      print 'Error1 Occurred :'+str(e)
      #$ print date,origin,destination 
      #$ print traceback.format_exc()
      #$ print string #$
      #$ print 'Error1 Occurred :'+str(e)
      #$ logError(str(e),str(date+' '+origin+' '+destination),1)
   return parsed_json

def parseFlightScheduleAndStoreDetails(flightSchedules,pattern,route,date):
   errorOccured = False
   for i in range(len(flightSchedules)):
      try:
         successInner = False
         retry = 3
         flights = ''
         flightPrices = ''
         while not successInner:
            try:
               flights = flightSchedules[i]['fltSchedule'][pattern]
               flightPrices = flightSchedules[i]['fareDetails'][pattern]
               successInner = True
               #print 'fareDetails have been parsed'
            except Exception,e:
               return True #$
               #$print 'Error2 Occurred :'+str(e)
               #$print traceback.format_exc()
               #$route.printRoute()
               #$#print flightSchedules[i]
               #$print 'Error2 Occurred :'+str(e)
               #$if retry:
               #$   print 'Will be Retried'
               #$   retry = retry - 1
               #$else:
               #$   #logError(\
               #$   #   str(e),\
               #$   #   str(date+\
               #$   #       ' '+route.getSource()+\
               #$   #       ' '+route.getDestination()),\
               #$   #    2)
               #$   break
         if not successInner:
            errorOccured = True
            continue
         iterate = len(flightPrices)
         print 'Total Flights: %s'%str(iterate)
         for j in range(iterate):
            flight = flights[j]
            flightId = flight['ID']
            for fl in flight['OD']:
               timings = ''
               price = -1
               if successInner:
                  price = int(flightPrices[flightId]['O']['ADT']['tf'])

               for t in fl['FS']:
                  timings += t['dd']+' '+t['ad']+' '
               args = [flightId,price,timings,route.getSource(),route.getDestination()]
               print 'Argument :',args
               #print 'Executing Procedure'
               #$flightUtil.executeProcedureAndReturn(\
               #$   'maybeInsertFlightDetails',\
               #$   args)
               if not successInner:
                  print 'Entered the Mysterious Flights'
               #print 'Executed Procedure'
      except Exception, e:
         logError(str(e),'',3) #$ 
         print 'Error3 Occured :',str(e)
         return True
         logError(str(e),'',3)
         print 'Error3 Occured :',str(e)
         print traceback.format_exc()
         print 'Error3 Occured :',str(e)
         errorOccured = True
   gc.collect()
   return errorOccured

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
      parsed_json = fetchParsedJson(\
                       route.getSource(),\
                       route.getDestination(),\
                       date)
      if parsed_json=='':
         retryQueue.put(\
            flightUtil.RetryFlight(\
                                  route,\
                                  date))
         continue
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
         print 'Got FlightSchedule Right'
      except Exception,e:
         print 'Error Occured2'
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
      if parseFlightScheduleAndStoreDetails(flightSchedules,pattern,route,date):
         
         print 'Error Occured2'
         print flightSchedules
      else :
         print 'No Error Occurred'
         print flightSchedules
         #$ print parsed_json
        #$ retryQueue.put(\
        #$    flightUtil.RetryFlight(\
        #$                          route,\
        #$                          date))
   #while not retryQueue.empty():
   #    retryFlight(retryQueue.get())
   sys.exit(0) 

allMonths = [32,29,32,31,32,31,32,32,31,32,31,32]

year = '2015'
currMonth = 10
endMonth = 10
if __name__=="__main__":
   year = sys.argv[1]
   currMonth = int(sys.argv[2])
   endMonth = int(sys.argv[3])
   isResetNeeded = ''
   if len(sys.argv)==5:
      isResetNeeded = sys.argv[4]
   sys.stdout = \
      open('/home/abhinav/c++/python/logDev'+year+str(currMonth)+'.out','w+')
   sys.stderr = \
      open('/home/abhinav/c++/python/logDev'+year+str(currMonth)+'.err','w+')
   if isResetNeeded!='':
      resetTheAvailability()
      print "Reset Availability"
   q = Queue.Queue()
   gc.set_debug(gc.DEBUG_LEAK)
   def formatInTwoDigit(num):
      if num/10==0:
         num = '0'+str(num)
      return str(num)
   months = allMonths[currMonth-1:endMonth]
   dates = \
      [(j,i+1) for i in range(currMonth-1,endMonth) for j in range(1,allMonths[i])]
   for date in dates:
      iterator = 0
      while iterator < len(date):
         if q.qsize() < 1:
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
            time.sleep(10)
   sys.stdout.close()
   sys.stderr.close()

print 'exiting'
sys.exit(0)
