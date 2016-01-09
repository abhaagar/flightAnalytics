# 2016.01.09 11:24:08 IST
import MySQLdb
import re
import sys
import gc
import json
import datetime
import signal
import traceback
import random
import urllib2
import gzip
import cStringIO
appError = 0
sysError = 0
airlineNameQuery = "SELECT name FROM airline where code='%s'"
airlineQuery = 'SELECT name, code FROM airline'
cityNameQuery = "SELECT name FROM cityWithCode where code='%s'"
cityQuery = 'SELECT name,code FROM cityWithCode'
directFlightsQuery = "SELECT DISTINCT name,timings FROM flights as A,directFlightsDetails as B WHERE A.id=B.fid AND A.name LIKE '%s' AND departureCity='%s' AND arrivalCity='%s'AND A.isAvailable=1 AND B.isAvailable=1"
oneStopFlightsQuery = "SELECT DISTINCT name,timings FROM flights as A,oneStopFlightsDetails as B WHERE A.id=B.fid AND A.name LIKE '%s' AND departureCity='%s' AND arrivalCity='%s'AND A.isAvailable=1 AND B.isAvailable=1"
twoStopFlightsQuery = "SELECT DISTINCT name,timings FROM flights as A,twoStopFlightsDetails as B WHERE A.id=B.fid AND A.name LIKE '%s' AND departureCity='%s' AND arrivalCity='%s'AND A.isAvailable=1 AND B.isAvailable=1"
errorLoggigQuery = "Insert into errorLog (traceBack,information,type) values('%s','%s',%s)"
directFlightPriceHistoryQuery = "SELECT price,DATE_FORMAT(C.sampleTime,'%%Y/%%m/%%d') as date FROM flights as A,directFlightsDetails as B,directFlightsPrices as C WHERE A.id=B.fid and B.id=C.sid and name='%s' ORDER BY date asc"
oneStopFlightPriceHistoryQuery = "SELECT price,DATE_FORMAT(oneStopFlightsPrices.sampleTime,'%%Y/%%m/%%d') as date FROM flights as A,oneStopFlightsDetails as B,oneStopFlightsPrices as C WHERE A.id=B.fid and B.id=C.sid and name='%s' ORDER BY date asc"
twoStopFlightPriceHistoryQuery = "SELECT price,DATE_FORMAT(twoStopFlightsPrices.sampleTime,'%%Y/%%m/%%d') as date FROM flights as A,twoStopFlightsDetails as B,twoStopFlightsPrices as C WHERE A.id=B.fid and B.id=C.sid and name='%s' ORDER BY date asc"
metricPriceHistoryQuery = "SELECT Date_FORMAT(sampleDate,'%%d/%%m/%%Y'), %s(price) FROM directFlights WHERE depart>='%s' and depart<='%s' and departureCity='%s' and arrivalCity='%s' and name LIKE '%s' GROUP BY sampleDate"
directFlightHistoryQuery = "SELECT Date_FORMAT(C.sampleDate,'%%d/%%m/%%Y'), %s(price) FROM flights as A, directFlightsDetails as B, directFlightsPrices as CWHERE A.id=B.fid and B.id=C.sid "
oneStopFlightHistoryQuery = "SELECT Date_FORMAT(C.sampleDate,'%%d/%%m/%%Y'), %s(price) FROM flights as A, oneStopFlightsDetails as B, oneStopFlightsPrices as C"
directFlightHistoryQuery = "SELECT Date_FORMAT(C.sampleDate,'%%d/%%m/%%Y'), %s(price) FROM flights as A, twoStopFlightsDetails as B, twoStopFlightsPrices as C"
resetFlightAvailabilityQuery = 'UPDATE flights SET isAvailable=0'
resetDirectFlightsDetailsAvailabilityQuery = 'UPDATE directFlightsDetails SET isAvailable=0'
resetOneStopFlightsDetailsAvailabilityQuery = 'UPDATE oneStopFlightsDetails SET isAvailable=0'
resetTwoStopFlightsDetailsAvailabilityQuery = 'UPDATE twoStopFlightsDetails SET isAvailable=0'
errorLoggingQuery = "INSERT INTO errorLog (traceBack,information,type) VALUES('%s','%s','%s')"
commit = 'commit'

class Button:

    def __init__(self, label, value):
        self.label = label
        self.value = value



    def setLabel(self, newLabel):
        self.label = newLabel



    def setValue(self, newValue):
        self.value = newValue



    def getLabel(self):
        return self.label



    def getValue(self):
        return self.value




class Route:

    def __init__(self, source, destination):
        self.source = source
        self.destination = destination



    def setSource(self, newSource):
        self.source = newSource



    def setDestination(self, newDestination):
        self.destination = newDestination



    def getSource(self):
        return self.source



    def getDestination(self):
        return self.destination



    def printRoute(self):
        print self.source,
        print self.destination



    def routeInformation():
        return self.source + self.destination




class RetryFlight:

    def __init__(self, route, date):
        self.route = route
        self.date = date



    def getRoute():
        return self.route



    def getDate():
        return self.date



    def retryInformation():
        return self.route.routeInformation() + self.date




class Flight:

    def __init__(self, route, date, price, airline):
        self.route = route
        self.date = date
        self.price = price
        self.airline = airline




class Journey:

    def __init__(self, departure, arrival):
        self.departure = departure
        self.arrival = arrival



    def getDeparture():
        return self.departure



    def getArrival():
        return self.arrival




class DirectFlight(Flight):

    def __init__(self, route, date, price, airline, journey):
        Flight.__init__(self, route, date, price, airline)
        self.journey = journey




class OneStopFlight(Flight):

    def __init__(self, route, date, price, airline, firstJourney, lastJourney):
        Flight.__init__(self, route, date, price, airline)
        self.firstJourney = firstJourney
        self.lastJourney = lastJourney




class TwoStopFlight(Flight):

    def __init__(self, route, date, price, airline, firstJourney, middleJourney, lastJourney):
        Flight.__init__(self, route, date, price, airline)
        self.firstJourney = firstJourney
        self.middleJourney = middleJourney
        self.lastJourney = lastJourney



months = {'01': 'January',
 '02': 'February',
 '03': 'March',
 '04': 'April',
 '05': 'May',
 '06': 'June',
 '07': 'July',
 '08': 'August',
 '09': 'September',
 '10': 'October',
 '11': 'November',
 '12': 'December',
 '': 'Select'}

def diff(a, b):
    b = set(b)
    return [ aa for aa in a if aa not in b ]



def handler(signum, frame):
    print traceback.print_stack()
    sys.exit(0)



def flightType(flt):
    pos = flt.find(flt[3:6], 4)
    if pos == -1:
        return 'direct'
    pos = flt.find(flt[(pos + 3):(pos + 6)], pos + 4)
    if pos == -1:
        return 'oneStop'
    return 'twoStop'



def currentDate():
    i = datetime.datetime.now()
    return '%s %s %s' % (i.year, i.month, i.day)



def slotList():
    type1Slot = '0%s:00'
    type2Slot = '%s:00'
    slotList = []
    for i in range(10):
        slotList.append(type1Slot % str(i))

    for i in range(11, 24):
        slotList.append(type2Slot % str(i))

    return slotList



def flightPrefix(orig, dest):
    return orig + dest



def flightSufix(yyyymmdd):
    yyyymmdd = yyyymmdd.replace('/', ' ')
    yyyymmdd = yyyymmdd.replace('-', ' ')
    yyyymmdd = yyyymmdd.split()
    yyyymmdd = yyyymmdd[2] + yyyymmdd[1] + yyyymmdd[0]
    return yyyymmdd



def jsonList(lst):
    jsn = []
    for l in lst:
        d = {}
        d[str(l[1])] = str(l[0])
        jsn.append(d)

    return json.dumps(jsn)



def flightQuerySufix(yyyymmdd):
    yyyymmdd = yyyymmdd.replace('/', ' ')
    yyyymmdd = yyyymmdd.replace('-', ' ')
    yyyymmdd = yyyymmdd.split()
    yyyymmdd = yyyymmdd[2] + yyyymmdd[0] + yyyymmdd[1]
    return yyyymmdd



def flightSearchPattern(orig, dest, date):
    prefix = flightPrefix(orig, dest)
    sufix = flightSufix(date)
    return prefix + '%' + sufix



def flightQueryPattern(airline, date):
    prefix = airline
    sufix = flightQuerySufix(date)
    return [prefix, '%' + sufix + '%']



def flightQueryPattern(airline, orig, date):
    airlinePattern = ''
    if airline == '':
        airlinePattern = '__'
    else:
        airlinePattern = airline
    suffix = flightQuerySufix(date)
    return orig + '%' + airlinePattern + '%' + suffix



def findAndSanitizeInput(response):
    match = ''
    try:
        for line in response:
            data = re.search('eagerFetch', line)
            if data:
                match = line
                break

        splited = match.replace(';', '=').split('=', 9)
        return (False, splited[1])
    except Exception as e:
        return (True, splited)



def createConnection():
    try:
        cnx = MySQLdb.connect(host='127.0.0.1', user='root', passwd='root', db='flightDetails')
    except Exception as e:
        print str(e)
        sys.exit(1)
    return cnx



def connection():
    cnx = createConnection()
    return cnx



def executeQueryAndReturn(query):
    try:
        cnx = connection()
        cur = cnx.cursor()
        cur.execute(query)
        print query
        data = cur.fetchall()
        cnx.close()
    except Exception as e:
        return 'Error' + str(e)
    return data



def executeProcedureAndReturn(procName, args):
    cnx = connection()
    cur = cnx.cursor()
    cur.callproc(procName, args)
    result = cur.fetchall()
    cnx.close()
    return result



def cityPairList():
    cnx = connection()
    cur = cnx.cursor()
    cur.execute(cityQuery)
    dataDuplicate = cur.fetchall()
    data = set(dataDuplicate)
    cnx.close()
    return [ (city1[0], city2[0]) for city1 in data for city2 in data if city1[0] != city2[0] ]



def fetchCityPairs(cityPair = cityPairList()):
    return cityPair



def airlineName(code):
    airlines = executeQueryAndReturn(airlineQuery)
    for airline in airlines:
        if code == airline[1]:
            return airline[0]

    return code



def flightDescriptionPair2(cityList, timings):
    fstr = ''
    iterLen = len(cityList) - 1
    for i in range(iterLen):
        fstr += cityList[i] + '( ' + timings[(2 * i)] + ' )' + ' >->> ' + '( ' + timings[(2 * i + 1)] + ' )'

    fstr += cityList[iterLen]
    return fstr



def flightDescription(code, timings):
    city1 = ''
    city2 = ''
    city3 = ''
    city4 = ''
    prefix = airlineName(code[6:8])
    pair1 = code
    pair2 = prefix + ' '
    timings = timings.split(' ')
    sufix = [ timing + ' ' for timing in timings ]
    cities = executeQueryAndReturn(cityQuery)

    def cityNames(codes):
        names = []
        for code in codes:
            name = code
            for city in cities:
                if city[1] == code:
                    name = city[0]
                    break

            names.append(name)

        return names


    city1 = code[0:3]
    city2 = code[3:6]
    city3Pos = code.find(city2, 7)
    if city3Pos == -1:
        pair2 += flightDescriptionPair2(cityNames([city1, city2]), sufix)
        return (pair1, pair2)
    city3 = code[(city3Pos + 3):(city3Pos + 6)]
    city4Pos = code.find(city3, city3Pos + 7)
    if city4Pos == -1:
        pair2 += flightDescriptionPair2(cityNames([city1, city2, city3]), sufix)
        return (pair1, pair2)
    city4 = code[(city4Pos + 3):(city4Pos + 6)]
    pair2 += flightDescriptionPair2(cityNames([city1,
     city2,
     city3,
     city4]), sufix)
    return (pair1, pair2)



def fetchSanitizedInput(orig, dest, date, travlr = 1):
    uniq = random.randint(100000000, 999999999)
    url = 'http://www.yatra.com/air-search/dom2/trigger?type=O&viewName=normal&flexi=0&noOfSegments=1&origin=%s&originCountry=IN&destination=%s&destinationCountry=IN&flight_depart_date=%s&ADT=%s&CHD=0&INF=0&class=Economy&hb=False&unique=%d' % (orig,
     dest,
     date,
     travlr,
     uniq)
    request = urllib2.Request(url)
    request.add_header('Host', 'www.yatra.com')
    request.add_header('Accept', '*/*')
    request.add_header('Accept-Language', 'en-US,en;q=0.5')
    request.add_header('Accept-Encoding', 'gzip, deflate')
    request.add_header('X-Requested-With', 'XMLHttpRequest')
    request.add_header('User-Agent', 'Mozilla/5.0 (Linux x86_64) Gecko/20100101 Firefox/41.0')
    response = urllib2.urlopen(request)
    if response.info().get('Content-Encoding').find('gzip') != -1:
        f = gzip.GzipFile(fileobj=cStringIO.StringIO(response.read()))
        line = f.readline()
        while line != '':
            try:
                if re.search('eagerFetch', line):
                    line = line.replace(';', '=').split('=', 4)
                    return (False, line[1])
            except Exception as e:
                return (True, line)
            line = f.readline()

    return (False, line)



# decompiled 1 files: 1 okay, 0 failed, 0 verify failed
# 2016.01.09 11:24:09 IST
