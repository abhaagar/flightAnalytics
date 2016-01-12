#!/usr/bin/python

from flask import Flask
from flask import render_template, request
import flightUtil
import sys, datetime, time
import json


app = Flask(__name__)

@app.route('/index',methods=['POST','GET'])
def index():
   return render_template('index.html')


@app.route('/flight/<name>', methods=['POST'])
def flight(name):
   typ = flightUtil.flightType(name)
   query = ''
   if typ=="direct":
      query = flightUtil.directFlightPriceHistoryQuery
   elif typ=="oneStop":
      query = flightUtil.oneStopFlightPriceHistoryQuery
   elif typ=="twoStop":
      query = flightUtil.twoStopFlightPriceHistoryQuery

   priceHistory = flightUtil.executeQueryAndReturn(
                     query%name)
   print priceHistory
   priceHistory = [(price[0],price[1]) for price in priceHistory]
   history =''
   p = ''
   print priceHistory
   for i in range(len(priceHistory)):
      p = priceHistory[i][0]
      dt = str(priceHistory[i][1])
      print dt
      dt = dt.split('/')
      history += dt[0]+' '+dt[1]+' '+dt[2]+' '
      history += str(priceHistory[i][0])+';'
   history += (flightUtil.currentDate())+' '
   history += str(p)
   print history
   return history

@app.route('/flightHistory/', methods=['POST','GET'])
def flightHistory():
   airlineButton = flightUtil.Button('Select','') 
   originButton = flightUtil.Button('Select','')
   destinationButton = flightUtil.Button('Select','')
   stopButton = flightUtil.Button('Select','')
   oneStop = []
   twoStop = []
   direct = []
   fltLst = []
   dt = ''
   try:
      cities = flightUtil.executeQueryAndReturn(flightUtil.cityQuery)
      als = flightUtil.executeQueryAndReturn(flightUtil.airlineQuery)
      als = (('Select',''),)+als
      if request.method=='POST':
         originButton.setValue(request.form['selectedOrigin'])
         destinationButton.setValue(request.form['selectedDestination'])
         dt = request.form['datepicker']
         airlineButton.setValue(request.form['selectedAirline'])
         stopButton.setValue(request.form['selectedStop'])
         stopButton.setLabel(request.form['selectedStop'])
         pattern = flightUtil.flightQueryPattern(\
                               airlineButton.getValue(),\
                               originButton.getValue(),\
                               dt)
         print pattern
         def flightView(query,pattern,orig,dest):
            fltLst = []
            nowTime = time.strftime("%H:%M")
            currentDay = time.strftime("%d")
            currentMonth = time.strftime("%m")
            flightDate = dt
            todayDate = time.strftime('%m/%d/%Y')  
            fltHst = \
               flightUtil.executeQueryAndReturn( \
                  query%(pattern,orig,dest))
            for flt in fltHst:
               if flightDate!=todayDate or nowTime < flt[1][:5]: 
                  fltLst.append(flightUtil.flightDescription(flt[0],flt[1]))
            return fltLst
         twoStop = flightView(\
            flightUtil.twoStopFlightsQuery,pattern,originButton.getValue(),destinationButton.getValue())
         oneStop = flightView(\
            flightUtil.oneStopFlightsQuery,pattern,originButton.getValue(),destinationButton.getValue())
         direct  = flightView(\
            flightUtil.directFlightsQuery,pattern,originButton.getValue(),destinationButton.getValue())
         destinationButton.setLabel(\
            flightUtil.executeQueryAndReturn(\
               flightUtil.cityNameQuery%(destinationButton.getValue()))[0][0]) 
         originButton.setLabel(\
            flightUtil.executeQueryAndReturn(\
               flightUtil.cityNameQuery%(originButton.getValue()))[0][0])
         if airlineButton.getValue()!='':
            airlineButton.setLabel(flightUtil.executeQueryAndReturn(\
                      flightUtil.airlineNameQuery%(airlineButton.getValue()))[0][0])
         else :
           airlineButton.setLabel('Select')
         if stopButton.getValue()=='':
            stopButton.setLabel('Select')
   except Exception, e:
      print 'Error occured'+str(e)
      return render_template('error.html',error=str(e))
   return render_template( \
             'fetchFlightHistory.html', \
             origCities=cities, \
             destCities=cities, \
             origin=originButton.getLabel(), \
             destination=destinationButton.getLabel(), \
             originValue=originButton.getValue(),\
             destinationValue=destinationButton.getValue(),\
             date=dt, \
             airlines=als,\
             airlineLabel=airlineButton.getLabel(),\
             airlineValue=airlineButton.getValue(),\
             stop=stopButton.getLabel(),\
             stopValue=stopButton.getValue(),\
             oneStopFlights=oneStop,\
             twoStopFlights=twoStop,\
             directFlights=direct)

@app.route('/priceHistory/',methods=['POST','GET'])
def priceHistory():
   slotFrom='Select'
   slotTo='Select'
   origin='Select'
   originValue=''
   destination='Select'
   destinationValue=''
   date=''
   metrics=['MIN','AVG','MAX'] 
   metric='Select'
   history=[]
   try:
      cities = flightUtil.executeQueryAndReturn(flightUtil.cityQuery)
      if request.method=='POST':
         slotFrom=request.form['selectedSlotFrom']
         slotTo=request.form['selectedSlotTo']
         origin=request.form['selectedOrigin']
         destination=request.form['selectedDestination']
         date=request.form['datepicker']
         metric=request.form['selectedMetric']
         query = flightUtil.metricPriceHistoryQuery%(\
                                             metric,\
                                             slotFrom,\
                                             slotTo,\
                                             origin,\
                                             destination,\
                                             origin+'%'+flightUtil.flightQuerySufix(date)+'%')
         print query
         history=flightUtil.executeQueryAndReturn(query)
         print history
         priceHistory = [(hist[0],hist[1]) for hist in history]
         print priceHistory
         history =''
         p = ''
         for i in range(len(priceHistory)):
            p = priceHistory[i][1]
            dt = str(priceHistory[i][0])
            print dt
            dt = dt.split('/')
            history += dt[2]+' '+dt[1]+' '+dt[0]+' '
            history += str(p)+':'

         history += (flightUtil.currentDate())+' '
         history += str(p)
         print history
         originValue = origin
         destinationValue = destination
         destination = flightUtil.executeQueryAndReturn(\
                          flightUtil.cityNameQuery%(destination))[0][0] 
         origin = flightUtil.executeQueryAndReturn(\
                     flightUtil.cityNameQuery%(origin))[0][0]
   except Exception, e:
      print 'Error occured'+str(e)
      return render_template('error.html',error=str(e))
   return render_template( \
             'fetchPriceHistory.html',\
             origCities=cities, \
             destCities=cities, \
             origin=origin,\
             originValue=originValue,\
             destination=destination,\
             destinationValue=destinationValue,\
             slots=flightUtil.slotList(),\
             slotFrom=slotFrom,\
             slotTo=slotTo,\
             date=date,\
             metrics=metrics,\
             metric=metric,\
             history=history)



@app.route('/cityHistory/', methods=['POST','GET'])
def cityHistory():
   print 'cityHistory'
   return 'cityHistory'

@app.route('/')
def hello_world():
   return 'Hello World!'

@app.route('/monthlyStatistics/',methods=['POST','GET'])
def monthlyStatistics():
   originButton = flightUtil.Button('Select','') 
   destinationButton = flightUtil.Button('Select','')
   airlineButton = flightUtil.Button('Select','')
   monthButton = flightUtil.Button('Select','')
   yearButton = flightUtil.Button('Select','Select')

   try:
      als = flightUtil.executeQueryAndReturn(flightUtil.airlineQuery)
      als = (('Select',''),)+als
      cities = flightUtil.executeQueryAndReturn(flightUtil.cityQuery)

      if request.method=='POST':
         originButton.setValue(request.form['selectedOrigin'])
         destinationButton.setValue(request.form['selectedDestination'])
         airlineButton.setValue(request.form['selectedAirline']) 
         yearButton.setValue(request.form['selectedYear'])
         yearButton.setLabel(request.form['selectedYear'])
         monthButton.setValue(request.form['selectedMonth'])
         #monthButton.setLabel(flightUtil.months[monthButton.getValue()])
         destinationButton.setLabel(\
            flightUtil.executeQueryAndReturn(\
               flightUtil.cityNameQuery%(destinationButton.getValue()))[0][0])
         originButton.setLabel(\
            flightUtil.executeQueryAndReturn(\
               flightUtil.cityNameQuery%(originButton.getValue()))[0][0])
         if airlineButton.getValue()!='':
            airlineButton.setLabel(flightUtil.executeQueryAndReturn(\
                      flightUtil.airlineNameQuery%(airlineButton.getValue()))[0][0])
         else :
            airlineButton.setLabel('Select')
   except Exception, e:
      print 'Error occured'+str(e)
      return render_template('error.html',error=str(e))
   return render_template( \
             'fetchMonthlyStatistics.html', \
             origCities=cities, \
             destCities=cities, \
             origin=originButton.getLabel(), \
             destination=destinationButton.getLabel(), \
             originValue=originButton.getValue(),\
             destinationValue=destinationButton.getValue(),\
             airlines=als,\
             airlineLabel=airlineButton.getLabel(),\
             airlineValue=airlineButton.getValue(),\
             year=yearButton.getLabel(),\
             monthLabel=monthButton.getLabel(),\
             monthValue=monthButton.getValue())


if __name__ == '__main__':
   #app.run(debug=True,port=12345)
   app.run(debug=True,port=12345, ssl_context=('/home/abhinav/c++/python/server.crt','/home/abhinav/c++/python/server.key'))

       

