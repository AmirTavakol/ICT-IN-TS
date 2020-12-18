#!/usr/bin/env python
# coding: utf-8

# In[56]:


import pymongo as pm #import MongoClient only
import pprint
import datetime as dt
import time
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

#3 lines of code to get the database ready
client = pm.MongoClient('bigdatadb.polito.it', ssl=True, authSource = 'carsharing', tlsAllowInvalidCertificates=True)
db = client['carsharing'] #Choose the DB to use
db.authenticate('ictts', 'Ictts16!')#, mechanism='MONGODB-CR') #authentication

# getting the collection

# Car2go
c2g_perm_book = db['PermanentBookings']
# Enjoy

enj_perm_book = db['enjoy_PermanentBookings']

def rentals (city, start, end):
    pipeline = [
        {
            '$match': { 'city': city,
                'init_time': {'$gte': start, '$lte': end}
                }
        },
        { 
            '$project' : { '_id':0, 
                'city': 1,
                'hourOfDay': {'$floor':{'$divide':['$init_time', 3600]}},
                'duration':{'$ceil': {'$divide': [{'$subtract': ['$final_time', '$init_time']}, 60]} },
                'moved': {'$ne': [
                        {'$arrayElemAt': ['$origin_destination.coordinates', 0]},
                        {'$arrayElemAt': ['$origin_destination.coordinates', 1]}]
                    }
                }
        },
        # Filter Block : it has to be moved to be booked and it has to last more than 3 min and less then 3 hours
        {
            '$match': {"$and": [{'moved': True}, {'duration': {'$gte': 3, '$lte': 180}}]}
        },
        {
            '$group': { '_id': '$hourOfDay', 
                'count':{'$sum':1}
                }
        },
        {
            '$sort': {'_id': 1}
        }
        ]
    return pipeline

# the projection of the time is done in unix time since pandas 
# doesn't work well with dates as indexes (its easier)

def plot_rentals(city, base_date):
    
    #add timezonez information
    timezones = {'Torino': +1, 'Wien': +1, 'Vancouver':-8}
    tz = dt.timezone(dt.timedelta(hours=timezones[city]))
    
    startDate = base_date.replace(tzinfo=tz)
    monthWnd = dt.timedelta(days = 30) 
    
    endDate=startDate+monthWnd
    startUnixTime = dt.datetime.timestamp(startDate)
    endUnixTime = dt.datetime.timestamp(endDate)
    
    #get the rentals
    books_pipe = rentals(city, startUnixTime, endUnixTime)
    
    if city == 'Torino':
        daily_bookings=enj_perm_book.aggregate(books_pipe)
    else:
        daily_bookings=c2g_perm_book.aggregate(books_pipe)
        
    book_df = pd.DataFrame (list(daily_bookings)) # pandas dataframes are easier to use for regressions
    book_df['date'] = pd.to_datetime ( book_df['_id'] , unit = 'h') # from unix to datetime
    
    book_df.drop('_id', axis=1, inplace=True)
    book_df.rename(columns={'count':'rentals'}, inplace=True) # for clarity
    
    book_df.to_csv ( "rentals_" + city + ".csv" ) # save the dataframe for later computations with arima (shit happens)

    plt.figure (figsize =(15 , 5))
    plt.grid ()
    plt.plot( book_df['date'] , book_df ['rentals'] )
    plt.title ( city + ': number of rentals per hour')
    
    # x axis dates formatting
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=3)) # interval of days in ticks for x axis
    plt.gcf().autofmt_xdate()
    
    plt.xlabel ('hour')
    plt.ylabel ('number of rentals')
    plt.savefig(city + 'rentals.png')
    plt.show()

initial_date = dt.datetime(2017,10,1,0,0,0)


city='Torino'
plot_rentals(city, initial_date)

city='Wien'
plot_rentals(city, initial_date)

city='Vancouver'
plot_rentals(city, initial_date)


# In[ ]:




