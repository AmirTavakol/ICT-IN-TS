import pymongo as pm #import MongoClient only
import pprint
from datetime import datetime as dt
import time
import numpy as np
import matplotlib.pyplot as plt


#3 lines of code to get the database ready
client = pm.MongoClient('bigdatadb.polito.it', ssl=True, authSource = 'carsharing', tlsAllowInvalidCertificates=True)
db = client['carsharing'] #Choose the DB to use
db.authenticate('ictts', 'Ictts16!')#, mechanism='MONGODB-CR') #authentication


## Task 2 ###########################################################


# getting the collection

# Car2go
c2g_perm_book = db['PermanentBookings']
c2g_perm_park = db['PermanentParkings']
# c2g_act_book = db['ActiveBookings']
# c2g_act_park = db['ActiveParkings']

# Enjoy

enj_perm_book = db['enjoy_PermanentBookings']
enj_perm_park = db['enjoy_PermanentParkings']
# enj_act_book = db['enjoy_ActiveBookings']
# enj_act_park = db['enjoy_ActiveParkings']

startDate = dt(2017, 11, 1, 0, 0)
endDate = dt(2017, 12, 1, 0, 0)
startUnixTime = dt.timestamp(startDate)
endUnixTime = dt.timestamp(endDate)

def carsPerHour(city):    
    pipeline = [
        {
            '$match': { 'city': city,
                'init_time': {'$gte': startUnixTime, '$lte': endUnixTime}
                }
        },
        { 
            '$project' : { '_id':0, 
                'city': 1,
                'hourOfDay': {'$dateFromParts': {'year':2017, 'month': 11, 'day': {'$dayOfMonth': '$init_date'}, 'hour': {'$hour': '$init_date' }}},
                'duration':{'$ceil': {'$divide': [{'$subtract': ['$final_time', '$init_time']}, 60]} },
                'moved': {'$ne': [
                        {'$arrayElemAt': ['$origin_destination.coordinates', 0]},
                        {'$arrayElemAt': ['$origin_destination.coordinates', 1]}]
                    }
                }
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

def plot_n_dailyBP(city_dict):
    fig, axs = plt.subplots(1, 3, figsize=(18, 6))
    fig.subplots_adjust(top=0.825,
        bottom=0.2,
        left=0.08,
        right=0.95,
        hspace=0.20,
        wspace=0.35)
    axs = axs.ravel()
    date_ticks = np.arange("2017-11-01", "2017-12-01", dtype ='datetime64')
    
    i=0
    for x in city_dict:
        
        booking_date, n_bookings = zip(*sorted(city_dict[x]['bookings'].items()))
        parking_date, n_parkings = zip(*sorted(city_dict[x]['parkings'].items()))

        
        axs[i].plot(booking_date, n_bookings, label='Bookings')

        axs[i].plot (parking_date, n_parkings, label='Parkings')
        
        axs[i].legend(loc='upper left')
        axs[i].set_title(x, fontsize=14)
        axs[i].set_xlabel('Dates', fontsize=12)
        axs[i].set_ylabel(' Number of booked / parked cars ', fontsize=12)
        axs[i].grid(True, which='both')
        i=i+1
    
    fig.suptitle('Number of booked / parked cars per hour')
    fig.show()
    
    

city_dict = {'Wien': {'bookings': {}, 'parkings': {}},
            'Vancouver': {'bookings': {}, 'parkings': {}},
            'Torino': {'bookings': {}, 'parkings': {}}}

## Wien ########################################################
city='Wien'

pipeline = carsPerHour(city)

# bookings
daily_bookings=c2g_perm_book.aggregate(pipeline)
for item in daily_bookings:
    city_dict[city]['bookings'][item['_id']]=item ['count']


# parkings
daily_parkings=c2g_perm_park.aggregate(pipeline)
for item in daily_parkings:
    city_dict[city]['parkings'][item['_id']]=item ['count']
 

## Vancouver ####################################################
city='Vancouver'

pipeline = carsPerHour(city)

# bookings
daily_bookings=c2g_perm_book.aggregate(pipeline)
for item in daily_bookings:
    city_dict[city]['bookings'][item['_id']]=item ['count']


# parkings
daily_parkings=c2g_perm_park.aggregate(pipeline)
for item in daily_parkings:
    city_dict[city]['parkings'][item['_id']]=item ['count']

    
## Torino ######################################################
city='Torino'

pipeline = carsPerHour(city)

# bookings
daily_bookings=enj_perm_book.aggregate(pipeline)
for item in daily_bookings:
    city_dict[city]['bookings'][item['_id']]=item ['count']


# parkings
daily_parkings=enj_perm_park.aggregate(pipeline)
for item in daily_parkings:
    city_dict[city]['parkings'][item['_id']]=item ['count']

    
## Final Plot    
    
plot_n_dailyBP(city_dict)
