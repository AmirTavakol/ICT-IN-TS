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

def daily_booking_stats(city):    
    pipeline = [
        {
            '$match': { 'city': city,
                'init_time': {'$gte': startUnixTime, '$lte': endUnixTime}
                }
        },
        { 
            '$project' : { '_id':0, 
                'city': 1,
                '_day': {'$dateFromParts': {'year':2017, 'month': 11, 'day': {'$dayOfMonth': '$init_date'}}},
                'duration':{'$ceil': {'$divide': [{'$subtract': ['$final_time', '$init_time']}, 60]} },
                'moved': {'$ne': [
                        {'$arrayElemAt': ['$origin_destination.coordinates', 0]},
                        {'$arrayElemAt': ['$origin_destination.coordinates', 1]}]
                    }
                }
        },
        # Filter Block : it has to be moved to be booked and it has to last more than 3 min and less then 3 hours
        {
            '$match': {"$and": [{'moved': True}, {'duration': {'$gte': 3*60, '$lte': 3*60*60}}]}
        },
        # ####
        {
            '$group': { '_id': '$_day', # group by day oof the month
         				'daily_duration': {'$push': '$duration'}, # get a list with all the duration of the trips happends in each day
            			'avg_duration': {'$avg': '$duration'}, # get the average of the duration of the trips in a day
            			'std_duration': {'$stdDevPop': '$duration'} # get the std of the duration of the trips in a day
                		}
        },
        {
            '$sort': {'_id': 1}
        }
        ]
    return pipeline

def daily_parkings_stats(city):    
    pipeline = [
        {
            '$match': { 'city': city,
                'init_time': {'$gte': startUnixTime, '$lte': endUnixTime}
                }
        },
        { 
            '$project' : { '_id':0, 
                'city': 1,
                '_day': {'$dateFromParts': {'year':2017, 'month': 11, 'day': {'$dayOfMonth': '$init_date'}}},
                'duration':{'$ceil': {'$divide': [{'$subtract': ['$final_time', '$init_time']}, 60]} },
                'moved': {'$ne': [
                        {'$arrayElemAt': ['$origin_destination.coordinates', 0]},
                        {'$arrayElemAt': ['$origin_destination.coordinates', 1]}]
                    }
                }
        },
        # Filter Block : it has to be moved to be booked and it has to last more than 3 min and less then 3 hours
        {
            '$match': {"$and": [{'moved': False}, {'duration': {'$gte': 3*60}}]}
        },
        # ####
        {
            '$group': { '_id': '$_day', # group by day oof the month
         				'daily_duration': {'$push': '$duration'}, # get a list with all the duration of the trips happends in each day
            			'avg_duration': {'$avg': '$duration'}, # get the average of the duration of the trips in a day
            			'std_duration': {'$stdDevPop': '$duration'} # get the std of the duration of the trips in a day
                		}
        },
        {
            '$sort': {'_id': 1}
        }
        ]
    return pipeline

################

def plotter(city_dict):
    
    date_ticks = np.arange("2017-11-01", "2017-12-01", dtype ='datetime64')
    #plt.xticks(rotation=45)

    for city in city_dict: ## Loop in cities
        fig, axs = plt.subplots(1, 2, figsize=(15, 6))
        fig.subplots_adjust(top=0.825,
            bottom=0.245,
            left=0.08,
            right=0.95,
            hspace=2,
            wspace=0.35)
        axs = axs.ravel()
        for j, x in enumerate(city_dict[city]): ## loop between booking and parkings
            dates, stats_tuple = zip(*sorted(city_dict[city][x].items()))

            perc90_tuple=()
            med_tuple = ()
            avg_tuple = ()
            std_tuple = ()
            for s in stats_tuple:
                perc90_tuple=perc90_tuple+(s['90percentile'],)
                med_tuple=med_tuple+(s['median'],)
                avg_tuple=avg_tuple+(s['average'],)
                std_tuple=std_tuple+(s['st_dev'],)

            axs[j].plot(dates, perc90_tuple, label='90 percentile')
            axs[j].plot(dates, med_tuple, label='median')
            axs[j].plot(dates, avg_tuple, label='average')
            axs[j].plot(dates, std_tuple, label='standard deviation')
            axs[j].set_xticks(date_ticks)
            axs[j].set_xticklabels(labels=date_ticks, rotation=45, ha='right')
            axs[j].set_title(city + ' ' + x, fontsize=16)
            axs[j].set_xlabel('Dates', fontsize=12)
            axs[j].set_ylabel(' Number of cars ', fontsize=12)
            axs[j].grid(True, which='both')
            axs[j].legend(loc='upper left')
        
            fig.suptitle('Stats per booking and parkings in '+ city)
        plt.show()    
        

################################################

city_dict = {'Wien': {'bookings': {}, 'parkings': {}},
            'Vancouver': {'bookings': {}, 'parkings': {}},
            'Torino': {'bookings': {}, 'parkings': {}}}

## Wien ########################################################

city='Wien'

books_pipe = daily_booking_stats(city)
parks_pipe = daily_parkings_stats(city)

# bookings
daily_bookings=c2g_perm_book.aggregate(books_pipe)
for item in daily_bookings:
    stats = {
        '90percentile': np.percentile(np.array(item['daily_duration']),90),
        'median': np.percentile(np.array(item['daily_duration']),50),
        'average': item['avg_duration'],
        'st_dev': item['std_duration']
    }
    city_dict[city]['bookings'][item['_id']]=stats


# parkings
daily_parkings=c2g_perm_park.aggregate(parks_pipe)
for item in daily_parkings:
    stats = {
        '90percentile': np.percentile(np.array(item['daily_duration']),90),
        'median': np.percentile(np.array(item['daily_duration']),50),
        'average': item['avg_duration'],
        'st_dev': item['std_duration']
    }
    city_dict[city]['parkings'][item['_id']]=stats

## Vancouver ###################################################

city='Vancouver'

books_pipe = daily_booking_stats(city)
parks_pipe = daily_parkings_stats(city)

# bookings
daily_bookings=c2g_perm_book.aggregate(books_pipe)
for item in daily_bookings:
    stats = {
        '90percentile': np.percentile(np.array(item['daily_duration']),90),
        'median': np.percentile(np.array(item['daily_duration']),50),
        'average': item['avg_duration'],
        'st_dev': item['std_duration']
    }
    city_dict[city]['bookings'][item['_id']]=stats


# parkings
daily_parkings=c2g_perm_park.aggregate(parks_pipe)
for item in daily_parkings:
    stats = {
        '90percentile': np.percentile(np.array(item['daily_duration']),90),
        'median': np.percentile(np.array(item['daily_duration']),50),
        'average': item['avg_duration'],
        'st_dev': item['std_duration']
    }
    city_dict[city]['parkings'][item['_id']]=stats

## Vancouver ###################################################

city='Torino'

books_pipe = daily_booking_stats(city)
parks_pipe = daily_parkings_stats(city)

# bookings
daily_bookings=enj_perm_book.aggregate(books_pipe)
for item in daily_bookings:
    stats = {
        '90percentile': np.percentile(np.array(item['daily_duration']),90),
        'median': np.percentile(np.array(item['daily_duration']),50),
        'average': item['avg_duration'],
        'st_dev': item['std_duration']
    }
    city_dict[city]['bookings'][item['_id']]=stats


# parkings
daily_parkings=enj_perm_park.aggregate(parks_pipe)
for item in daily_parkings:
    stats = {
        '90percentile': np.percentile(np.array(item['daily_duration']),90),
        'median': np.percentile(np.array(item['daily_duration']),50),
        'average': item['avg_duration'],
        'st_dev': item['std_duration']
    }
    city_dict[city]['parkings'][item['_id']]=stats
    

plotter(city_dict)