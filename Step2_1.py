import pymongo as pm #import MongoClient only
import pprint
from datetime import datetime as dt
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

#3 lines of code to get the database ready
client = pm.MongoClient('bigdatadb.polito.it', ssl=True, authSource = 'carsharing', tlsAllowInvalidCertificates=True)
db = client['carsharing'] #Choose the DB to use
db.authenticate('ictts', 'Ictts16!')#, mechanism='MONGODB-CR') #authentication


## Task 1 ###########################################################


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

startDate = dt(2017, 11, 1, 0)
endDate = dt(2017, 12, 1, 0)
startUnixTime = dt.timestamp(startDate)
endUnixTime = dt.timestamp(endDate)



def duration_pipeline(city):
	pipeline = [ 
			{  
				'$match' : {
					'city': city,
					'init_time': {'$gte': startUnixTime, '$lte': endUnixTime}
				}
			},
			{  
				'$project':{ 
				'_id':0, 
				'city':1, 
				'duration':{
					'$ceil': {
						'$divide': [{'$subtract': ['$final_time', '$init_time']}, 60]}
					}
				} 
			},
			{
				'$group': {
					'_id': '$duration'}
			}
		]

	return pipeline



def plot_bp(city_dict):

    fig, axs = plt.subplots(1, 3, figsize=(18, 6))
    fig.subplots_adjust(top=0.825,
        bottom=0.2,
        left=0.08,
        right=0.95,
        hspace=0.20,
        wspace=0.35)
    axs = axs.ravel()

    i=0
    for x in city_dict:

        axs[i].hist(city_dict[x]['bookings'], 
            bins=1000,
            density=True,
            histtype='step',
            cumulative=True,
            label='Bookings',
            color='green')
        axs[i].set_xscale('log')

        axs[i].hist(city_dict[x]['parkings'],
            bins=1000,
            density=True,
            histtype='step',
            cumulative=True,
            label='Parkings',
            color='orange')
        
        axs[i].set_xscale('log')
        axs[i].legend(loc='upper left')
        axs[i].set_title(x, fontsize=14)
        axs[i].set_xlabel('Duration [min]', fontsize=12)
        axs[i].set_ylabel('CDF', fontsize=12)
        axs[i].grid(True, which='both')
        i=i+1
    
    fig.suptitle('CDF of Bookings and Parkings per city')
    fig.show()

city_dict = {'Wien': {'bookings': [], 'parkings': []},
			'Vancouver': {'bookings': [], 'parkings': []},
			'Torino': {'bookings': [], 'parkings': []}}

## Wien ####################################################################

city='Wien'

pipeline = duration_pipeline(city)

# bookings
bookings_duration=c2g_perm_book.aggregate(pipeline)
city_dict[city]['bookings'] =[d['_id'] for d in list(bookings_duration)]


# parkings
parking_duration=c2g_perm_park.aggregate(pipeline)
city_dict[city]['parkings']=[d['_id'] for d in list(parking_duration)]


## Vancouver ####################################################################

city='Vancouver'

pipeline = duration_pipeline(city)

# bookings
bookings_duration=c2g_perm_book.aggregate(pipeline)
city_dict[city]['bookings'] =[d['_id'] for d in list(bookings_duration)]

# parkings
parking_duration=c2g_perm_park.aggregate(pipeline)
city_dict[city]['parkings']=[d['_id'] for d in list(parking_duration)]


## Torino ####################################################################

city='Torino'

pipeline = duration_pipeline(city)

# bookings
bookings_duration=enj_perm_book.aggregate(pipeline)
city_dict[city]['bookings'] =[d['_id'] for d in list(bookings_duration)]

# parkings
parking_duration=enj_perm_park.aggregate(pipeline)
city_dict[city]['parkings']=[d['_id'] for d in list(parking_duration)]


plot_bp(city_dict)


########################################################################
## Change over time time ###############################################
########################################################################

def duration_by_weekDay(city, day):
    pipeline = [ 
            {  
                '$match' : {'city': city, 'init_time': {'$gte': startUnixTime, '$lte': endUnixTime}}
            },
            {  
                '$project':{ 
                '_id':0, 
                'city':1, 
                'duration':{'$ceil': {'$divide': [{'$subtract': ['$final_time', '$init_time']}, 60]}},
                'WeekDay': { '$dayOfWeek': "$init_date" },
                } 
            },
            {
                '$match': {'WeekDay': day}
            },
            {
                '$group': { '_id': '$duration' }
            },
            {
                '$sort':{'_id':1}
            }
        ]
    
    return pipeline

## We just do Torino for simplicity but can be plotted easity for the others too

city='Torino'

plt.figure(figsize=(8,4))

plt.subplot(121)

#bookings
for day in range(1, 8):
    bookings_dict={}
    bookings_duration=enj_perm_book.aggregate(duration_by_weekDay(city, day))
    bookings=[d['_id'] for d in list(bookings_duration)]
    
    plt.hist(bookings, 
        bins=1000,
        density=True,
        histtype='step',
        cumulative=True)
    plt.xscale('log')

plt.legend(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], loc='upper left')
plt.title('Bookings', fontsize=13)
plt.xlabel('Duration [min]', fontsize=12)
plt.ylabel('CDF', fontsize=12)
plt.grid(True, which='both')


plt.subplot(122)

#parkings
for day in range(1, 8):
    bookings_dict={}
    parkings_duration=enj_perm_park.aggregate(duration_by_weekDay(city, day))
    parkings=[d['_id'] for d in list(parkings_duration)]
    
    plt.hist(parkings, 
        bins=1000,
        density=True,
        histtype='step',
        cumulative=True)
    plt.xscale('log')

plt.legend(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], loc='upper left')
plt.title('Parkings', fontsize=13)
plt.xlabel('Duration [min]', fontsize=12)
plt.ylabel('CDF', fontsize=12)
plt.grid(True, which='both')

plt.suptitle('CDF of Bookings and Parkings per day of the week in '+ city)


plt.show()