import pymongo as pm #import MongoClient only
import pprint
from datetime import datetime as dt
import time
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd


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


## pipeline
def public_alt(city, mode):
    pipeline = [
        {
            '$match': { 'city': city,
                'init_time': {'$gte': startUnixTime, '$lte': endUnixTime},
                  mode:{"$gte":0}
                      
            }
        },
        { 
            '$project' : { '_id':0, 
                'duration_bin':{'$ceil':{'$divide':[{'$divide':['$'+mode,60]},5]}},
                'duration':{'$ceil': {'$divide': [{'$subtract': ['$final_time', '$init_time']}, 60]} },
                'moved': {'$ne': [
                        {'$arrayElemAt': ['$origin_destination.coordinates', 0]},
                        {'$arrayElemAt': ['$origin_destination.coordinates', 1]}]
                    }
                }
        },
        # Filter Block : it has to be moved to be booked and it has to last more than 3 min and less then 3 hours
        {
            '$match': {'moved': True,
                       'duration': {'$gte': 3, '$lte':3*60},
                       'duration_bin':{'$lte':35}
                      }
        },
        {
            '$group': {'_id': '$duration_bin',
                      'count':{'$sum':1}}
        },
        {
            '$sort':{'_id':1}
        }
        ]
    return pipeline

## init
startDate = dt(2017, 11, 1, 0, 0)
endDate = dt(2017, 12, 1, 0, 0)
startUnixTime = dt.timestamp(startDate)
endUnixTime = dt.timestamp(endDate)

_city='Torino'

modes = ["walking.duration", "public_transport.duration", "driving.duration"]

#dataframe for teh istograms
walk_df = pd.DataFrame(list(enj_perm_book.aggregate(public_alt(_city, modes[0]))))
bus_df = pd.DataFrame(list(enj_perm_book.aggregate(public_alt(_city, modes[1]))))
drive_df = pd.DataFrame(list(enj_perm_book.aggregate(public_alt(_city, modes[2]))))


#plots
fig, axs = plt.subplots(1, 3, figsize=(18, 6))
fig.subplots_adjust(top=0.825,
    bottom=0.245,
    left=0.08,
    right=0.95,
    hspace=0.4,
    wspace=0.35)

axs[0].grid(True, which='both')
axs[0].set_xlabel('Walking Duration [min]')
axs[0].set_ylabel('Number ofrentals')
axs[0].bar(walk_df['_id'].values*5-2.5, walk_df['count'].values, width=5)

axs[1].grid(True, which='both')
axs[1].set_xlabel('Driving Duration [min]')
axs[1].set_ylabel('Number ofrentals')
axs[1].bar(drive_df['_id'].values*5-2.5, drive_df['count'].values, width=5)

axs[2].grid(True, which='both')
axs[2].set_xlabel('Public Transport Duration [min]')
axs[2].set_ylabel('Number ofrentals')
axs[2].bar(bus_df['_id'].values*5-2.5, bus_df['count'].values, width=5)

plt.show()
