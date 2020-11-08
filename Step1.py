import pymongo as pm #import MongoClient only
import pprint
from datetime import datetime as dt
import time
import pandas as pd


#3 lines of code to get the database ready
client = pm.MongoClient('bigdatadb.polito.it', ssl=True, authSource = 'carsharing', tlsAllowInvalidCertificates=True)
db = client['carsharing'] #Choose the DB to use
db.authenticate('ictts', 'Ictts16!')#, mechanism='MONGODB-CR') #authentication

#getting the collection

# Car2go
# c2g_perm_book = db['PermanentBookings']
# c2g_perm_park = db['PermanentParkings']
# c2g_act_book = db['ActiveBookings']
# c2g_act_park = db['ActiveParkings']

# # Enjoy

# enj_perm_book = db['enjoy_PermanentBookings']
# enj_perm_park = db['enjoy_PermanentParkings']
# enj_act_book = db['enjoy_ActiveBookings']
# enj_act_park = db['enjoy_ActiveParkings']

## Step 1 ##############################################################

## Task 1 ##############################################################

# print("In PermanentParkings there are", c2g_perm_park.count_documents({}), "documents")
# print("In PermanentBookings there are", c2g_perm_book.count_documents({}), "documents")
# print("In ActiveBookings there are", c2g_act_book.count_documents({}), "documents")
# print("In ActiveParkings there are", c2g_act_park.count_documents({}), "documents")

# print("In enjoy_PermanentParkings there are", enj_perm_park.count_documents({}), "documents")
# print("In enjoy_PermanentBookings there are", enj_perm_book.count_documents({}), "documents")
# print("In enjoy_ActiveBookings there are", enj_act_book.count_documents({}), "documents")
# print("In enjoy_ActiveParkings there are", enj_act_park.count_documents({}), "documents")

## Task 3 ##############################################################


# print(c2g_perm_book.distinct("city"))

# print(enj_perm_book.disticnt("city"))

## Task 4 & 5 ##########################################################

# #car2go
# first = c2g_perm_book.find({}, {"city":1, "init_time":1, "init_date":1, "_id":0}).sort("init_time", 1).limit(1)

# last = c2g_perm_book.find({}, {"city":1, "init_time":1, "init_date":1, "_id":0}).sort("init_time", -1).limit(1)

# list_res=list(first)+list(last)

# print("\nCar2Go:")
# print("The first measurement is in: ", list_res[0]["city"] ,"\nAt: ", list_res[0]["init_time"], "timestamp. \nEquivalent to", dt.fromtimestamp(list_res[0]["init_time"]), "at GMT+1. \nIn local timezone is: ", list_res[0]["init_date"]) 

# print("The last measurement is in: ", list_res[1]["city"] ,"\nAt: ", list_res[1]["init_time"], "timestamp. \nEquivalent to", dt.fromtimestamp(list_res[1]["init_time"]), "at GMT+1. \nIn local timezone is: ", list_res[1]["init_date"]) 


# first = enj_perm_book.find({}, {"city":1, "init_time":1, "init_date":1, "_id":0}).sort("init_time", 1).limit(1)

# last = enj_perm_book.find({}, {"city":1, "init_time":1, "init_date":1, "_id":0}).sort("init_time", -1).limit(1)

# list_res=list(first)+list(last)
# print("\nEnjoy:")

# print("The first measurement is in: ", list_res[0]["city"] ,"\nAt: ", list_res[0]["init_time"], "timestamp. \nEquivalent to", dt.fromtimestamp(list_res[0]["init_time"]), "at GMT+1. \nIn local timezone is: ", list_res[0]["init_date"]) 

# print("The last measurement is in: ", list_res[1]["city"] ,"\nAt: ", list_res[1]["init_time"], "timestamp. \nEquivalent to", dt.fromtimestamp(list_res[1]["init_time"]), "at GMT+1. \nIn local timezone is: ", list_res[1]["init_date"]) 

## Task 6 #############################################################

# # Wien and Vancouver (Car2go)

# c2g_perm_book = db['PermanentBookings']
# c2g_act_book = db['ActiveBookings']
# c2g_act_park = db['ActiveParkings']

# wien_cars = c2g_perm_book.distinct("plate", {"city": "Wien"})
# n_perm_wien_cars = len(wien_cars) # number of cars that were in wien during all the interval of time
# ab_wien_cars = c2g_act_book.distinct("plate", {"city": "Wien"})
# ap_wien_cars = c2g_act_park.distinct("plate", {"city": "Wien"})
# n_act_wien_cars = len(ab_wien_cars) + len(ap_wien_cars)
# print("By Wien circulated", n_perm_wien_cars,"car2go vehicles. \nOf which only", n_act_wien_cars, "were running as of the last measurement")

# Vancouver_cars = c2g_perm_book.distinct("plate", {"city": "Vancouver"})
# n_perm_Vancouver_cars = len(Vancouver_cars) # number of cars that were in Vancouver during all the interval of time
# ab_Vancouver_cars = c2g_act_book.distinct("plate", {"city": "Vancouver"})
# ap_Vancouver_cars = c2g_act_park.distinct("plate", {"city": "Vancouver"})
# n_act_Vancouver_cars = len(ab_Vancouver_cars) + len(ap_Vancouver_cars)
# print("By Vancouver circulated", n_perm_Vancouver_cars,"car2go vehicles. \nOf which only", n_act_Vancouver_cars, "were running as of the last measurement")


# # Torino (Enjoy)

# enj_perm_book = db['enjoy_PermanentBookings']

# Torino_cars = enj_perm_book.distinct("plate", {"city": "Torino"})
# n_perm_Torino_cars = len(Torino_cars) # number of cars that were in Torino during all the interval of time
# print("By Torino circulated", n_perm_Torino_cars,"Enjoy vehicles.\nWe can't do the active bookings+active parking measurement due to the files being empty")

## Task 7 ############################################################

# # Limit the date to November 2017
# startDate = dt(2017, 11, 1, 0)
# endDate = dt(2017, 12, 1, 0)
# startUnixTime = dt.timestamp(startDate)
# endUnixTime = dt.timestamp(endDate)

# # Wien and Vancouver (Car2Go)

# c2g_perm_book = db['PermanentBookings']
# pipeline = [ 
# 		{  
# 			'$match' : { 'init_time': {'$gte': startUnixTime, '$lte': endUnixTime}, '$or': [{'city': "Wien"},{'city': "Vancouver"}]}
# 		},
# 		{  
# 			'$project':{ '_id':0, 'city':1, 'plate':1 } 
# 		},
# 		{
# 			'$group': { '_id': "$city", 'tot_rentals': {'$sum':1} }
# 		}
# 	]
# res = c2g_perm_book.aggregate(pipeline)
# print("the number of rentals per city in November 2017 are:")
# for i in res:
# 	print("{0}:{1}".format(i["_id"], i["tot_rentals"]))

# # Torino (Enjoy)

# enj_perm_book = db['enjoy_PermanentBookings']
# pipeline = [ 
# 		{  
# 			'$match' : { 'init_time': {'$gte': startUnixTime, '$lte': endUnixTime}, 'city': "Torino"}
# 		},
# 		{  
# 			'$project':{ '_id':0, 'city':1, 'plate':1 } 
# 		},
# 		{
# 			'$group': { '_id': "$city", 'tot_rentals': {'$sum':1} }
# 		}
# 	]
# res = c2g_perm_book.aggregate(pipeline)
# for i in res:
# 	print("{0}:{1}".format(i["_id"], i["tot_rentals"]))

## Task 8 #######################################################

