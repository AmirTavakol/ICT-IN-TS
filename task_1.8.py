#Task 8
import pymongo as pm #import MongoClient only
import pprint
from datetime import datetime as dt
import time
import seaborn as sns

if __name__ == "__main__":

	#3 lines of code to get the database ready
	client = pm.MongoClient("bigdatadb.polito.it", ssl=True, authSource = "carsharing", tlsAllowInvalidCertificates=True)
	db = client["carsharing"] #Choose the DB to use
	db.authenticate("ictts", "Ictts16!")#, mechanism="MONGODB-CR") #authentication

	c2g_perm_book = db["PermanentBookings"]
	enj_perm_book = db["enjoy_PermanentBookings"]

	modes = ["walking.duration", "public_transport.duration", "driving.duration"]		#find in the keys of the db with robo3T

#Vancouver (car2go)
	all_alternative_Vancouver = c2g_perm_book.count_documents({"$and": [
	            {"city": "Vancouver"}, 
	            # Count documents that match the city and have values different to -1 in the three alternative modes
	            { "$and": [ 
	                    {"walking.duration":{"$ne":-1}} , 
	                    {"public_transport.duration":{"$ne":-1}}, 
	                    {"driving.duration":{"$ne":-1}}
	                    ]
	            }
	        ]
	    })           #the combination find and count generate a DeprecationWarning
	print("Bookings alternative transportation all modes recorder in Vancouver are: ", all_alternative_Vancouver)

	one_alternative_Vancouver = c2g_perm_book.count_documents({"$and": [
	            {"city": "Vancouver"}, 
	            # Count documents that match the city and have values different to -1 in the three alternative modes
	            { "$or": [ 
	                    {"walking.duration":{"$ne":-1}} , 
	                    {"public_transport.duration":{"$ne":-1}}, 
	                    {"driving.duration":{"$ne":-1}}
	                    ]
	            }
	        ]
	    })
	print("Bookings alternative transportation at least one of modes recorder in Vancouver are: ", one_alternative_Vancouver)
	print("of which: " )
	for mode in modes:
	    alt_mod = c2g_perm_book.count_documents({"city": "Vancouver", mode: {"$ne": -1}})
	    print("     " + mode.split(".")[0] + " mode: " + str(alt_mod))
	





#Wien (car2go)
	all_alternative_Wien = c2g_perm_book.count_documents({"$and": [
	            {"city": "Wien"}, 
	            # Count documents that match the city and have values different to -1 in the three alternative modes
	            { "$and": [ 
	                    {"walking.duration":{"$ne":-1}} , 
	                    {"public_transport.duration":{"$ne":-1}}, 
	                    {"driving.duration":{"$ne":-1}}
	                    ]
	            }
	        ]
	    })           
	print("Bookings alternative transportation all modes recorder in Wien are: ", all_alternative_Wien)

	one_alternative_Wien = c2g_perm_book.count_documents({"$and": [
	            {"city": "Wien"}, 
	            # Count documents that match the city and have values different to -1 in the three alternative modes
	            { "$or": [ 
	                    {"walking.duration":{"$ne":-1}} , 
	                    {"public_transport.duration":{"$ne":-1}}, 
	                    {"driving.duration":{"$ne":-1}}
	                    ]
	            }
	        ]
	    })
	print("Bookings alternative transportation at least one of modes recorder in Wien are: ", one_alternative_Wien)
	print("of which: " )
	for mode in modes:
	    alt_mod = c2g_perm_book.count_documents({"city": "Wien", mode: {"$ne": -1}})
	    print("     " + mode.split(".")[0] + " mode: " + str(alt_mod))
	 




 # Torino (enjoy)
 #Vancouver (car2go)
	all_alternative_Torino = enj_perm_book.count_documents({"$and": [
	            {"city": "Torino"}, 
	            # Count documents that match the city and have values different to -1 in the three alternative modes
	            { "$and": [ 
	                    {"walking.duration":{"$ne":-1}} , 
	                    {"public_transport.duration":{"$ne":-1}}, 
	                    {"driving.duration":{"$ne":-1}}
	                    ]
	            }
	        ]
	    })           
	print("Bookings alternative transportation all modes recorder in Torino are: ", all_alternative_Torino)

	one_alternative_Torino = enj_perm_book.count_documents({"$and": [
	            {"city": "Torino"}, 
	            # Count documents that match the city and have values different to -1 in the three alternative modes
	            { "$or": [ 
	                    {"walking.duration":{"$ne":-1}} , 
	                    {"public_transport.duration":{"$ne":-1}}, 
	                    {"driving.duration":{"$ne":-1}}
	                    ]
	            }
	        ]
	    })
	print("Bookings alternative transportation at least one of modes recorder in Torino are: ", one_alternative_Torino)
	print("of which: " )
	for mode in modes:
	    alt_mod = enj_perm_book.count_documents({"city": "Torino", mode: {"$ne": -1}})
	    print("     " + mode.split(".")[0] + " mode: " + str(alt_mod))
	 