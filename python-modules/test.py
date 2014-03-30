from pprint import pprint
import argparse
import GTFSparser
import dict_converter

if __name__ == "__main__":

	# parser = argparse.ArgumentParser()
	# # parser.add_argument("GTFS", help="enter path to GTFS folder")
	# # parser.add_argument("day", help="enter day in lowercase")
	# parser.add_argument("converter", help="choose a converter to apply") #can i provide a list of options in help? and make it so it doesn't accept bad options?
	# args = parser.parse_args()

	# GTFSparser.calendarIterator('../GTFS-welly/calendar.txt', 'monday', GTFSparser.tester)
	#print GTFSparser.calendarIterator('../GTFS-welly/calendar.txt', 'monday')
	# print GTFSparser.shapeDictBuilder('../GTFS-welly/shapes.txt')
	# print GTFSparser.tripsIterator('../GTFS-welly/trips.txt', '1')
	# print GTFSparser.stoptimesDictBuilder('../GTFS-welly/stop_times.txt')

	# latlons = [(-41.23092295,174.8177883),(-41.23095897,174.8177893),(-41.23100398,174.8177906),(-41.23103118,174.8177794)]
	# latlons = [(0,1),(0,2),(1,2),(2,1)]
	# times = ('06:58:00', '06:59:16')

	# pprint(GTFSparser.positionInterpolator(latlons, times))

	# ##testing dict_converter toCSV
	# test = {'day': {'trip': [{'time': ('lat', 'lon')}, 'shortname', 'longname', 'type']}}
	# conv = getattr(dict_converter, args.converter)
	# conv(test,'test.csv')

	# sd = GTFSparser.shapeDictBuilder('../GTFS-welly/shapes.txt')
	# latlons = []
	# times = ('06:58:00', '06:59:16')

	# for shp_shpdst, latlon in sd['WBALNK0 005'].iteritems():
	# 	if shp_shpdst > '0' and shp_shpdst < '39': #basically, looking for bits of the line segment between the two times in the 'times' tuple
	# 		latlons.append(latlon)
	# for new_time, ll in GTFSparser.positionInterpolator(latlons, times).iteritems(): #running positionInterpolator to get interpolated lat/lons for all the seconds in between the two times
	# 	print new_time, ll
	# # print latlons

	#imports
	import json
	import io
	import sqlite3
	from shapely.geometry import LineString
	from datetime import datetime
	from datetime import timedelta

	#connect to db
	con = sqlite3.connect('../GTFSSQL_Wellington_20140308_203334.db')

	#test dict
	#trip_dict looks like: {"trip":{"time":[["lat","lon"],"short_name","long_name","type"]}}
	trip_dict = {}

	i = 1
	while i<7678:
		trip_dict[i] = {"00:00:00":[["lat","lon"],"short_name,long_name,type"],"23:00:00":[["lat","lon"],"short_name,long_name,type"]}
		print i
		i+=1

	print "test trip_dict done"

	#drop test table in case it exists already
	con.cursor().execute('DROP TABLE test;')
	#add test table
	# contains trip_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, time, lat, lon, short_name, long_name, type
  	con.cursor().execute('CREATE TABLE test(trip_id INTEGER REFERENCES trips(trip_id), monday INTEGER, tuesday INTEGER, wednesday INTEGER, thursday INTEGER, friday INTEGER, saturday INTEGER, sunday INTEGER, time TEXT, lat FLOAT, lon FLOAT, route_short_name TEXT REFERENCES routes(route_short_name), route_long_name TEXT REFERENCES routes(route_long_name), route_type_desc TEXT REFERENCES routes(route_type_desc))')
  	# con.cursor().execute('CREATE TABLE test(t TEXT, i INTEGER)')
  	# #set up single cursor for db begin...commit
  	# cur = con.cursor()

 #  	for trip_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday in con.cursor().execute('SELECT trips.trip_id, calendar.monday, calendar.tuesday, calendar.wednesday, calendar.thursday, calendar.friday, calendar.saturday, calendar.sunday FROM calendar JOIN trips ON calendar.service_id = trips.service_id;'):
 #  		print trip_id
 #  		for time, details in trip_dict[trip_id].iteritems():
 #  			print trip_id, time, details

 #  			lat = details[0][0]
 #  			lon = details[0][1]
 #  			short_name = details[1].split(",")[0].strip('()').strip() #double strip to also remove whitespace
 #  			long_name = details[1].split(",")[1].strip('()').strip()
 #  			route_type_desc = details[1].split(",")[2].strip('()').strip()

	#     	cur.execute('INSERT INTO test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (trip_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, time, lat, lon, short_name, long_name, route_type_desc))
	#     	# con.commit()
	# 	con.commit()
	# con.close()

	rows = []
	for trip_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday in con.cursor().execute('SELECT trips.trip_id, calendar.monday, calendar.tuesday, calendar.wednesday, calendar.thursday, calendar.friday, calendar.saturday, calendar.sunday FROM calendar JOIN trips ON calendar.service_id = trips.service_id;'):
 		# rows.append([trip_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday])
 		# trip_dict[trip_id] = {"00:00:00":[["lat","lon"],"short_name,long_name,type",monday, tuesday, wednesday, thursday, friday, saturday, sunday],"23:00:00":[["lat","lon"],"short_name,long_name,type",monday, tuesday, wednesday, thursday, friday, saturday, sunday]}
 		for time, details in trip_dict[trip_id].iteritems():
 			print trip_id, time, details

			lat = details[0][0]
			lon = details[0][1]
			short_name = details[1].split(",")[0].strip('()').strip() #double strip to also remove whitespace
			long_name = details[1].split(",")[1].strip('()').strip()
			route_type_desc = details[1].split(",")[2].strip('()').strip()

 			rows.append([trip_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, time, lat, lon, short_name, long_name, route_type_desc])

 	print "row appending done"

	con.cursor().executemany('INSERT INTO test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', rows)
	con.commit()
	con.close()
 	


	
	# for time, details in trip_dict[row[0]].iteritems():
	# 	print row[0], time, details

	# 	lat = details[0][0]
	# 	lon = details[0][1]
	# 	short_name = details[1].split(",")[0].strip('()').strip() #double strip to also remove whitespace
	# 	long_name = details[1].split(",")[1].strip('()').strip()
	# 	route_type_desc = details[1].split(",")[2].strip('()').strip()

		# con.cursor().execute('INSERT INTO test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], time, lat, lon, short_name, long_name, route_type_desc))


	# curOuter = db.cursor()
	# rows=[]
	# for row in curOuter.execute('SELECT * FROM myConnections'):    
	#     id  = row[0]    
	#     scList = retrieve_shared_connections(id)  
	#     for sc in scList:

	#         rows.append((id,sc))
	# curOuter.executemany('''INSERT INTO sharedConnections(IdConnectedToMe, IdShared) VALUES (?,?)''', rows)  
	# db.commit()

	# j = 1
	# s = "test"
	# while j<7678:
	# 	con.cursor().execute('INSERT INTO test VALUES (?, ?)', (s, j))
	# 	print j
	# 	j+=1
	
	# con.commit()
	# con.close()

