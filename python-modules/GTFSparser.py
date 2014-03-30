#this module includes functions to parse a set of GTFS files
#they can be used to answer the question:
#for each trip that runs on a particular day, where is it (lat / lon) at each second between + including it's start and finish?

#requires Python 2.7, due to the use of the OrderedDict from collections

##imports
import argparse
import sqlite3 as dbapi
import time
import csv
#modules for interpolating time / position
#Shapely is an external lib, download and install it (will also need GEOS)
from shapely.geometry import LineString
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict

##my funcs
#not completely modular for now, will write later if i feel like it
# def calendarIterator(calendar, day, func=None, *args):
def calendarIterator(calendar, day):
	'''
	iterates over a calendar.txt file, returns a list of service_id's that run on a given day
	if func=someFunc, then call someFunc for each iteration that retrieves a match
	'''
	#python has no switch, WAT, it's been SO LONG since i've pythoned
	def switch(x):
		return {
	        'monday': 1,
	        'tuesday': 2,
	        'wednesday': 3,
	        'thursday': 4,
	        'friday': 5,
	        'saturday': 6,
	        'sunday': 7,
	    }[x]

	ids = []
	with open(calendar) as cal:
	    next(cal) # skip header
	    for line in cal:
	    	service = line.split(",")
	    	if service[switch(day)] == '1':
	    		ids.append(service[0])

	# if func != None:
	# 	pass
	# else:
	return ids

def tripsIterator(trips, service_ids):
	'''
	iterates over a trips.txt file, returns a dict of trip_id keys with route_id and shape_id values that correlate to given service_ids
	'''
	trips_dict = {}
	with open(trips) as trps:
		next(trps) # skip header
		for line in trps:
			trip = line.strip().split(",") # strip some /r/n off line end
			#iterate over service_ids
			if trip[1] in service_ids:
				trips_dict[trip[2]] = [trip[0], trip[6]]

	return trips_dict

def routesDictBuilder(routes):
	'''
	iterates over a routes.txt file, returns a dict of route_id keys with route_short_name, route_long_name, and route_type values 
	just for ease when the route names are needed, rather than having to open the file can just access this dict
	'''
	routes_dict = {}
	with open(routes) as rts:
		next(rts) # skip header
		for line in rts:
			route = line.strip().split(",") # strip some /r/n off line end
			routes_dict[route[0]] = [route[2],route[3],route[5]]

	return routes_dict

def shapeDictBuilder(shapes):
	'''
	iterates over a shapes.txt file, returns a dict with lat / lon tuples as values to a shape_dist_travelled key, which is a sub-dict for a shape_id key
	this saves iteration time rather than searching each line of shapes.txt every time a route_id shape is needed
	storing the lat/lons as floats for interpolation by shapely
	'''
	shapes_dict = {}
	with open(shapes) as shps:
		next(shps) # skip header
		for line in shps:
			shape = line.strip().split(",") # strip some /r/n off line end
			if shape[0] not in shapes_dict.keys():
				#must generate first-level key seperately
				shapes_dict[shape[0]] = {}
				shapes_dict[shape[0]][shape[4]] = (float(shape[1]),float(shape[2]))
			else:
				shapes_dict[shape[0]][shape[4]] = (float(shape[1]),float(shape[2]))

	return shapes_dict

def stoptimesDictBuilder(stop_times):
	'''
	iterates over a stop_times.txt file, returns an OrderedDict of trip_id keys with values that are sub-dicts of time keys and shape_dist_travelled values
	'''
	stop_times_Odict = OrderedDict()
	with open(stop_times) as stptms:
		next(stptms) # skip header
		for line in stptms:
			stoptime = line.strip().split(",") # strip some /r/n off line end
			# if stoptime[0] not in shapes_dict.keys():
			if stoptime[0] not in stop_times_Odict.keys():
				#must generate first-level key seperately
				stop_times_Odict[stoptime[0]] = OrderedDict()
				stop_times_Odict[stoptime[0]][stoptime[1]] = stoptime[8]
			else:
				stop_times_Odict[stoptime[0]][stoptime[1]] = stoptime[8]

	return stop_times_Odict

def positionInterpolator(latlons, times):
	'''
	takes a list of lat/lons (that describe a contiguous line) and a tuple of times (where the times are strings)
	for the number of seconds between the two times, calculates interpolated lat/lons along the line for each second between the two times
	returns a dict of times as keys with lat/lons as values

	EXTERNAL LIB: uses Shapely for line interpolation
	'''
	#specify time format
	FMT = '%H:%M:%S'
	#convert times to datetime objects
	try:
		t1 = datetime.strptime(times[0], FMT)
	except ValueError:
		t1 = datetime.strptime(('00'+times[0][2:]), FMT)
	try:
		t2 = datetime.strptime(times[1], FMT)
	except ValueError: #trying to parse a time that is past midnight, stored like '24:00:00'
		t2 = datetime.strptime(('00'+times[1][2:]), FMT) #strip the '24' off and shove in a '00'

	#useful vars
	end_of_day = datetime.strptime('23:59:59', FMT)
	start_of_day = datetime.strptime('00:00:00', FMT)
	#difference between times, using timedelta objs
	#richards 'midnight bug' - if times[1] is after midnight AND times[0] is before midnight (i.e. trip crosses midnight), then add the diff between times[0] and midnight (plus 1 to account for 23:59:59 being latest datetime will go) ((23:59:59 - times[0]) +1 ) to the diff between times[1] and midnight (times [1] - 00:00:00)
	if t2 < t1:
		diff = ((end_of_day - t1).total_seconds() + 1) + ((t2 - start_of_day).total_seconds())
	else:
		diff = (t2 - t1).total_seconds()

	#fraction to be plugged into shapely's interpolate (what % of 100 is diff?)
	fraction = (100 / diff) / 100

	#build dict
	interp_dict = {}

	#buiid Linestring
	linepoints = []
	for s,l in sorted(latlons.iteritems()):
		linepoints.append(l)
	line = LineString(linepoints)

	i = 1
	while i < diff:
		#t1 + seconds(i)
		coords = line.interpolate((fraction * i), normalized=True).coords[0]
		t = (t1 + timedelta(0,i)).strftime(FMT)
		interp_dict[t] = coords
		i += 1

	return interp_dict

def completeDictBuilder(day, trips_dict, routes_dict, shapes_dict, stop_times_Odict):
	'''
	COMPLEX YO

	NB. this could be better if it returned a dict structure with keys that become CSV-like headers (would work well for JSON too)

	got trips_dict from tripsIterator, which is {trip_id:[route_id,shape_id]}
	got routes_dict from routesDictBuilder, which is {route_id:[route_short_name,route_long_name,route_type]}
	got shapes_dict from shapeDictBuilder, which is {shape_id:{shape_dist_travelled:(lat,lon)}}
	got stop_times_Odict from stoptimesDictBuilder, which is {trip_id:{arrival_time:shape_dist_travelled}}
	will get interp_dict from positionInterpolator, which is {time, (lat, lon)}

	PROCESS:
	(of course, include the route name (short and long) and route type as well) NOT YET DOCUMENTED HERE

	new dict with day key
	for each trip (for trip in trips_dict):
		access stop_times_Odict with trip, then for each arrival_time (for arrival_time in stop_times_Odict[trip]):
			check storage_dict, if not empty then
				1. put the time in the storage_dict and arrival_time into a tuple IN THAT ORDER (times)
				2. get the shape_dist_travelled value for arrival_time key (stop_times_Odict[trip][arrival_time]) (UNLESS the value can also be passed down as a var)
				3. get the shape_id value for this trip key from trips_dict (trips_dict[trip]) (UNLESS the value can also be passed down as a var)
				4. access shapes_dict with this trip shape_id as key
				5. for each shape_dist_travelled key > the storage_dict shape_dist_travelled BUT < the current shape_dist_travelled:
					- get the lat/lon, add them to a list
				6. call positionInterpolator with the latlons list and times tuple as args
				7. get the returned dict and iterate over it, adding the times and lat/lons to new dict (complete_dict[day][trip][time] = (lat,lon))
				8. add arrival_time and accessed lat/lon (if shape_dist_travelled == 0, just access 1) to new dict (complete_dict[day][trip][arrival_time] = (lat,lon))
				9. replace values in the storage_dict with arrival_time as key and shape_dist_travelled as value
			if empty:
				1. add arrival_time and accessed lat/lon (if shape_dist_travelled == 0, just access 1) to new dict (complete_dict[day][trip][arrival_time] = (lat,lon))
				2. add arrival_time as key and shape_dist_travelled as value to storage_dict
		after all arrival_times:
			1. remove key:value from the storage_dict


	final dict structure: {day:{trip_id:[{time:(lat,lon)},short_name,long_name,type]}}
	means the __main__ question can be answered
	this dict can be iterated over to build a CSV file.
	'''
	storage_dict = {}
	complete_dict = {day: {}}

	for trip, route_shape in trips_dict.iteritems(): #route_shape[0] is a route_id, route_shape[1] is a shape_id
		complete_dict[day][trip] = [{}]
		for time, stop_shpdst in stop_times_Odict[trip].iteritems(): #sorting this will take bloody forever, think of a better way in future (store in lists?) OrderedDict from collections module (only python2.7+)

			#changing to use both the current and previous stop_time + position to interpolate in-betweens
			if any(storage_dict) == False: #check if storage_dict is empty, if so then this is first pass for this trip_id
				complete_dict[day][trip][0][time] = shapes_dict[route_shape[1]]['1'] #no '0' shp_shpdst, so just using '1' instead for lat/lon
				storage_dict[time] = stop_shpdst #prep it for the next run
			else:
				for storage_key, storage_value in storage_dict.iteritems():
					times = (storage_key, time) #packed and ready to ship to positionInterpolator
					latlons = {} #will soon be packed for shipping
					for shp_shpdst, latlon in shapes_dict[route_shape[1]].iteritems():
						if int(shp_shpdst) >= int(storage_value) and int(shp_shpdst) <= int(stop_shpdst): #basically, looking for bits of the line segment between the two times in the 'times' tuple
							latlons[shp_shpdst] = latlon
					for new_time, ll in positionInterpolator(latlons, times).iteritems(): #running positionInterpolator to get interpolated lat/lons for all the seconds in between the two times
						#if the dict returns empty? this could happen if 'times' happen to only be 1 second apart anyway, may never happen (if it does, step below will error with no 'new_time' or 'll')
						complete_dict[day][trip][0][new_time] = ll #adding all those lovely new times and positions to the master dict

				#add the current stop_time + position to the complete_dict
				complete_dict[day][trip][0][time] = shapes_dict[route_shape[1]][stop_shpdst] #adding the time and associated lat/lon from initial trip_id search to complete_dict
				storage_dict = {} #clearing out the storage_dict
				storage_dict[time] = stop_shpdst #and prepping it for the next run

			# if any(storage_dict) == True: #check if storage_dict is NOT empty
			# 	for storage_key, storage_value in storage_dict.iteritems():
			# 		times = (storage_key, time) #packed and ready to ship to positionInterpolator
			# 		latlons = [] #will soon be packed for shipping
			# 		for shp_shpdst, latlon in shapes_dict[route_shape[1]].iteritems():
			# 			print shp_shpdst, latlon, type(shp_shpdst), type(latlon)
			# 			print storage_value, type(storage_value)
			# 			print stop_shpdst, type(stop_shpdst)
			# 			if int(shp_shpdst) > int(storage_value) and int(shp_shpdst) < int(stop_shpdst): #basically, looking for bits of the line segment between the two times in the 'times' tuple
			# 				latlons.append(latlon)
			# 		for new_time, ll in positionInterpolator(latlons, times).iteritems(): #running positionInterpolator to get interpolated lat/lons for all the seconds in between the two times
			# 			#if the dict returns empty? this could happen if 'times' happen to only be 1 second apart anyway, may never happen (if it does, step below will error with no 'new_time' or 'll')
			# 			complete_dict[day][trip][0][new_time] = ll #adding all those lovely new times and positions to the master dict

			# 	complete_dict[day][trip][0][time] = shapes_dict[route_shape[1]][stop_shpdst] #adding the time and associated lat/lon from initial trip_id search to complete_dict
			# 	storage_dict = {} #clearing out the storage_dict
			# 	storage_dict[time] = stop_shpdst #and prepping it for the next run

			# else: #if storage_dict IS empty, i.e. this is the first run for this trip
			# 	#stop_shpdst will == '0' due to use of OrderedDict, and there is no '0' shape_dist_travelled in the shapes.txt for some reason: so i'm fudging and just having '0' use the same lat/lon as '1'
			# 	complete_dict[day][trip][0][time] = shapes_dict[route_shape[1]]['1'] #adding the time and associated lat/lon from initial trip_id search to complete_dict
			# 	storage_dict[time] = stop_shpdst

		complete_dict[day][trip].append(routes_dict[route_shape[0]][0]), complete_dict[day][trip].append(routes_dict[route_shape[0]][1]), complete_dict[day][trip].append(routes_dict[route_shape[0]][2]) #monster assignment of route parameters, easy enough
		storage_dict = {} #clearing out the storage_dict for the first run of the next trip

	return complete_dict

#running from terminal
if __name__ == "__main__":
	#import dict_converter.py for conversion funcs
	import dict_converter
	
	##get and parse terminal args
	parser = argparse.ArgumentParser()
	parser.add_argument("GTFS", help="enter path to GTFS folder, must end with trailing slash")
	parser.add_argument("fileout", help="enter path/name for output file")
	parser.add_argument("day", help="enter day in lowercase")
	parser.add_argument("converter", help="choose a converter to apply") #can i provide a list of options in help? and make it so it doesn't accept bad options?
	args = parser.parse_args()

	#run funcs with args.GTFS + *.txt, store results, feed into further funcs

	service_ids = calendarIterator((args.GTFS + 'calendar.txt'), args.day)
	trips_dict = tripsIterator((args.GTFS + 'trips.txt'), service_ids)
	routes_dict = routesDictBuilder((args.GTFS + 'routes.txt'))
	shapes_dict = shapeDictBuilder((args.GTFS + 'shapes.txt'))
	stop_times_Odict = stoptimesDictBuilder((args.GTFS + 'stop_times.txt'))
	
	# service_ids = calendarIterator(('../GTFS-welly/' + 'calendar.txt'), 'monday')
	# trips_dict = tripsIterator(('../GTFS-welly/' + 'trips.txt'), service_ids)
	# routes_dict = routesDictBuilder(('../GTFS-welly/' + 'routes.txt'))
	# shapes_dict = shapeDictBuilder(('../GTFS-welly/' + 'shapes.txt'))
	# stop_times_Odict = stoptimesDictBuilder(('../GTFS-welly/' + 'stop_times.txt'))	

	complete_dict = completeDictBuilder(args.day, trips_dict, routes_dict, shapes_dict, stop_times_Odict)
	# complete_dict = completeDictBuilder('monday', trips_dict, routes_dict, shapes_dict, stop_times_Odict)
	

	##debugging
	# print service_ids #works
	# print trips_dict #assuming works, looks good
	# print routes_dict #works
	# print shapes_dict #works
	# print stop_times_Odict #works
	# print complete_dict['monday']['1']

	#fetch converter from dict_converter
	conv = getattr(dict_converter, args.converter)

	#apply conversion
	conv(complete_dict, args.fileout)

	print "if you see this, well done, file built and time to party"