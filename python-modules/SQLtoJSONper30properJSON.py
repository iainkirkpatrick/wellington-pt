#extracting GTFS data from SQL db after Richard's AB_GTFStoSQL has built it
#put the data in a JSON format for use on the web

# this script builds a JSONobj with positions for all applicable trips for every 30 seconds of a day
# useful for smaller size data for web viz

#imports
import json
import io
import sqlite3
from shapely.geometry import LineString
from datetime import datetime
from datetime import timedelta

#funcs
def positionInterpolator(latlons, times):
	'''
	takes a list of lat/lons (that describe a contiguous line) and a tuple of times (where the times are strings)
	for the number of seconds between the two times, calculates interpolated lat/lons along the line for each second between the two times
	returns a dict of times as keys with lat/lons as values

	EXTERNAL LIB: uses Shapely for line interpolation
	'''
	#build dict
	interp_dict = {}

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

	#if there is only a second or less difference between the two times (happens at least once in the stop_times.txt file, bizarre)
	if diff <= 1:
		#then just return an empty interp_dict, as there are no in-between seconds to interpolate positions for
		return interp_dict
	else:
		#fraction to be plugged into shapely's interpolate (what % of 100 is diff?)
		fraction = (100 / diff) / 100

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

def shapes_dictBuilder():
	#shapes_dict looks like: {'shape_id':{'shape_dist_traveled':[lat,lon]}}
	shapes_dict = {}
	for results in con.cursor().execute('SELECT * FROM shapes;'):
		try:
			shapes_dict[results[0]][str(results[4])] = [results[1],results[2]]
		except KeyError:
			shapes_dict[results[0]] = {}
			shapes_dict[results[0]][str(results[4])] = [results[1],results[2]]

	return shapes_dict

def trip_dictBuilder():
	#trip_dict looks like: {"trip":{"time":[["lat","lon"],"short_name","long_name","type"]}}
	trip_dict = {}
	for route_id, trip_id, shape_id in con.cursor().execute('SELECT route_id, trip_id, shape_id from trips;'):
		print trip_id
		storage_dict = {}
		trip_dict[trip_id] = {}
		for stop_time, stop_shpdst in con.cursor().execute('SELECT arrival_time, shape_dist_traveled FROM stop_times WHERE trip_id = ?;', [trip_id]):
			stop_time = stop_time[:-4] #whacking the .000 off the end, don't need it
			details = con.cursor().execute('SELECT route_short_name, route_long_name, route_type_desc FROM routes WHERE route_id = ?;', [route_id])
			
			trip_dict[trip_id][stop_time] = []
			# try:
			# 	JSONobj[day][stop_time][trip_id] = []
			# except KeyError:
			# 	JSONobj[day][stop_time] = {}
			# 	JSONobj[day][stop_time][trip_id] = []

			#getting latlons
			if stop_shpdst == 0:
				#just fudging for now, stop_shpdst == 0 is same latlong as stop_shpdst == 1
				trip_dict[trip_id][stop_time].append(shapes_dict[str(shape_id).strip()]['1'])
			else:
				trip_dict[trip_id][stop_time].append(shapes_dict[str(shape_id).strip()][str(int(stop_shpdst))])
			
			#use the details ow
			for d in details:
				trip_dict[trip_id][stop_time].append(str(d))

			#check if storage_dict is empty, if so then this is first pass for this trip_id, no interp needed
			if any(storage_dict) == False: 
				pass
			#if not
			else:
				#interpolation time!
				for storage_key, storage_value in storage_dict.iteritems():
					times = (storage_key, stop_time) #packed and ready to ship to positionInterpolator, [:-4] is to lob the milliseconds of the time string passed
					latlons = {} #will soon be packed for shipping
					#get latlons to pass to positionInterpolator to build a line out of
					for shp_shpdst, latlon in shapes_dict[str(shape_id).strip()].iteritems():
						if int(shp_shpdst) >= int(storage_value) and int(shp_shpdst) <= int(stop_shpdst): #basically, looking for bits of the line segment between the two times in the 'times' tuple
							latlons[shp_shpdst] = latlon
					for new_time, ll in positionInterpolator(latlons, times).iteritems(): #running positionInterpolator to get interpolated lat/lons for all the seconds in between the two times
						#if the dict returns empty (due to the difference between the times being <= 1), then these should just fail quietly as there is no new_time var returned
						# new_time = new_time + '.000'
						# print new_time, ll

						trip_dict[trip_id][new_time] = []
						# try:
						# 	JSONobj[day][new_time][trip_id] = []
						# except KeyError:
						# 	JSONobj[day][new_time] = {}
						# 	JSONobj[day][new_time][trip_id] = []

						#geting latlons
						trip_dict[trip_id][new_time].append([ll[0],ll[1]])
						#use the details ow
						for d in details:
							trip_dict[trip_id][new_time].append(str(d))

			#clear storage, prep it for next run
			storage_dict = {}
			storage_dict[stop_time] = stop_shpdst

	return trip_dict

##run the building funcs, then grab from the trip_dict the data from each trip that runs on the day
##remember to smush the time key's values together, to form a complete dict for the JSONobj

#init JSON obj, will look like {"day":{"time":{"trip":[["lat","lon"],"short_name","long_name","type"]}}}
###FIX FOR D3 EASYNESS: (and more betterer JSON i think)
# {
#     "days": [
#         {
#             "day": "monday",
#             "times": [
#                 {
#                     "time": "00:00:00",
#                     "trips": [
#                         {
#                             "trip": 2323,
#                             "position": [
#                           	-41.30613061561395,
#          						174.77825838030213
#                             ],
#                             "short_name": "bla",
#                             "long_name": "blabla",
#                             "type": "busyo"
#                         }
#                     ]
#                 }
#             ]
#         }
#     ]
# }

JSONobj = {"days":[]}

#storage dict
storage_dict = {}

#days list
# days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
days = ["monday"]

#connect to db
con = sqlite3.connect('../GTFSSQL_Wellington_20140308_203334.db')

#shapes_dict looks like: {'shape_id':{'shape_dist_traveled':[lat,lon]}}
shapes_dict = shapes_dictBuilder()
print "shapes_dict built, now building trip_dict (7677 trips)"
#trip_dict looks like: {"trip":{"time":[["lat","lon"],"short_name","long_name","type"]}}
trip_dict = trip_dictBuilder()
print "trip_dict built"


#let the games begin
#run monday for now, crashed last time doing all days

#counter to access the correct day object in the list
day_counter = 0

for day in days:
	print day
	JSONobj["days"].append({"day":day, "times":[]})

	FMT = '%H:%M:%S'
	per30time = datetime.strptime('00:00:00', FMT)
	j = 0
	while j < 2880: #that's how many 30 second periods there are in a day, beginning at midnight
		t = per30time.strftime(FMT)
		print t
		JSONobj["days"][day_counter]["times"].append({"time":t, "trips":[]})
		per30time += timedelta(seconds=30)
		j += 1

	#just a visual indicator of progress of script to terminal
	i = 0

	#select trip_ids that occur on day
	for trip_id in con.cursor().execute('SELECT trips.trip_id FROM calendar JOIN trips ON calendar.service_id = trips.service_id WHERE calendar.%s = 1;' %day):
		#for each trip_id, look up the trip_dict, get the values and add to JSONobj
		trip_id = trip_id[0]
		i += 1
		print trip_id, i
		#i think faster to iterate over each trip's times in the trip_dict (about 300 avg? surely < 2800 avg) rather than over all possible JSONobj times (2800) for matches
		for time, details in trip_dict[trip_id].iteritems():
			# if time in JSONobj[day].keys():
			if time[-2:] == "00" or time[-2:] == "30":
				# JSONobj[day][time][trip_id] = details
				for obj in JSONobj["days"][day_counter]["times"]:
					if obj["time"] == time:
						obj["trips"].append({"trip":trip_id, "position":details[0], "short_name":details[1], "long_name":details[2], "type":details[3]}) 
						#break once match is found, saves time
						break
				
		# for time, details in trip_dict[trip_id].iteritems():
		# 	try:
		# 		JSONobj[day][time][trip_id] = details
		# 	except KeyError:
		# 		JSONobj[day][time] = {}
		# 		JSONobj[day][time][trip_id] = details

	day_counter += 1

#write JSONobj to file
with io.open('mondayper30.json', 'w', encoding='utf-8') as f:
  f.write(unicode(json.dumps(JSONobj, ensure_ascii=False)))

print "finshed"










