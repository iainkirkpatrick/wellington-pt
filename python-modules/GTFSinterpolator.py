#interpolating GTFS trip positions for each second in a 24-hour day
#and then writing to SQL db that Richard's AB_GTFStoSQL has built

#imports
import argparse
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

#shapes_dict looks like: {'shape_id':{'shape_dist_traveled':[lat,lon]}}
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

#trip_dict looks like: {"trip":{"time":[["lat","lon"],"short_name","long_name","type"]}}
def trip_dictBuilder():
	#trip_dict looks like: {"trip":{"time":[["lat","lon"],"short_name","long_name","type"]}}
	trip_dict = {}
	for route_id, trip_id, shape_id in con.cursor().execute('SELECT route_id, trip_id, shape_id from trips;'):
		print trip_id
		storage_dict = {}
		trip_dict[trip_id] = {}
		for stop_time, stop_shpdst in con.cursor().execute('SELECT arrival_time, shape_dist_traveled FROM stop_times WHERE trip_id = ?;', [trip_id]):
			stop_time = stop_time[:-4] #whacking the .000 off the end, don't need it
			# details = con.cursor().execute('SELECT route_short_name, route_long_name, route_type_desc FROM routes WHERE route_id = ?;', [route_id])
			
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
			details = con.cursor().execute('SELECT route_short_name, route_long_name, route_type_desc FROM routes WHERE route_id = ?;', [route_id])
			for d in details:
				trip_dict[trip_id][stop_time].append(str(d))

			# #debugging
			# print trip_dict[trip_id][stop_time]
			# print "base time"

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

						#getting latlons
						trip_dict[trip_id][new_time].append([ll[0],ll[1]])
						#use the details ow
						details = con.cursor().execute('SELECT route_short_name, route_long_name, route_type_desc FROM routes WHERE route_id = ?;', [route_id])
						for d in details:
							trip_dict[trip_id][new_time].append(str(d))

						# #debugging
						# print trip_dict[trip_id][new_time]
						# print "new time"

			#clear storage, prep it for next run
			storage_dict = {}
			storage_dict[stop_time] = stop_shpdst

		# #debugging
		# for time, details in trip_dict[trip_id].iteritems():
		# 	print time, details[0][0], details[1].split(",")[1].strip('()').strip() #double strip to also remove whitespace

	return trip_dict


if __name__ == '__main__':

	parser = argparse.ArgumentParser()
	parser.add_argument("db", help="enter path to SQL database") #i.e. '../GTFSSQL_Wellington_20140308_203334.db'
	args = parser.parse_args()

	#connect to db
	con = sqlite3.connect(args.db)

	#build dicts
	shapes_dict = shapes_dictBuilder()
	print "shapes_dict built, now building trip_dict (7677 trips)"
	trip_dict = trip_dictBuilder()
	print "trip_dict built"

	#drop positions table in case it exists already
	##NB. this will error if there is no table, fix this
	con.cursor().execute('DROP TABLE positions;')
	#add positions table
	# contains trip_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, time, lat, lon, short_name, long_name, type
  	con.cursor().execute('CREATE TABLE positions(trip_id INTEGER REFERENCES trips(trip_id), monday INTEGER, tuesday INTEGER, wednesday INTEGER, thursday INTEGER, friday INTEGER, saturday INTEGER, sunday INTEGER, time TEXT, lat FLOAT, lon FLOAT, route_short_name TEXT REFERENCES routes(route_short_name), route_long_name TEXT REFERENCES routes(route_long_name), route_type_desc TEXT REFERENCES routes(route_type_desc))')

  	#must generate a big old list of lists, where each list represents a row to be entered
  	#because sqlite3 python is a #$#$#$@8#*#@^ and doesn't play nice with nested for loops and INSERT's 

  	rows = []

  	for trip_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday in con.cursor().execute('SELECT trips.trip_id, calendar.monday, calendar.tuesday, calendar.wednesday, calendar.thursday, calendar.friday, calendar.saturday, calendar.sunday FROM calendar JOIN trips ON calendar.service_id = trips.service_id;'):
  		# print trip_id
  		for time, details in trip_dict[trip_id].iteritems():
  			print trip_id, time, details

  			lat = details[0][0]
  			lon = details[0][1]
  			short_name = details[1].split(",")[0].strip('()').strip() #double strip to also remove whitespace
  			long_name = details[1].split(",")[1].strip('()').strip()
  			route_type_desc = details[1].split(",")[2].strip('()').strip()

			rows.append([trip_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, time, lat, lon, short_name, long_name, route_type_desc])

	con.cursor().executemany('INSERT INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', rows)

	    	# con.cursor().execute('INSERT INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (trip_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, time, lat, lon, short_name, long_name, route_type_desc))

	con.commit()
	con.close()



