#just generating the full list per second of each trip in CSV
#only generating for a specific day for now, fix it up later
#nice to be able to store it so don't have to repeat 40 mins all the time
#then use CSVtoJSON to produce JSON

#if this works, consider modularising it, and allow either to CSV or a further SQL table
#and then modularise the toJSON i guess

#NB. this uses Richard's definition of stop_times and which day a trip runs on - a trip can run on multiple days
#so really selecting trip portions per day

#imports
import argparse
import csv
import sqlite3
from shapely.geometry import LineString
from datetime import datetime
from datetime import timedelta

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
	#the 'midnight bug' - if times[1] is after midnight AND times[0] is before midnight (i.e. trip crosses midnight), then add the diff between times[0] and midnight (plus 1 to account for 23:59:59 being latest datetime will go) ((23:59:59 - times[0]) +1 ) to the diff between times[1] and midnight (times [1] - 00:00:00)
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
#### NEW
#trip_dict looks like: {"trip":{"time":["lat","lon"],"details":["short_name","long_name","type"]}}
def trip_dictBuilderMonday():
	#trip_dict looks like: {"trip":{"time":["lat","lon"],"details":["short_name","long_name","type"]}}
	trip_dict = {}
	storage_dict = {}

	#get corrected trips, as with Richard's amendments there may now be some trips which run on additional days
	for trip_id, arrival_time, shape_dist_traveled in con.cursor().execute('SELECT trip_id, arrival_time, shape_dist_traveled FROM stop_times_amended WHERE monday = 1;'):

		#multiple entries for each trip_id in stop_times_amended, don't want to overwrite previous iterations
		if trip_id in trip_dict:
			pass
		else:
			#if it's a 'new' trip_id
			print trip_id
			trip_dict[trip_id] = {}
			trip_dict[trip_id]["details"] = []
			storage_dict = {}

			#use the details ow
			details = con.cursor().execute('SELECT routes.route_short_name, routes.route_long_name, routes.route_type_desc FROM routes JOIN trips ON routes.route_id = trips.route_id WHERE trips.trip_id = ?;', [trip_id])
			for d in details:
				trip_dict[trip_id]["details"].append(str(d).split(',')[0].strip('()').strip())
				trip_dict[trip_id]["details"].append(str(d).split(',')[1].strip('()').strip())
				trip_dict[trip_id]["details"].append(str(d).split(',')[2].strip('()').strip())

			# print trip_dict[trip_id]["details"]

		# trip_dict[trip_id] = {}
		stop_time = arrival_time[:-4] #whacking the .000 off the end, don't need it
		# shape_id = con.cursor().execute('SELECT shapes.shape_id, shapes.shape_pt_sequence FROM shapes JOIN trips ON shapes.shape_id = trips.shape_id WHERE trips.trip_id = ?;', [trip_id])
		shape_id = con.cursor().execute('SELECT shape_id from trips WHERE trip_id =?;', [trip_id])

		#reassigning shape_id value from cursor object
		shape_id = str(shape_id.fetchone()[0])

		# trip_dict[trip_id][stop_time] = []
		# #getting latlons
		# if shape_dist_traveled == 0:
		# 	#just fudging for now, shape_dist_traveled == 0 is same latlong as shape_dist_traveled == 1
		# 	print shapes_dict[str(shape_id).strip()]['1']
		# 	trip_dict[trip_id][stop_time].append(shapes_dict[str(shape_id).strip()]['1'])
		# else:
		# 	# print shapes_dict[str(shape_id).strip()][str(int(shape_dist_traveled))]
		# 	trip_dict[trip_id][stop_time].append(shapes_dict[str(shape_id).strip()][str(int(shape_dist_traveled))])

		#getting latlons
		if shape_dist_traveled == 0:
			#just fudging for now, shape_dist_traveled == 0 is same latlong as shape_dist_traveled == 1
			# print shapes_dict[str(shape_id).strip()]['1']
			trip_dict[trip_id][stop_time] = shapes_dict[str(shape_id).strip()]['1']
		else:
			# print shapes_dict[str(shape_id).strip()][str(int(shape_dist_traveled))]
			trip_dict[trip_id][stop_time] = shapes_dict[str(shape_id).strip()][str(int(shape_dist_traveled))]

		# #use the details ow
		# details = con.cursor().execute('SELECT routes.route_short_name, routes.route_long_name, routes.route_type_desc FROM routes JOIN trips ON routes.route_id = trips.route_id WHERE trips.trip_id = ?;', [trip_id])
		# for d in details:
		# 	trip_dict[trip_id][stop_time].append(str(d))

		# print "original time", trip_dict[trip_id][stop_time]

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
					if int(shp_shpdst) >= int(storage_value) and int(shp_shpdst) <= int(shape_dist_traveled): #basically, looking for bits of the line segment between the two times in the 'times' tuple
						latlons[shp_shpdst] = latlon

				for new_time, ll in positionInterpolator(latlons, times).iteritems(): #running positionInterpolator to get interpolated lat/lons for all the seconds in between the two times
					#if the dict returns empty (due to the difference between the times being <= 1), then these should just fail quietly as there is no new_time var returned
					# new_time = new_time + '.000'
					# print new_time, ll
					
					# try:
					# 	JSONobj[day][new_time][trip_id] = []
					# except KeyError:
					# 	JSONobj[day][new_time] = {}
					# 	JSONobj[day][new_time][trip_id] = []

					# trip_dict[trip_id][new_time] = []
					# #geting latlons
					# trip_dict[trip_id][new_time].append([ll[0],ll[1]])

					#geting latlons
					trip_dict[trip_id][new_time] = [ll[0],ll[1]]
					# #use the details ow
					# details = con.cursor().execute('SELECT routes.route_short_name, routes.route_long_name, routes.route_type_desc FROM routes JOIN trips ON routes.route_id = trips.route_id WHERE trips.trip_id = ?;', [trip_id])
					# for d in details:
					# 	trip_dict[trip_id][new_time].append(str(d))

					# print "new time", trip_dict[trip_id][new_time]

		#prep storage for next run
		storage_dict = {}
		storage_dict[stop_time] = shape_dist_traveled

	return trip_dict

if __name__ == "__main__":

	##get and parse terminal args
	parser = argparse.ArgumentParser()
	parser.add_argument("db", help="enter path to SQL db") #i.e. '../GTFSSQL_Wellington_20140308_203334.db'
	parser.add_argument("csvfileout", help="enter path/name for output CSV file")
	args = parser.parse_args()

	#connect to db
	con = sqlite3.connect(args.db)

	#shapes_dict looks like: {'shape_id':{'shape_dist_traveled':[lat,lon]}}
	shapes_dict = shapes_dictBuilder()
	print "shapes_dict built, now building trip_dict (7677 trips)"
	#trip_dict looks like: {"trip":{"time":[["lat","lon"],"short_name","long_name","type"]}}
	trip_dict = trip_dictBuilderMonday()
	print "trip_dict built"

	#process
	#trip_dict only contains trips and times that occur on a single day
	#write it all out flat to a csv
	with open(args.csvfileout, 'wb') as csvfile:
	    csvwriter = csv.writer(csvfile, delimiter=',')
	    #trip_dict looks like: {"trip":{"time":["lat","lon"],"details":["short_name","long_name","type"]}}
	    for trip,obj in trip_dict.iteritems(): 
	    	trip_details = obj["details"]
	    	for k2,v2 in obj.iteritems(): #k2 is a time or "details", v2 is either list of latlons or list of details, yes this is confusing, need to change
	    		if k2 == "details":
	    			pass
	    		else:
	    			# print trip,k2,v2
	    			#can now write to csv row
	    			#each row should look like trip,time,lat,lon,short_name,long_name,type
	    			row = [trip,k2,v2[0],v2[1],trip_details[0],trip_details[1],trip_details[2]]
	    			print row
	    			csvwriter.writerow(row)
	    		
	print "toCSV complete"