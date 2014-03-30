#this module contains various functions for parsing and converting dictionaries to other filetypes
#developed for use with GTFS dictionaries created by GTFSparser.py

#final dict structure: {day:{trip_id:[{time:(lat,lon)},short_name,long_name,type]}}

#imports
import csv
import json, io

def toCSV(dictionary, csvfileout):
	'''
	iterates over a complete_dict and writes rows to an output CSV file
	each day is a different CSV, an example row: trip,time,lat,lon,short_name,long_name,type
	a row for each second of the total trip (lots of rows!)
	'''

	with open(csvfileout, 'wb') as csvfile:
	    csvwriter = csv.writer(csvfile, delimiter=',')
	    
	    #build in some testing for if v is another dict, rather than assuming it is
	    #for now, just handling complete_dict {day:{trip_id:[{time:(lat,lon)},short_name,long_name,type]}}
	    for k,v in dictionary.iteritems():
	    	for k2,v2 in v.iteritems(): #v2 is the list
	    		for k3,v3 in v2[0].iteritems():

	    			#can now write to csv row
	    			csvwriter.writerow([k2,k3,v3[0],v3[1],v2[1],v2[2],v2[3]])

	print "toCSV complete"

def toJSON(dictionary, jsonfileout):
	'''
	iterates over a complete_dict and writes rows to an output JSON file
	NB. produces a standard format JSON file, i.e. more verbose than the input dict
	NB. and the structure order is changed, times are 'higher' in the structure
	output structure: 
	{ "days": [ {"day": "monday", "times": [ { "time": 23, "trips": [ { "trip": 2323, "position": [ "lat", "lon" ], "short_name": "bla", "long_name": "blabla", "type": "busyo" } ] } ] } ] }
	'''

	JSON_dict = {}
	
	
	with io.open(jsonfileout, 'w', encoding='utf-8') as jsonfile:
		jsonfile.write(unicode(json.dumps(dictionary, ensure_ascii=False)))