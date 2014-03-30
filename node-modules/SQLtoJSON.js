// purpose of this script is to read from a GTFS SQLite db
// then create a JSON file ready for use in a web viz
// run it with node yo

//args can be passed to generate different JSON files
//days: all, or specific days
//resolution: min is 1 sec, value is expressed in seconds (probably wouldn't recommend going above 60 seconds?)

var fs = require("fs");
var path = require("path");
var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('../GTFSSQL_Wellington_20140308_203334.db', sqlite3.OPEN_READONLY);
var argv = require('minimist')(process.argv.slice(2));
var _ = require('lodash');

//probably need Database.each, as this calls callback func for each result row returned
//can then add to JSON iteratively?
//or better to grab the entire result, then iterate over it?

// {
//     "days": [
//         {
//             "day": "monday",
//             "times": [
//                 {
//                     "time": "00:00:00",
//                     "trips": [
//                         {
//                             "trip": 2323,
//                             "position": [
//                           	-41.30613061561395,
//          						174.77825838030213
//                             ],
//                             "short_name": "bla",
//                             "long_name": "blabla",
//                             "type": "busyo"
//                         }
//                     ]
//                 }
//             ]
//         }
//     ]
// }

//SELECT calendar.service_id, trips.trip_id FROM calendar JOIN trips ON calendar.service_id = trips.service_id WHERE calendar.monday = 1;
//that selects all trip_id's on a monday

// db.each('sql', [ params ], function(err, row) {
	
// })
// contains trip_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, time, latlon, short_name, long_name, type

var days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"];
var JSONobj = { "days": [] };

//write a better way to set this in future
//setting times in the JSONobj, per 30 secs atm
for (var i = 0; i < days.length; i++) {

	JSONobj["days"].push( { "day": days[i], "times": [] } );

	for (var j = 0; j < 86400; j=j+30) {
		// console.log(JSONobj["days"][i]);
		var d = new Date(2000, 0, 1,  0, 0, j, 0);
		// console.log(d)

		var hours = d.getHours().toString();
		if (hours.length < 2) {
			hours = "0" + hours
		};
		var mins = d.getMinutes().toString();
		if (mins.length < 2) {
			mins = "0" + mins
		};
		var secs = d.getSeconds().toString();
		if (secs.length < 2) {
			secs = "0" + secs
		};

		var timeString = hours + ":" + mins + ":" + secs;

		JSONobj["days"][i]["times"].push( { "time": timeString, "trips": [] } );

		// console.log(timeString);

	};
};

//for the moment, just selecting the data where monday = 1
// db.each('SELECT calendar.service_id, trips.trip_id FROM calendar JOIN trips ON calendar.service_id = trips.service_id WHERE calendar.monday = 1;', function(err, row) {
db.each('SELECT * FROM positions WHERE monday = 1;', function(err, row) {
	//add results of each row to the JSONobj
	
	console.log(row.trip_id);
	if (row.time.slice(-2) == "00" || row.time.slice(-2) == "30") {
		//use lo-dash to find the index of the object that has the matching time
		var i = _.findIndex(JSONobj["days"][0].times, { time: row.time });
		JSONobj["days"][0].times[i].trips.push({ "trip": row.trip_id, "position": [row.lat,row.lon], "short_name": row.route_short_name, "long_name": row.route_long_name, "type": row.route_type_desc })
	};
});
console.log("JSONobj built")

//write JSONobj to file
var strJSONobj = JSON.stringify(JSONobj)
console.log("stringified")

fs.writeFileSync("monday.json") , strJSONobj, function (err) {
  if (err) throw err;
  console.log('file written');
};





