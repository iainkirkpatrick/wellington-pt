var fs = require('fs');
var csv = require('csv');
var _ = require('lodash');

// opts is optional
// var opts = ;

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

		console.log(timeString);

	};
};

csv()
.from.path(__dirname+'/../results/firstjamMonday.csv', { delimiter: ','})
// .to.stream(fs.createWriteStream(__dirname+'/sample.json'))
.to.path(__dirname+'/sample.json')
// .transform( function(row){
//   row.unshift(row.pop());
//   return row;
// })
.on('record', function(row,index){
  // console.log('#'+index+' '+JSON.stringify(row));

  if (row[1].slice(-2) == "00" || row[1].slice(-2) == "30") {
  	// console.log('#'+index+' '+JSON.stringify(row));
  	var i = _.findIndex(JSONobj["days"][0].times, { time: row[1] });
	JSONobj["days"][0].times[i].trips.push({ "trip": row[0], "position": [row[2],row[3]], "short_name": row[4], "long_name": row[5], "type": row[6] })
  	console.log(JSONobj["days"][0].times[i].trips);
  };
})
.on('close', function(count){
  // when writing to a file, use the 'close' event
  // the 'end' event may fire before the file has been written
  console.log('Number of lines: '+count);
})
.on('error', function(error){
  console.log(error.message);
});