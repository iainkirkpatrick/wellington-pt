import csv
import json
import io
from datetime import datetime
from datetime import timedelta

#index finding func, imitates lo-dash _.findIndex
def findIndex(lst, key, value):
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return i
            break

#build JSON structure
# {
#     "days": [
#         {
#             "day": "monday",
#             "times": [
#                 {
#                     "time": '00:00:00',
#                     "trips": [
#                         {
#                             "trip": 2323,
#                             "position": [
#                                 "lat",
#                                 "lon"
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
# days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
days = ["monday"]
JSONobj = { "days": [] }
i = 0
for day in days:
	print day
	JSONobj["days"].append({"day":day, "times":[]})

	FMT = '%H:%M:%S'
	per30time = datetime.strptime('00:00:00', FMT)
	j = 0
	while j < 2880: #that's how many 30 second periods there are in a day, beginning at midnight
		t = per30time.strftime(FMT)
		print t
		JSONobj["days"][i]["times"].append({"time":t, "trips":[]})
		per30time += timedelta(seconds=30)
		j += 1
	i+=1

#iterate over csv, find times that end in :00 or :30, stick the data in JSONobj
with open('../results/monday.csv', 'rb') as csvfile:
	reader = csv.reader(csvfile, delimiter=',')
	i=0
	for row in reader:
		# print row[6]
		if row[1][-2:] == "00" or row[1][-2:] == "30":
			# print row
			# ind = next(index for (index, d) in enumerate(JSONobj["days"][0]["times"]) if d["time"] == row[1])
			ind = findIndex(JSONobj["days"][0]["times"], "time", row[1])
			print i
			JSONobj["days"][0]["times"][ind]["trips"].append({ "trip": row[0], "position": [row[2],row[3]], "short_name": row[4].strip("'u"), "long_name": row[5].strip("'u").strip('"'), "type": row[6].strip("'u") })
		i+=1

#write JSONobj to file
with io.open('../results/mondayper30.json', 'w', encoding='utf-8') as f:
  f.write(unicode(json.dumps(JSONobj, ensure_ascii=False)))

print "finshed"

