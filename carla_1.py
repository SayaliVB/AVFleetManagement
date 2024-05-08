from datetime import datetime
import time
import requests
import json
from flask import Flask

app = Flask(__name__)

api_ip = '127.0.0.1:5000'
#api_ip='54.193.129.173:5000'

trip_mapping = {"4a77d76cab9beed398389f2203e85e0bb852271d9e035cc957e6ab5b9bbd75d1": "", "ef0bf2004d81b3b27d0ec5e5d7e5477f6f1205e44e44145c7e16d9fb2de0040e": ""}

@app.route("/createTrip")
def createTrip():
    # Endpoint of the Flask API
    url = f'http://{api_ip}/createTrip'

    # Headers to indicate JSON data
    headers = {'Content-Type': 'application/json'}

    data={
        "vehicle_id": "2",
        "source": { "type": "Point", "coordinates": [45.1222, 122.4222] },
        "destination": { "type": "Point", "coordinates": [36.877, 112.655] },
        "occupancy": 3,
        "completed": False,
        "start_time": "2024-05-07T08:12:00Z",
        "end_time": None
    }

    response = requests.post(url, data=json.dumps(data), headers=headers)
    if response.status_code == 201:
        # Load JSON data from response
        resp_data = response.json()
        # print(data)
        print(f"Create for trip_id {resp_data['id']}")
    if(data['vehicle_id'] =="2"):
        print("in here")
        trip_mapping['4a77d76cab9beed398389f2203e85e0bb852271d9e035cc957e6ab5b9bbd75d1'] = resp_data['id']
        print(trip_mapping)
    elif(data['vehicle_id'] =="1"):
        print("in here1")
        trip_mapping['ef0bf2004d81b3b27d0ec5e5d7e5477f6f1205e44e44145c7e16d9fb2de0040e'] = resp_data['id']
        print(trip_mapping)
    else:
        trip_mapping[''] = resp_data['id']
    return resp_data['id']

def createTimeStamp(time_str):
    # Get current date
    today = datetime.today().date()

    # Parse the time string to a time object
    hours, minutes, seconds = map(int, time_str.split(':'))
    time_of_day = datetime(today.year, today.month, today.day, hours, minutes, seconds)

    # Convert to ISO 8601 format
    iso_format = time_of_day.isoformat()

    return iso_format

@app.route("/sendTripStatus")
def sendTripStatus():
    createTrip()
    # Load data from JSON file
    with open('SensorDataLog5.json', 'r') as file:
        all_data = json.load(file)
        records = all_data["records"]

    # Endpoint of the Flask API
    url = f'http://{api_ip}/createStatus'

    # Headers to indicate JSON data
    headers = {'Content-Type': 'application/json'}
    i=0
    responsestr =""
    # Iterate over each record and send it as a separate POST request
    for data in records:
        # Convert timestamp into ISO format for mongo
        timestamp = createTimeStamp(data['Simulation time'])
        data['timestamp'] = timestamp
        # print("Trip mapping dictionarty: ",trip_mapping)
        data["trip_id"] = trip_mapping[data["Unique_id"]]

        print("tripid: ", data["trip_id"])

        del data['Simulation time']

        latitude = data["Location"]["Y"]  # This is hypothetical and would need actual logic based on your system's geospatial mapping
        longitude = data["Location"]["X"]  # Hypothetical mapping

        # Modify the data structure to include latitude and longitude
        data["location_lat"] = latitude
        data["location_long"] = longitude
        del data['Location']

        data["speed"] = data.pop("Speed (km/h)")

        response = requests.post(url, data=json.dumps(data), headers=headers)
        if response.status_code == 201:
            print(f"Sent data for trip_id {data['trip_id']}, response: {response.text}")
            responsestr +=f"Sent data for trip_id {data['trip_id']}, response: {response.text} <br>"
            time.sleep(1)
        # i+=1
        # if i >60:
        #     break
    endTrip("2")
    return responsestr


@app.route("/endTrip/<vehicle_id>")
def endTrip(vehicle_id):
    # Endpoint of the Flask API
    url = f'http://{api_ip}/endTrip'

    data={
        "trip_id":"66286e1fed4328b545fa9705",
        "completed": True,
        "end_time": "2024-05-07T08:42:00Z"
    }

    # Headers to indicate JSON data
    headers = {'Content-Type': 'application/json'}
    if(vehicle_id =="2"):
        print("end in here 1")
        data['trip_id'] = trip_mapping['4a77d76cab9beed398389f2203e85e0bb852271d9e035cc957e6ab5b9bbd75d1']
        print(trip_mapping)
    elif(vehicle_id =="1"):
        print("end in here 2")
        data['trip_id'] = trip_mapping['ef0bf2004d81b3b27d0ec5e5d7e5477f6f1205e44e44145c7e16d9fb2de0040e']
        print(trip_mapping)
    else:
        data['trip_id'] = "663b1d380aeb0fe08c8c1b7e"

    

    response = requests.post(url, data=json.dumps(data), headers=headers)
    if response.status_code == 201:
        # Load JSON data from response
        resp_data = response.json()
        # print(data)
        print(f"End for trip_id {data['trip_id']}")
    
    return data['trip_id']


if __name__ == '__main__':
   
   app.run(host='127.0.0.1',port=5002,debug=True)