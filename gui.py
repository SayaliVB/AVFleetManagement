from flask import Flask, request, jsonify
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime, timedelta
from bson.json_util import dumps
from bson.objectid import ObjectId


app = Flask(__name__)

client = MongoClient('mongodb://localhost:27017/')
db = client['AVFleetRealTime']


#API to create trip record
@app.route('/createTrip', methods=['POST'])
def createTrip():
    try:
        collection = db["trips"]
        record = request.json
        start_time = datetime.fromisoformat(record["start_time"])

        record["start_time"] = start_time
        end_time = datetime.fromisoformat(record["end_time"]) if record["end_time"] != None else None

        record["end_time"] = end_time

        record["expiry_date"]= start_time + timedelta(days=365)
        # record["expiry_date"]= start_time + timedelta(seconds=60)
        if record:
            response_ack = collection.insert_one(record)
            print(type(response_ack))
            print(response_ack)
            return jsonify({'message': 'Trip created successfully', 'id': str(response_ack.inserted_id)}), 201
        else:
            return jsonify({'message': 'Error: No data provided'}), 400
    except PyMongoError as e:
        return jsonify({'error': str(e)}), 500



#API to get trip information
@app.route('/getCurrentTrip', methods=['GET'])
def getCurrentTrip():
    vehicle_id = request.args.get('vehicle_id')
    print(vehicle_id)
    try:
        collection = db["trips"]

        records = collection.find({'vehicle_id': vehicle_id}).sort({'start_time': -1}).limit(1) #asc: 1, desc:-1
        record_list = list(records)
        # print(record_list)
        if len(record_list)>0:
            return dumps(record_list), 200
        else:
            return jsonify({'message': 'Error: No data found for the vehicle id'}), 404
    except PyMongoError as e:
        return jsonify({'error': str(e)}), 500
    

#API to get trip information
@app.route('/getAllTrips', methods=['GET'])
def getAllTrips():
    vehicle_id = request.args.get('vehicle_id')
    print(vehicle_id)
    try:
        collection = db["trips"]

        records = collection.find({'vehicle_id': vehicle_id}).sort({'start_time': -1}) #asc: 1, desc:-1
        record_list = list(records)
        # print(record_list)
        if len(record_list)>0:
            return dumps(record_list), 200
        else:
            return jsonify({'message': 'Error: No data found for the vehicle id'}), 404
    except PyMongoError as e:
        return jsonify({'error': str(e)}), 500
    



#API to get Sensor information
@app.route('/getInfoByVehicle', methods=['GET'])
def getInfoByVehicle():
    vehicle_id = request.args.get('vehicle_id')
    print(vehicle_id)
    try:
        collection = db["trips"]
        
        #get all trip records for vehicle id
        trip_records = list(collection.find({'vehicle_id': vehicle_id}, {'_id': 1}))
        print(trip_records)
        if len(trip_records)>0 :
            trip_ids = [trip['_id'] for trip in trip_records]
            print(trip_ids)

            collection = db["vehicle_sensors"]

            #find status records with trip_ids obtained from trip records
            status_records = list(collection.find({'trip_id': {'$in': trip_ids}}))
            # print(record_list)
            if len(status_records)>0:
                return dumps(status_records), 200
            else:
                    return jsonify({'message': 'Error: No sensor data found for the vehicle id'}), 404
            
        else:
            return jsonify({'message': 'Error: No trip data found for the vehicle id'}), 404
    except PyMongoError as e:
        return jsonify({'error': str(e)}), 500


#API to get Sensor information
@app.route('/getInfoByTrip', methods=['GET'])
def getInfoByTrip():
    trip_id = ObjectId(request.args.get('trip_id'))
    print(trip_id)
    try:
        collection = db["vehicle_sensors"]

        #find sensor records with trip_ids obtained from trip records
        sensor_records = list(collection.find({'trip_id': trip_id}))
        # print(record_list)
        if len(sensor_records)>0:
            return dumps(sensor_records), 200
        else:
            return jsonify({'message': 'Error: No data found for the trip id'}), 404
    except PyMongoError as e:
        return jsonify({'error': str(e)}), 500




#API to get Sensor information
@app.route('/getRecentInfo', methods=['GET'])
def getRecentInfo():
    vehicle_id = request.args.get('vehicle_id')
    print(vehicle_id)
    try:

        collection = db["trips"]

        records = collection.find({'vehicle_id': vehicle_id}).sort({'start_time': -1}).limit(1) #asc: 1, desc:-1
        record_list = list(records)

        if len(record_list)>0:

            trip_id = record_list[0]['_id']
            collection = db["vehicle_sensors"]

            pipeline = [
                {'$match': {'trip_id': trip_id}},
                {'$sort': {'timestamp': -1}},  #bytimestamp in descending order
                {'$group': {'_id': '$sensor_id', 'latest_status': {'$first': '$$ROOT'}}}
            ]

            #execute the aggregation pipeline
            latest_sensor_statuses = list(collection.aggregate(pipeline))

            if len(latest_sensor_statuses)>0:
                latest_sensor_statuses = [status['latest_status'] for status in latest_sensor_statuses]
                return dumps(latest_sensor_statuses), 200
            else:
                return jsonify({'message': 'Error: No sensor data found for the vehicle id'}), 404
            
        else:
            return jsonify({'message': 'Error: No trip data found for the vehicle id'}), 404
    except PyMongoError as e:
        return jsonify({'error': str(e)}), 500


#API to get Vehicle status information
@app.route('/getLastStatus', methods=['GET'])
def getLastStatus():
    trip_id = ObjectId(request.args.get('trip_id'))
    print(trip_id)
    try:
        collection = db["vehicle_status"]

        #find latest status records 
        status_records = list(collection.find({'trip_id': trip_id}).sort({'timestamp': -1}).limit(1))
        # print(record_list)
        if len(status_records)>0:
            return dumps(status_records), 200
        else:
            return jsonify({'message': 'Error: No data found for the trip id'}), 404
    except PyMongoError as e:
        return jsonify({'error': str(e)}), 500



#API to get Vehicle status information
@app.route('/getStatusInfo', methods=['GET'])
def getStatusInfo():
    trip_id = ObjectId(request.args.get('trip_id'))
    print(trip_id)
    try:
        collection = db["vehicle_status"]

        #find latest status records 
        status_records = list(collection.find({'trip_id': trip_id}).sort({'timestamp': -1}))
        # print(record_list)
        if len(status_records)>0:
            return dumps(status_records), 200
        else:
            return jsonify({'message': 'Error: No data found for the trip id'}), 404
    except PyMongoError as e:
        return jsonify({'error': str(e)}), 500


'''

def Pull()):
content = request.get_json(
ViewQuery = []
PulledData = mycul. find({"Name": content ["Name" ]})
for i in PulledData:
VienQuery.append ({"Name" :i["Name"], "Score": i["Score" ]})
return jsonify(ViewQuery)



==============

document = collection.find_one({'_id': id})
    if document:
        return jsonify(document), 200
    else:
        return jsonify({'error': 'Document not found'}), 404

'''

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1',port=5001)