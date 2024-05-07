from flask import Flask, request, jsonify
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime, timedelta
from bson.objectid import ObjectId

app = Flask(__name__)

# client = MongoClient('mongodb://localhost:27017/')
client = MongoClient('mongodb+srv://sayali:k8qfDNzKHE5JOqt5@avfleetcluster.p0ttflr.mongodb.net/?retryWrites=true&w=majority&appName=AVFleetCluster')

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


#API to create sensor info record
@app.route('/createSensor', methods=['POST'])
def createSensor():
    try:
        collection = db["vehicle_sensors"]
        record = request.json

        timestamp_obj = datetime.fromisoformat(record["timestamp"])

        record["timestamp"] = timestamp_obj

        trip_id = ObjectId(record["trip_id"])

        record["trip_id"] = trip_id

        '''
        # Check if index exists
        index_info = collection.index_information()
        if "expiry_date_1" in index_info:
            collection.create_index("expiry_timestamp", expireAfterSeconds=0)
            print("Index already exists on 'expiry_date' field")
        else:
            print("Index does not exist on 'expiry_date' field")
        '''

        record["expiry_date"]= timestamp_obj + timedelta(days=365)

        if record:
            response_ack = collection.insert_one(record)
            print(type(response_ack))
            print(response_ack)
            return jsonify({'message': 'Success'}), 201
        else:
            return jsonify({'message': 'Error: No data provided'}), 400
    except PyMongoError as e:
        return jsonify({'error': str(e)}), 500


#API to create status record
@app.route('/createStatus', methods=['POST'])
def createStatus():
    try:
        collection = db["vehicle_status"]
        record = request.json
        # print(type(record))
        timestamp_obj = datetime.fromisoformat(record["timestamp"])
        # print(timestamp_obj)

        record["timestamp"] = timestamp_obj
        # print(record)

        trip_id = ObjectId(record["trip_id"])
        print(trip_id)

        record["trip_id"] = trip_id
        # print(record)
        record["expiry_date"]= timestamp_obj + timedelta(days=365)
        
        if record:
            response_ack = collection.insert_one(record)
            print(type(response_ack))
            print(response_ack)
            return jsonify({'message': 'Success'}), 201
        else:
            return jsonify({'message': 'Error: No data provided'}), 400
    except PyMongoError as e:
        return jsonify({'error': str(e)}), 500



#API to add end time to trip record
@app.route('/endTrip', methods=['POST'])
def endTrip():
    try:
        collection = db["trips"]
        record = request.json

        end_time = datetime.fromisoformat(record["end_time"]) if record["end_time"] != None else None

        record["end_time"] = end_time

        trip_id = ObjectId(record["trip_id"])

        del record["trip_id"]
        print(trip_id)
        print(record)
        if record:
            ended_trip = collection.update_one({'_id': trip_id}, {'$set': record})
            if ended_trip.modified_count > 0:
                return jsonify({'message': 'Trip ended successfully'}), 200
            else:
                return jsonify({'error': 'Trip not found'}), 400
        else:
            return jsonify({'error': 'No data provided'}), 400
    except PyMongoError as e:
        return jsonify({'error': str(e)}), 500



if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1',port=5000)
