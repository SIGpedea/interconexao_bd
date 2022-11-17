from google.transit import gtfs_realtime_pb2
from psycopg2 import sql
import time
import psycopg2
import requests

hostname = 'localhost'
username = 'postgres'
password = 'admin'
database = 'sydney_transport'

transport = 'ferries'
COUNT = 0

def doQuery(conn, query, data):
    cur = conn.cursor()
    return cur.execute(query, data)

def getPositions(req):
    global COUNT
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(req.content)
    connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    doQuery(connection, """BEGIN""", None)
    for entity in feed.entity:
        query = "INSERT INTO {} (entity_id, trip_id, vehicle_id, label, time_text, time_posix, latitude, longitude, bearing, speed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        data = (str(entity.id), str(entity.vehicle.trip.trip_id), str(entity.vehicle.vehicle.id),
            str(entity.vehicle.vehicle.label), str(time.ctime(int(entity.vehicle.timestamp))),
            str(entity.vehicle.timestamp), str(entity.vehicle.position.latitude),
            str(entity.vehicle.position.longitude), str(entity.vehicle.position.bearing),
            str(entity.vehicle.position.speed))
        print(doQuery(connection, sql.SQL(query).format(sql.Identifier(transport)), data))
    doQuery(connection, "SELECT deleteduplicate();", None)
    doQuery(connection, sql.SQL("SELECT createGistIndex('{}');").format(sql.Identifier(transport)), None)

    if COUNT % 50 == 0:
        doQuery(connection, sql.SQL("SELECT createGistIndex('{}');").format(sql.Identifier(transport + '_hist')), None)
        doQuery(connection, """COMMIT""", None)
        doQuery(connection, sql.SQL("VACUUM ANALYZE {};").format(sql.Identifier(transport + '_hist')), None)
    else:
        doQuery(connection, """COMMIT""", None)
    print('Iteration : ' + str(COUNT))
    COUNT += 1

while True:
    try:
        req = requests.get(
            "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/metro",
             headers={
            "Accept": "application/x-google-protobuf",
            "Authorization": "apikey G2j8KhTM3WJBztVuTCLlUsGzkPtE36hucASZ"
          } )
        getPositions(req)
    except Exception as e:
     print(e)
    time.sleep(10.0)
