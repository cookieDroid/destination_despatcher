import psycopg2
import paho.mqtt.client as mqtt
import json
import os
# MQTT broker credentials
broker_host = "mqtt.eclipseprojects.io"
broker_port = 1883 
topic = "elevator/events" 

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {str(rc)}")
    client.subscribe(topic)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        
        print(f"Received message: {payload}") 
        eID = int(payload.get("elevator_id", ""))
        eType = payload.get("event_type", "")
        flr = int(payload.get("floor", ""))
        dir = payload.get("direction", "")
        stat = payload.get("status", "")
        errMsg = payload.get("error_message", "")
        log_elevator_event(eID, eType, flr, dir, stat, errMsg) 
    except Exception as e:
        print(f"Error processing message: {e}")

def log_elevator_event(elevator_id, event_type, floor, direction, status, error_message=None):
    """
    Logs elevator events to PostgreSQL database.

    Args:
        elevator_id: ID of the elevator
        event_type: Type of event (e.g., "request", "arrival", "door_open", "door_close", "movement")
        floor: Current floor of the elevator
        direction: Direction of movement ("up", "down", "idle")
        status: Status of the event ("success", "failed")
        error_message: Optional error message if status is "failed"
    """
    try:
        postgres_url = os.environ['POSTGRES_CREDENTIAL']
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()

        # Create table if it doesn't exist
        # cur.execute("""
        #     CREATE TABLE IF NOT EXISTS elevator_events (
        #         timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        #         elevator_id INTEGER,
        #         event_type TEXT,
        #         floor INTEGER,
        #         direction TEXT,
        #         status TEXT,
        #         error_message TEXT
        #     )
        # """)

        # Insert event data
        cur.execute("""
            INSERT INTO elevator_events (elevator_id, event_type, floor, direction, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (elevator_id, event_type, floor, direction, status, error_message))

        conn.commit()
        cur.close()
        conn.close()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

# Create an MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
client.connect(broker_host, broker_port, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()