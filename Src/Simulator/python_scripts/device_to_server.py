import paho.mqtt.client as mqtt
import json
import time
import random

# MQTT broker configuration
broker_host = "mqtt.eclipseprojects.io"
broker_port = 1883
topic = "elevator/events"

# Sample elevator event data
def generate_event_data():
    elevators = 3
    floors = list(range(1, 3))
    event_types = ["request", "arrival", "door_open", "door_close", "movement"]
    directions = ["up", "down", "idle"]
    error_messages = [
        "Door sensor malfunction",
        "Motor overload",
        "Communication error",
        "Obstacle detected",
        "Emergency stop"
    ]

    elevator_id = random.randint(1, elevators)
    event_type = random.choice(event_types)
    floor = random.choice(floors)
    direction = random.choice(directions)
    status = "success" if random.random() > 0.1 else "failed"
    error_message = random.choice(error_messages) if status == "failed" else None

    return {
        "elevator_id": elevator_id,
        "event_type": event_type,
        "floor": floor,
        "direction": direction,
        "status": status,
        "error_message": error_message
    }

# Callback function for successful connection
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print(f"Failed to connect, return code {str(rc)}")

# Callback function for receiving messages
def on_message(client, userdata, msg):
    print(f"Received message: {msg.payload.decode()}")

# Create an MQTT client instance
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
client.connect(broker_host, broker_port, 60)

# Publish sample events
while True:
    event_data = generate_event_data()
    json_data = json.dumps(event_data)
    client.publish(topic, json_data)
    print(f"Published message: {json_data}")
    time.sleep(1)  # Publish an event every second

# Start the MQTT loop
client.loop_forever()