import socket
import pickle
import sys

class Publisher:
    def __init__(self, host='localhost', port=5555):
        self.host = host
        self.port = port

    def publish(self, topic, data):
        message = {'command': 'PUBLISH', 'topic': topic, 'data': data}
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((self.host, self.port))
            client.send(pickle.dumps(message))
            confirmation = pickle.loads(client.recv(1024))
            if confirmation == 'PUBLISH_CONFIRMATION_ACK':
                print(f"Message published to topic: {topic}")

if __name__ == '__main__':
    if len(sys.argv) != 5 or sys.argv[1] != '-t' or sys.argv[3] != '-m':
        print("Usage: python3 broker_pub.py -t <topic> -m <message>")
        sys.exit(1)
    topic = sys.argv[2]
    message = sys.argv[4]
    publisher = Publisher()
    publisher.publish(topic, message)

