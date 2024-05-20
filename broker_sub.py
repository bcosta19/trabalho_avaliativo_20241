import socket
import pickle
import sys

class Subscriber:
    def __init__(self, host='localhost', port=5555, topics=[]):
        self.host = host
        self.port = port
        self.topics = topics

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((self.host, self.port))
            message = {'command': 'SUBSCRIBE', 'topics': self.topics}
            client.send(pickle.dumps(message))
            confirmation = pickle.loads(client.recv(1024))
            if confirmation == 'SUBSCRIBE_CONFIRMATION_ACK':
                print(f"Subscribed to topics: {', '.join(self.topics)}")
            while True:
                data = client.recv(1024)
                if not data:
                    break
                message = pickle.loads(data)
                print(f"TOPIC: {message['topic']} MESSAGE: {message['data']}")

if __name__ == '__main__':
    if len(sys.argv) < 3 or sys.argv[1] != '-t':
        print("Usage: python3 broker_sub.py -t <topic1> <topic2> ... <topic_n>")
        sys.exit(1)
    topics = sys.argv[2:]
    subscriber = Subscriber(topics=topics)
    subscriber.start()

