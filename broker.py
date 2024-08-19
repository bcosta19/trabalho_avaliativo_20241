import select
import socket
import threading
import pickle
import datetime

class Broker:
    def __init__(self, host='localhost', port=5555):
        self.host = host
        self.port = port
        self.topics = {}
        self.lock = threading.Lock()
        self.threads = []
        self.running = True

    def log(self, message):
        print(f"{datetime.datetime.now()} - {message}")

    def handle_client(self, conn, addr, stop_event):
        self.log(f"handle_client: Started handling client {addr}")
        try:
            while not stop_event.is_set():
                try:
                    readable, _, _ = select.select([conn], [], [], 1)
                    if readable:
                        data = conn.recv(1024)
                        if not data:
                            break
                        self.log(f"handle_client: Received data from {addr}")
                        message = pickle.loads(data)
                        self.process_message(message, conn, addr)
                except Exception as e:
                    print(e)
        finally:
            self.log(f"handle_client: Cleaning up connection for {addr}")
            with self.lock:
                self.cleanup_connection(conn)
            conn.close()
            stop_event.set()
        self.log(f"handle_client: Finished handling client {addr}")

    def cleanup_connection(self, conn):
        self.log("cleanup_connection: Cleaning up connections")
        for topic in list(self.topics.keys()):
            self.topics[topic] = [(c, a) for c, a in self.topics[topic] if c != conn]
        self.log("cleanup_connection: Finished cleaning up connections")

    def process_message(self, message, conn, addr):
        self.log(f"process_message: Processing message from {addr}")
        command = message['command']
        if command == 'SUBSCRIBE':
            print(command)
            topics = message['topics']
            with self.lock:
                for topic in topics:
                    if topic not in self.topics:
                        self.topics[topic] = []
                    self.topics[topic].append((conn, addr))
            self.cleanup_connections()

            response = 'SUBSCRIBE_CONFIRMATION_ACK'
            conn.send(pickle.dumps(response))
            self.print_subscribers()
        elif command == 'PUBLISH':
            print(command)
            topic = message['topic']
            msg_data = message['data']
            with self.lock:
                if topic in self.topics:
                    for subscriber_conn, _ in self.topics[topic]:
                        subscriber_conn.send(pickle.dumps({'topic': topic, 'data': msg_data, 'quit': False}))
            response = 'PUBLISH_CONFIRMATION_ACK'
            conn.send(pickle.dumps(response))
        elif command == 'QUIT':
            print(command)
            topic = message['topic']
            msg_data = message['data']
            with self.lock:
                if topic in self.topics:
                    for subscriber_conn, _ in self.topics[topic]:
                        subscriber_conn.send(pickle.dumps({'topic': topic, 'data': msg_data, 'quit': True}))
            response = 'QUIT_CONFIRMATION_ACK'
            conn.send(pickle.dumps(response))

        self.log(f"process_message: Finished processing message from {addr}")

    def cleanup_connections(self):
        self.log("cleanup_connections: Cleaning up inactive connections")
        with self.lock:
            for topic in list(self.topics.keys()):
                active_subscribers = []
                for conn, addr in self.topics[topic]:
                    if self.is_connection_active(conn):
                        active_subscribers.append((conn, addr))
                self.topics[topic] = active_subscribers
        self.log("cleanup_connections: Finished cleaning up inactive connections")

    def is_connection_active(self, conn):
        self.log("is_connection_active: Checking if connection is active")
        try:
            readable, _, _ = select.select([conn], [], [], 0)
            if readable:
                data = conn.recv(1024, socket.MSG_PEEK)
                if len(data) == 0:
                    return False
        except socket.error:
            return False
        return True

    def print_subscribers(self):
        self.log("print_subscribers: Printing current subscribers")
        with self.lock:
            print("Current Subscribers:")
            for topic, subscribers in self.topics.items():
                for conn, addr in subscribers:
                    print(f"Topic: {topic}, Subscriber: {addr}")
        self.log("print_subscribers: Finished printing current subscribers")

    def start(self):
        self.log("start: Starting broker")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind((self.host, self.port))
            server.listen()
            self.log(f"Broker listening on {self.host}:{self.port}")
            try:
                while self.running:
                    conn, addr = server.accept()
                    self.log(f"start: Accepted connection from {addr}")
                    stop_event = threading.Event()
                    thread = threading.Thread(target=self.handle_client, args=(conn, addr, stop_event))
                    thread.start()
                    self.threads.append((thread, stop_event, conn))
            except KeyboardInterrupt:
                self.running = False
            finally:
                self.cleanup()

    def cleanup(self):
        self.log("cleanup: Cleaning up broker")
        for thread, stop_event, conn in self.threads:
            stop_event.set()
            conn.close()
            thread.join()
        self.log("cleanup: Broker shutdown complete")

if __name__ == '__main__':
    broker = Broker()
    broker.start()
