import select
import socket
import threading
import pickle

class Broker:
    def __init__(self, host='localhost', port=5555):
        self.host = host
        self.port = port
        self.topics = {}
        self.lock = threading.Lock()
        self.threads = []
        self.running = True

    def handle_client(self, conn, addr, stop_event):
        try:
            while not stop_event.is_set():
                try:
                    readable, _, _ = select.select([conn], [], [], 1)
                    if readable:
                        data = conn.recv(1024)
                        if not data:
                            break
                        message = pickle.loads(data)
                        self.process_message(message, conn, addr)
                except ConnectionResetError:
                    pass

        finally:
            with self.lock:
                self.cleanup_connection(conn)
            conn.close()
            stop_event.set()

    def cleanup_connection(self, conn):
        for topic in list(self.topics.keys()):
            self.topics[topic] = [(c, a) for c, a in self.topics[topic] if c != conn]

    def process_message(self, message, conn, addr):
        command = message['command']
        if command == 'SUBSCRIBE':
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
            topic = message['topic']
            msg_data = message['data']
            with self.lock:
                if topic in self.topics:
                    for subscriber_conn, _ in self.topics[topic]:
                        subscriber_conn.send(pickle.dumps({'topic': topic, 'data': msg_data}))
            response = 'PUBLISH_CONFIRMATION_ACK'
            conn.send(pickle.dumps(response))

    def cleanup_connections(self):
        with self.lock:
            for topic in list(self.topics.keys()):
                active_subscribers = []
                for conn, addr in self.topics[topic]:
                    if self.is_connection_active(conn):
                        active_subscribers.append((conn, addr))
                self.topics[topic] = active_subscribers

    def is_connection_active(self, conn):
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
        with self.lock:
            print("Current Subscribers:")
            for topic, subscribers in self.topics.items():
                for conn, addr in subscribers:
                    print(f"Topic: {topic}, Subscriber: {addr}")

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind((self.host, self.port))
            server.listen()
            print(f"Broker listening on {self.host}:{self.port}")
            try:
                while self.running:
                    conn, addr = server.accept()
                    stop_event = threading.Event()
                    thread = threading.Thread(target=self.handle_client, args=(conn, addr, stop_event))
                    thread.start()
                    self.threads.append((thread, stop_event, conn))
            except KeyboardInterrupt:
                self.running = False
            finally:
                self.cleanup()

    def cleanup(self):
        for thread, stop_event, conn in self.threads:
            stop_event.set()
            conn.close()
            thread.join()
        print("Broker shutdown complete")

if __name__ == '__main__':
    broker = Broker()
    broker.start()

