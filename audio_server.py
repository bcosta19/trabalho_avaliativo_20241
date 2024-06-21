import socket
import threading
import pickle
import time
from pydub import AudioSegment
from pydub.utils import make_chunks

class AudioServer:
    def __init__(self, broker_host='localhost', broker_port=5555, audio_port=5000):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.audio_port = audio_port
        self.genres = {'ROCK': 'Saturday Night.mp3', 'JAZZ': 'music_jazz.mp3', 'CLASSICAL': 'music_classical.mp3'}
        self.clients = {genre: [] for genre in self.genres}
        self.lock = threading.Lock()

    def subscribe_to_broker(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((self.broker_host, self.broker_port))
            message = {'command': 'SUBSCRIBE', 'topics': list(self.genres.keys())}
            client.send(pickle.dumps(message))
            confirmation = pickle.loads(client.recv(1024))
            if confirmation == 'SUBSCRIBE_CONFIRMATION_ACK':
                print(f"Subscribed to broker for genres: {', '.join(self.genres.keys())}")
            while True:
                data = client.recv(1024)
                if not data:
                    break
                message = pickle.loads(data)
                if message['topic'] in self.genres:
                    self.handle_client_request(message['data'])

    def handle_client_request(self, data):
        genre = data['genre']
        client_addr = data['addr']
        with self.lock:
            if client_addr not in self.clients[genre]:
                self.clients[genre].append(client_addr)
                print(f"New client for {genre}: {client_addr}")
                if len(self.clients[genre]) == 1:
                    threading.Thread(target=self.stream_audio, args=(genre,)).start()

    def stream_audio(self, genre):
        filename = self.genres[genre]
        audio = AudioSegment.from_file(filename, format="mp3")
        chunk_length_ms = 5000  # Length of each chunk in milliseconds
        chunks = make_chunks(audio, chunk_length_ms)  # Split audio into chunks of 50ms

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
            for chunk in chunks:
                i = 0
                with self.lock:
                    if not self.clients[genre]:
                        break
                    data = chunk.raw_data
                    print(len(data))
                    max_packet_size = 1024 
                    for i in range(0, len(data), max_packet_size):
                        packet = data[i:i+max_packet_size]
                        for addr in self.clients[genre]:
                            udp_socket.sendto(packet, addr)
                        time.sleep(chunk.duration_seconds / 1000)

    def start(self):
        threading.Thread(target=self.subscribe_to_broker).start()

if __name__ == '__main__':
    server = AudioServer()
    server.start()
