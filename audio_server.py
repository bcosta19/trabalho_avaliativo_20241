import socket
import threading
import pickle
import time
from pydub import AudioSegment
from pydub.utils import make_chunks, mediainfo

class AudioServer:
    def __init__(self, broker_host='localhost', broker_port=5555, audio_port=5000):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.audio_port = audio_port
        self.genres = {'ROCK': 'Saturday Night.mp3', 'POP': "Baby, I'm Back.mp3", 'CLASSICAL': "Chopin - Nocturne op9 No2.mp3"}
        self.clients = {genre: [] for genre in self.genres}
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect((self.broker_host, self.broker_port))

    def subscribe_to_broker(self):
        with self.client as client:
            message = {'command': 'SUBSCRIBE', 'topics': list(self.genres.keys())}
            client.send(pickle.dumps(message))
            confirmation = pickle.loads(client.recv(1024))
            if confirmation == 'SUBSCRIBE_CONFIRMATION_ACK':
                print(f"Subscribed to broker for genres: {', '.join(self.genres.keys())}")
            while True:
                print('entrada subscribe_to_broker')
                print(self.clients)
                data = client.recv(1024)
                if not data:
                    break
                message = pickle.loads(data)

                
                print(message['quit'])
                if message['quit'] == True:
                    data = message['data']
                    genre = data['genre']
                    client_addr = data['addr']
                    self.clients[genre].remove(client_addr)
                    print(self.clients)
                    continue

                if message['topic'] in self.genres:
                    self.handle_client_request(message['data'])


            print('saida subscribe_to_broker')


    def verify_connections(self, addr):
        for genre in self.clients:
            if addr in self.clients[genre]:
                return True
        return False

    def handle_client_request(self, data):
        genre = data['genre']
        client_addr = data['addr']
        if self.verify_connections(client_addr):

            print(f"Address {client_addr} is already in use")
            print('saida handle_client_request')
            return

        if client_addr not in self.clients[genre]:
            self.clients[genre].append(client_addr)
            print(f"New client for {genre}: {client_addr}")
            if len(self.clients[genre]) == 1:
                threading.Thread(target=self.stream_audio, args=(genre,)).start()

                print("nova thread criada")
        print('saida handle_client_request')

    def stream_audio(self, genre):
        filename = self.genres[genre]
        info = mediainfo(filename)
        audio = AudioSegment.from_file(filename, format=info['format_name'])
        info = {'sample_rate': info['sample_rate']}


        chunk_length_ms = 5000
        chunks = make_chunks(audio, chunk_length_ms)

        print(self.clients)

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:

            for chunk in chunks:
                i = 0
                if not self.clients[genre]:
                    break
                data = chunk.raw_data
                max_packet_size = 1024 


                for i in range(0, len(data), max_packet_size):
                    packet = data[i:i+max_packet_size]
                    for addr in self.clients[genre]:
                        udp_socket.sendto(packet, addr)
                    time.sleep(chunk.duration_seconds / 1000)

            for addr in self.clients[genre]:
                self.remove_client(addr, genre)

    def remove_client(self, addr_client, genre):
        self.clients[genre].remove(addr_client)

    def start(self):
        threading.Thread(target=self.subscribe_to_broker).start()

if __name__ == '__main__':
    server = AudioServer()
    server.start()
