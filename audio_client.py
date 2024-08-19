import socket
import pickle
import sys
import pyaudio

class AudioClient:
    def __init__(self, genre, my_host, my_port, broker_host='localhost', broker_port=5555):
        self.genre = genre
        self.my_host = my_host
        self.my_port = my_port
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.chunk_size = 1024
        self.audio_format = pyaudio.paInt16
        self.socket_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def publish_to_broker(self):
        message = {'command': 'PUBLISH', 'topic': self.genre, 'data': {'genre': self.genre, 'addr': (self.my_host, self.my_port)}}
        with self.socket_client as client:
            client.send(pickle.dumps(message))
            confirmation = pickle.loads(client.recv(1024))
            if confirmation == 'PUBLISH_CONFIRMATION_ACK':
                print(f"Requested streaming of {self.genre} to {self.my_host}:{self.my_port}")

    def receive_stream(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
            udp_socket.bind((self.my_host, self.my_port))
            print(f"Listening for {self.genre} stream on {self.my_host}:{self.my_port}")

            p = pyaudio.PyAudio()

            stream = p.open(format=self.audio_format, channels=2, rate=48000, output=True, frames_per_buffer=self.chunk_size)

            while True:
                try:
                    data, addr = udp_socket.recvfrom(1024)
                    if not data:
                        break
                    stream.write(data)
                except KeyboardInterrupt:
                    print("System interrupted by user")
                    stream.stop_stream()
                    stream.close()
                    p.terminate()
                    self.quit()
                    break

    def start(self):
        self.socket_client.connect((self.broker_host, self.broker_port))
        self.publish_to_broker()
        self.receive_stream()

    def quit(self):
        print('quit')
        message = {'command': 'QUIT', 'topic': self.genre, 'data': {'genre': self.genre, 'addr': (self.my_host, self.my_port)}}
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((self.broker_host, self.broker_port))
            client.send(pickle.dumps(message))
            confirmation = pickle.loads(client.recv(1024))
            if confirmation == 'QUIT_CONFIRMATION_ACK':
                print(f"Requested quiting") 
                self.socket_client.close()

if __name__ == '__main__':
    if len(sys.argv) != 5 or sys.argv[1] != '-t' or sys.argv[3] != '-m':
        print("Usage: python3 audio_client.py -t <genre> -m <ip:port>")
        sys.exit(1)
    genre = sys.argv[2]
    my_host, my_port = sys.argv[4].split(':')
    my_port = int(my_port)
    client = AudioClient(genre, my_host, my_port)
    client.start()

