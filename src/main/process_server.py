import socket
import threading
import time
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class ProcessServer:
    def __init__(self, id, target_host, target_port):
        """
        Initialize the ProcessServer with the target NetworkServer's host and port.
        """
        self.target_host = target_host
        self.target_port = target_port
        self.socket = None
        self.is_running = True
        self.server_port = self.target_port + 1 + id
        self.id = id

    def connect(self):
        """
        Establish a connection to the NetworkServer.
        """
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.bind(('localhost', self.server_port))
            self.socket.connect((self.target_host, self.target_port))
            logging.info(f"ProcessServer connected to NetworkServer at {self.target_host}:{self.target_port}")

            # Start a thread to listen for incoming messages
            listener_thread = threading.Thread(target=self.listen, daemon=True)
            listener_thread.start()

        except Exception as e:
            logging.exception(f"ProcessServer failed to connect to {self.target_host}:{self.target_port}: {e}")

    def listen(self):
        """
        Listen for incoming messages from the NetworkServer.
        """
        try:
            while self.is_running:
                data = self.socket.recv(1024)
                if data:
                    message = data.decode().strip()
                    logging.debug(f"ProcessServer received message: {message}")
                    if message == "KILL":
                        logging.info("ProcessServer received KILL message. Shutting down.")
                        self.shutdown()
                        break
                    elif message.startswith("ACK:"):
                        logging.info(f"ProcessServer received acknowledgment: {message}")
                    else:
                        logging.warning(f"ProcessServer received unknown message: {message}")
                else:
                    break
        except Exception as e:
            if self.is_running:
                logging.exception(f"ProcessServer error while listening: {e}")
        finally:
            self.socket.close()
            logging.info("ProcessServer connection closed")

    def send_message(self, dest_id, content):
        """
        Send a message to a specific destination server via the NetworkServer.
        Message format: "dest_id:content"
        """
        if not self.socket:
            logging.error("ProcessServer is not connected to any NetworkServer.")
            return

        message = f"{self.id} {dest_id} {content}"
        try:
            self.socket.sendall(message.encode())
            logging.info(f"ProcessServer sent message: {message}")
        except Exception as e:
            logging.exception(f"ProcessServer failed to send message: {e}")

    def shutdown(self):
        """
        Shutdown the ProcessServer gracefully.
        """
        self.is_running = False
        if self.socket:
            try:
                self.socket.close()
            except Exception as e:
                logging.exception(f"ProcessServer error while closing socket: {e}")
        logging.info("ProcessServer shutdown complete")

    def user_input_handler(self):
        """
        Handle user inputs to send messages.
        """
        while self.is_running:
            try:
                user_input = input("Enter command (send <dest_id> <message> or exit): ")
                tokens = user_input.strip().split()
                if not tokens:
                    continue
                command = tokens[0]
                if command == "send" and len(tokens) >= 3:
                    dest_id = int(tokens[1])
                    message = ' '.join(tokens[2:])
                    self.send_message(dest_id, message)
                elif command == "exit":
                    logging.info("ProcessServer exiting upon user request.")
                    self.shutdown()
                    break
                else:
                    print("Invalid command. Use 'send <dest_id> <message>' or 'exit'.")
            except Exception as e:
                logging.exception(f"ProcessServer error handling user input: {e}")

    def run(self):
        """
        Run the ProcessServer by connecting and starting the user input handler.
        """
        self.connect()
        self.user_input_handler()

# Example usage
if __name__ == "__main__":
    import sys

    if len(sys.argv) != 4:
        print("Usage: python process_server.py <id> <target_host> <target_port>")
        sys.exit(1)

    id = int(sys.argv[1])
    target_host = sys.argv[2]
    target_port = int(sys.argv[3])

    process_server = ProcessServer(id, target_host, target_port)
    process_server.run()
