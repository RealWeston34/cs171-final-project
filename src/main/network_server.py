import socket
import threading
import time
import logging
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# High level overview:
# Need to establish a connection to each server
# Need to start threads for all messages coming from the server
# When the message is received and decoded, send reply back to the server, have a 3 second delay before sending to simulate network conditions
# Simulate connections failing using a dictionary: connection_map
# By default, when a server is connected, connection_map[server] will be set to 1
# When starting the server, have a main thread that handles all user facing inputs:
# - failLink <src> <dest>, set connection_map[src][dest] = 0
# - fixLink <src> <dest>, set connection_map[src][dest] = 1
# - failNode <nodeNum>, need to send a message to nodeNum that result in a kill protocol, potentially reboot afterwards.
# Use a Lock when dealing with connection_map so there are no mutex errors
# Optional: Add error handling for when connections actually fail

class NetworkServer:
  def __init__(self, base_port, num_servers):
    self.connection_map = [[False for _ in range(num_servers)] for _ in range(num_servers)]
    self.server_port = base_port
    self.cur_leader = 0
    self.server_socket = None
    self.is_running = True
    self.connection_lock = threading.Lock()
    self.connections = {} # keep track of all TCP connections, node_num --> socket
    logging.info("Successfully initialized network server")
  
  def get_server_id(self, addr):
    if(len(addr) != 2):
      return -1
    
    port = addr[1]
    return port - self.server_port - 1 

  def start_server(self):
    logging.info("Starting server...")
    self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server_socket.bind(('localhost', self.server_port))
    self.server_socket.listen(5)

    threading.Thread(target=self.accept_connections, daemon=True).start()
    threading.Thread(target=self.user_input_handler).start() # main thread
  
  def add_connections(self, src_id):
    for dest_id in self.connections.keys():
      with self.connection_lock:
        self.connection_map[src_id][dest_id] = True
        self.connection_map[dest_id][src_id] = True
  
  def accept_connections(self):
    while self.is_running:
      try:
        client_socket, addr = self.server_socket.accept()
        server_id = self.get_server_id(addr)
        if server_id == -1:
          logging.error(f"Could not get server id for connection with address: {addr}")
          continue

        self.add_connections(server_id)
        self.connections[server_id] = client_socket
        logging.debug(f"updating connections dictionary: {self.connections}")
        handler_thread = threading.Thread(target=self.handle_process, args=(client_socket,), daemon=True)
        handler_thread.start()
      except Exception as e:
        if self.is_running:
          logging.exception(f"Error accepting connections: {e}")
  
  def handle_process(self, p_socket):
    logging.debug("Handling the process in a new thread")
    try:
      while self.is_running:
        data = p_socket.recv(1024)
        if data:
          message = data.decode().strip()
          forward_thread = threading.Thread(target=self.forward_message, args=(p_socket, message,))
          forward_thread.start()
        else:
          break
    except Exception as e:
      logging.exception(f"Error handling client: {e}")
    finally:
      p_socket.close()
  
  # demo function, logic needs to be updated to handle multi-paxos protocol
  def forward_message(self, p_socket, message):

    logging.debug(f"Forwarding message: {message}")

    try:
      n_message = message.strip().split(" ")

      # more advanced parsing needed for actual function
      if len(n_message) != 3:
        print("Invalid message received")
        return 

      src_id = int(n_message[0])
      dest_id = int(n_message[1])
      content = n_message[2]

      if dest_id not in self.connections:
        logging.error(f"Could not connect to server: {dest_id}")
        return 

      dest_sock = self.connections[dest_id]
      if self.connection_map[src_id][dest_id]:
        time.sleep(3)
        dest_sock.sendall(content.encode())
        logging.info(f"Sent message: {content} from server {src_id} to server {dest_id}")
      else:
        logging.error(f"Failed to send message '{content}' from {src_id} to {dest_id}")
    except Exception as e:
      logging.exception(f"Error sending reply: {e}")

  def connect_to_node(self, server_id, port):
    try:
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      sock.connect(('localhost', port))
      self.connections[server_id] = sock
      logging.info(f"Connected to server {server_id} on port {port}")
    except Exception as e:
      logging.exception(f"Failed to connect to server {server_id} on port {port}: {e}")

  def user_input_handler(self):
    while self.is_running:
      try:
        user_input = input()
        tokens = user_input.strip().split()
        if not tokens:
          continue
        command = tokens[0]
        if command == "failLink" and len(tokens) == 3:
          src = int(tokens[1])
          dest = int(tokens[2])
          self.failLink(src, dest)
          logging.info(f"Link between {src} and {dest} failed")
        elif command == "fixLink" and len(tokens) == 3:
          src = int(tokens[1])
          dest = int(tokens[2])
          self.fixLink(src, dest)
          logging.info(f"Link between {src} and {dest} fixed")
        elif command == "failNode" and len(tokens) == 2:
          node_num = int(tokens[1])
          self.failNode(node_num)
          logging.info(f"Node {node_num} failed")
        else:
          logging.warning("Invalid command")
      except Exception as e:
        logging.exception(f"Error handling user input: {e}")

  def failLink(self, src, dest):
    with self.connection_lock:
      self.connection_map[src][dest] = False
      self.connection_map[dest][src] = False

  def fixLink(self, src, dest):
    with self.connection_lock:
      self.connection_map[src][dest] = True
      self.connection_map[dest][src] = True

  def failNode(self, nodeNum):
    with self.connection_lock:
      if nodeNum not in self.connections:
        logging.error(f"No connection to node {nodeNum}")
        return

      try:
        kill_message = "KILL"
        self.connections[nodeNum].sendall(kill_message.encode())
        logging.info(f"Sent KILL message to node {nodeNum}")
        self.connections[nodeNum].close()
        del self.connections[nodeNum]
      except Exception as e:
        logging.exception(f"Error sending KILL message to node {nodeNum}: {e}")

# Example usage
if __name__ == "__main__":
  if len(sys.argv) != 3:
    print("Usage: python process_server.py <base_port> <num_servers>")
    sys.exit(1)

  base_port = int(sys.argv[1])
  target_port = int(sys.argv[2])

  network_server = NetworkServer(base_port, target_port)
  network_server.start_server()


