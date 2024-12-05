import socket
import threading
import time
import logging
import sys
import json
import struct

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# TODO:
# - Exit gracefully with exit command
# - Get rid of port already in use error
# - Test Leader Fail
# - Test partition scenario
# - Ask about seq_num

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

  # Ai GENERATED
  def recvall(self, sock, n):
    """Helper function to read exactly n bytes."""
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None  # Connection closed or error
        data.extend(packet)
    return bytes(data)

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
        # Read exactly 4 bytes for the length prefix
        raw_length = self.recvall(p_socket, 4)
        if not raw_length:
          break  # Connection closed or error

        # Unpack the length (big-endian unsigned integer)
        message_length = struct.unpack('>I', raw_length)[0]

        # Read the message data based on the length
        message_bytes = self.recvall(p_socket, message_length)
        if not message_bytes:
          break  # Connection closed or error

        # Decode and deserialize the JSON message
        message_str = message_bytes.decode('utf-8')
        message = json.loads(message_str)
        forward_thread = threading.Thread(target=self.forward_message, args=(message,))
        forward_thread.start()
    except Exception as e:
      logging.exception(f"Error handling client: {e}")
    finally:
      p_socket.close()
    
  # demo function, logic needs to be updated to handle multi-paxos protocol
  def forward_message(self, json_message):
    try:
      logging.debug(f"Forwarding message: {json_message}")
      src_id = json_message["ballot_number"][1]
      dest_id = json_message["dest"]
      
      if dest_id not in self.connections:
        logging.error(f"Could not connect to server: {dest_id}")
        return 

      dest_sock = self.connections[dest_id]

      if self.connection_map[src_id][dest_id]:
        time.sleep(3)
        # Convert message to JSON string and encode to bytes
        dest_msg = json.dumps(json_message).encode('utf-8')
        
        # Calculate message length and create length prefix
        message_length = len(dest_msg)
        length_prefix = struct.pack('>I', message_length)
        
        # Send length prefix followed by message
        dest_sock.sendall(length_prefix + dest_msg)
        logging.info(f"Sent message: {json_message['header']} from server {src_id if src_id != -1 else 'Network Server'} to server {dest_id}")
      else:
          logging.error(f"Failed to send message from {src_id} to {dest_id}")
              
    except json.JSONDecodeError:
        logging.error("Invalid JSON message received")
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
        kill_message = {
        "header" : "KILL",
        "message" : "",
        "ballot_number" : (-1, -1, -1),
        "dest" : nodeNum,
        "context_id" : -1
      }
        self.forward_message(kill_message)
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
