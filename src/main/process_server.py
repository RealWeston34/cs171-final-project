import socket
import threading
import time
import logging
import collections
import json
import math

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
    self.op = 0
    self.id = id
    self.seq_num = id # to ensure uniqueness, can only add a multiple of self.num_nodes to it
    self.leader = -1 # keep track of the current leader in multi paxos
    self.num_nodes = 3 # maybe pass as an arg ?
    self.majority = (self.num_nodes // 2) + 1
    self.contexts = {} # replicated dictionary
    self.contexts_lock = threading.Lock()
    
    # Logic is once greater than majority, will send decide and then reset to 0, this assumes one accept at a time, which aligns with multi paxos assumptions
    self.accepted_num = 0
    # self.accepted_lock = threading.Lock()
    self.accepted_condition = threading.Condition()
    self.pending_operations = collections.deque()
    
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
          message = json.loads(data.decode('utf-8'))
          header = message["header"]
          src = message["ballot_number"][1]
          logging.debug(f"ProcessServer received message: {message}")
          if header == "KILL":
            logging.info("ProcessServer received KILL message. Shutting down.")
            self.shutdown()
            break
          elif header == "ACCEPT":
            logging.info(f"ProcessServer received message: {header} from {src}")
            op_num = message["ballot_number"][0]
            response_thread = threading.Thread(target=self.send_response, args=("ACCEPTED", src, op_num,), daemon=True)
            response_thread.start()
          elif header == "ACCEPTED":
            logging.info(f"ProcessServer received message: {header} from {src}")
            # maybe start in a new thread?
            with self.accepted_condition:
              self.accepted_num += 1
              self.accepted_condition.notify_all()
          elif header == "DECIDE":
            # create new decide function
            logging.info(f"ProcessServer received message: {header} from {src}")
            msg = message["message"]
            decide_thread = threading.Thread(target=self.send_message, args=("DECIDE", msg,), daemon=True)
            decide_thread.start()
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
  
  # Assumptions in multipaxos:
  # Each proccess only proposes one message as a time ?
  # - Maybe add a system to prevent this 
  # - I think it is added to a queue
  # - synchronous loop
  
  # Sending messages
  # Use as skeleton for other operations
  # Eventually will want to have a thread that continuosly checks the queue for operations
  # When the user inputs a command, then it will be added to this queue rather than handled by its own thread
  # In the listen thread, it will also be added to the queue
  # In the 'handle_operation' thead, it will bascially do operations using multi paxos.
  
  
  def send_message(self, header, message):
    for node in range(self.num_nodes):
        if node == self.id:
          continue
        
        message = {
          "header" : header,
          "message" : message,
          "ballot_number" : (self.op, self.id, self.seq_num),
          "dest" : node
        }
        
        # Maybe consider having a send lock if this becomes a problem
        self.socket.sendall(json.dumps(message).encode('utf-8'))
    
  def reach_consensus(self, context_id):
    msg = f"create_content: {context_id}"
    # Send PROPOSE message if not leader:
    # if self.leader != self.id:
    #   self.send_message(header="PROPOSE", message=msg)
    #   self.accepted_condition.wait_for(lambda: self.accepted_num >= self.majority)
    #   self.accepted_num = 0
    #   self.leader = self.id
      
    # Send ACCEPT message:
    self.send_message(header="ACCEPT", message=msg)
    self.accepted_condition.wait_for(lambda: self.accepted_num >= self.majority)
    self.accepted_num = 0
  
    # Send DECIDE message:
    self.send_message(header="DECIDE", message=msg)
    
    
      
      
      
    # initialize context_id to an empty string
    # Send to other servers through multi paxos
    # - If not leader, send proposal
    # - If leader, assume replication phase and send accept messages
    # - The network server will forward the message to all other servers
    # - The network server will forward the response to the server
    # - process server will have a local variable for number of response
    # - Use a lock to update the value of this variable
    # - Once local variable is a majority, then decide 
  
  # For ACCEPT message
  def send_response(self, header, dest, op_num):
    if self.op_num > op_num:
      return
    
    message = {
      "header" : header,
      "message" : message,
      "ballot_number" : (self.op, self.id, self.seq_num),
      "dest" : dest
    }
    
    # Maybe consider having a send lock if this becomes a problem
    self.socket.sendall(json.dumps(message).encode('utf-8'))

  def broadcast_accept(self):
    pass
    

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

#   7.1 Expected Server Input
# Each node should support the following operations.
# • create <context ID>: create a new context with a context ID.
# • query <context ID> <query string>: query the LLM on a context ID with a query
# string>.
# 6
# • choose <context ID> <response number>: select an LLM response based equal to the
# server ID that responded on a context ID.
# • view <context ID>: prints a context of a thread of discussion with LLM.
# • viewall: prints all contexts of discussion with the LLM.
#
# Logic: handle each command in its own thread
# Structure it according to the multi-paxos protocol

  def user_input_handler(self):
    """
    Handle user inputs to send messages.
    """
    while self.is_running:
      try:
        user_input = input("Enter command: ")
        tokens = user_input.strip().split()
        if not tokens:
          continue
        command = tokens[0]
        
        if command == "create" and len(tokens) == 2 and tokens[1].isdigit():
          context_id = tokens[1]
          # start create context thread
        elif command == "query" and len(tokens) == 3 and tokens[1].isdigit():
          context_id = tokens[1]
          # start query thread
        elif command == "choose" and len(tokens) == 3 and tokens[1].isdigit() and tokens[2].isdigit():
          context_id = tokens[1]
          repsonse_number = tokens[2]
        elif command == "view" and len(tokens) == 2 and tokens[1].isdigit():
          # start view context thread
          pass
        elif command == "viewall":
          pass
          # start view all thread
        elif command == "exit":
          logging.info("ProcessServer exiting upon user request.")
          self.shutdown()
          break
        else:
          print("Invalid command.")
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
