import socket
import threading
import time
import logging
import collections
import json
import math
import os
import struct
from llm_service import LLMService

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
    self.collected_responses = {}  # context_id -> {server_id -> response}
    
    # Logic is once greater than majority, will send decide and then reset to 0, this assumes one accept at a time, which aligns with multi paxos assumptions
    self.accepted_num = 0
    # self.accepted_lock = threading.Lock()
    self.accepted_condition = threading.Condition()
    self.pending_operations = collections.deque()
    self.send_lock = threading.Lock()

    # Initialize the LLM service
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        raise EnvironmentError("Please set GEMINI_API_KEY environment variable")
    self.service = LLMService(api_key)
    
    logging.info(f"Successfully initialized processer server {self.id}")
    
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
      listener_thread = threading.Thread(target=self.listen)
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
            content = message["message"]
            logging.debug("Starting response thread for ACCEPT")
            response_thread = threading.Thread(target=self.send_response, args=("ACCEPTED", src, op_num, content), daemon=True)
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
            decide_thread = threading.Thread(target=self.decide, args=(msg, src,), daemon=True)
            decide_thread.start()
          elif header == "RESPONSE":
            # Store response from other server
            context_id = message["context_id"]
            server_id = message["ballot_number"][1]
            response = message["message"]
            
            # Initialize dict for context if needed
            if context_id not in self.collected_responses:
                self.collected_responses[context_id] = {}
            
            # Store response
            self.collected_responses[context_id][server_id] = response
            
            # Print received response
            print(f"\nReceived from server {server_id} for context {context_id}:")
            print(f"Response: {response}\n")
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
  
  
  def send_message(self, header, content):
    for node in range(self.num_nodes):
      if node == self.id:
        continue
      
      message = {
        "header" : header,
        "message" : content,
        "ballot_number" : (self.op, self.id, self.seq_num),
        "dest" : node
      }
      
      # Maybe consider having a send lock if this becomes a problem
      # Serialize the message to JSON and encode it to bytes
      message_bytes = json.dumps(message).encode('utf-8')

      # Calculate the length of the message
      message_length = len(message_bytes)

      # Pack the length into 4 bytes using big-endian format
      length_prefix = struct.pack('>I', message_length)

      # Send the length prefix followed by the message bytes
      with self.send_lock:
        self.socket.sendall(length_prefix + message_bytes)
  
  def reach_consensus(self, command):
    # Send PROPOSE message if not leader:
    # if self.leader != self.id:
    #   self.send_message(header="PROPOSE", message=msg)
    #   self.accepted_condition.wait_for(lambda: self.accepted_num >= self.majority)
    #   self.accepted_num = 0
    #   self.leader = self.id
      
    # Send ACCEPT message:
    self.send_message(header="ACCEPT", content=command)
    with self.accepted_condition:
      self.accepted_condition.wait_for(lambda: self.accepted_num >= self.majority)
      self.accepted_num = 0
  
    # Send DECIDE message:
    self.decide(message=command, src=-1, is_leader=True)
    self.send_message(header="DECIDE", content=command)
    
    

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
  def send_response(self, header, dest, op_num, content):
    if self.op > op_num:
      logging.debug("I DONT ACCEPT!!!!!!!!!!!!")
      return
    
    message = {
      "header" : header,
      "message" : content,
      "ballot_number" : (self.op, self.id, self.seq_num),
      "dest" : dest
    }
  
    # Serialize the message to JSON and encode it to bytes
    message_bytes = json.dumps(message).encode('utf-8')

    # Calculate the length of the message
    message_length = len(message_bytes)

    # Pack the length into 4 bytes using big-endian format
    length_prefix = struct.pack('>I', message_length)

    
    
    # Maybe consider having a send lock if this becomes a problem
    with self.send_lock:
      self.socket.sendall(length_prefix + message_bytes)
  
  def decide(self, message, src):
    """Handle consensus decisions and coordinate responses."""
    tokens = message.strip().split()
    if not tokens:
        return
        
    command = tokens[0]
    
    if command == "create" and len(tokens) == 2 and tokens[1].isdigit():
        context_id = tokens[1]
        success = self.service.create_context(context_id)
        if success:
            print(f"NEW CONTEXT {context_id}")
            
    elif command == "query" and len(tokens) == 3 and tokens[1].isdigit():
        context_id = tokens[1]
        query_string = tokens[2]
        
        # Add query to local context
        if self.service.add_query_to_context(context_id, query_string):
            print(f"NEW QUERY on {context_id} with {query_string}")
            
            # Generate this server's response
            response = self.service.generate_response(context_id)
            if response:
                # Send response back to source server
                message = {
                    "header": "RESPONSE",
                    "message": response,
                    "ballot_number": (self.op, self.id, self.seq_num),
                    "dest": src,
                    "context_id": context_id  # Include context_id in response
                }
                self.socket.sendall(json.dumps(message).encode('utf-8'))
                
    elif command == "choose" and len(tokens) == 3 and tokens[1].isdigit() and tokens[2].isdigit():
        context_id = tokens[1]
        server_id = int(tokens[2])
        if server_id in self.collected_responses.get(context_id, {}):
            chosen_answer = self.collected_responses[context_id][server_id]
            if self.service.save_answer(context_id, chosen_answer):
                print(f"CHOSEN ANSWER on {context_id} with {chosen_answer}")
                self.collected_responses.pop(context_id, None)
                
    elif command == "view" and len(tokens) == 2 and tokens[1].isdigit():
        context_id = tokens[1]
        context = self.service.get_context(context_id)
        if context:
            print(f"\nContext {context_id}:\n{context}\n")
            
    elif command == "viewall":
        contexts = self.service.get_all_contexts()
        print("\nAll Contexts:")
        for cid, context in contexts.items():
            print(f"\nContext {cid}:\n{context}")

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
          message = f"{command} {context_id}"
          self.reach_consensus(message)
          
          # start create context thread
        elif command == "query" and len(tokens) == 3 and tokens[1].isdigit():
          context_id = tokens[1]
          query_string = tokens[2]
          message = f"{command} {context_id} {query_string}"
          self.reach_consensus(message)
          # start query thread
        elif command == "choose" and len(tokens) == 3 and tokens[1].isdigit() and tokens[2].isdigit():
          context_id = tokens[1]
          response_number = tokens[2]
          message = f"{command} {context_id} {response_number}"
          self.reach_consensus(message)
        elif command == "view" and len(tokens) == 2 and tokens[1].isdigit():
          # TODO: start view thread and create view function
          context_id = tokens[1]
          context_string = self.service.get_context(context_id)
          print(context_string)
        elif command == "viewall":
          # TODO: start viewall thread and create viewall function
          all_context_strings = self.service.get_all_contexts()
          print(all_context_strings)
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
