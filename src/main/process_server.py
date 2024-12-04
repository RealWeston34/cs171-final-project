import socket
import threading
import time
import logging
import collections
import json
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
    
    self.promised_num = 0
    self.proposal_condition = threading.Condition() # Are there edge cases associated with this?
    # Logic is once greater than majority, will send decide and then reset to 0, this assumes one accept at a time, which aligns with multi paxos assumptions
    self.accepted_num = 0
    # self.accepted_lock = threading.Lock()
    self.accepted_condition = threading.Condition()
    self.pending_operations = collections.deque() # each entry is a command
    self.send_lock = threading.Lock()
    self.operation_event = threading.Event()
    self.leader_ack_event = threading.Event()

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
      
      consensus_thread = threading.Thread(target=self.handle_consensus, daemon=True)
      consensus_thread.start()
      
      

    except Exception as e:
      logging.exception(f"ProcessServer failed to connect to {self.target_host}:{self.target_port}: {e}")
      
  def recvall(self, sock, n):
    """Helper function to read exactly n bytes."""
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None  # Connection closed or error
        data.extend(packet)
    return bytes(data)
  
  # might want to split this function up into one that receives the message and one that processes the message in a new thread?
  def listen(self):
    """
    Listen for incoming messages from the NetworkServer.
    """
    try:
      while self.is_running:
        raw_length = self.recvall(self.socket, 4)
        if not raw_length:
          break  # Connection closed or error

        # Unpack the length (big-endian unsigned integer)
        message_length = struct.unpack('>I', raw_length)[0]

        # Read the message data based on the length
        message_bytes = self.recvall(self.socket, message_length)
        if not message_bytes:
          break  # Connection closed or error

        # Decode and deserialize the JSON message
        message = json.loads(message_bytes.decode('utf-8'))
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
        elif header == "PROPOSE":
          op_num = message["ballot_number"][0]
          content = message["message"]
          logging.debug("Starting response thread for PROPOSE")
          propose_response_thread = threading.Thread(target=self.send_response, args=("PROMISE", src, op_num, content,), daemon=True)
          propose_response_thread.start()
        elif header == "PROMISE":
          with self.proposal_condition:
            self.promised_num += 1
            self.proposal_condition.notify_all()
        elif header == "FORWARD":
          content = message["message"]
          forward_thread = threading.Thread(target=self.send_response, args=("ACK", src, float("inf"), content,), daemon=True)
          forward_thread.start()
          self.pending_operations.append(content)
          self.operation_event.set()
        elif header == "ACK":
          content = message["message"]
          self.leader_ack_event.set()
          print(f"Received ACK from {src} for command {content}")
        elif header == "DECIDE":
          # create new decide function
          logging.info(f"ProcessServer received message: {header} from {src}")
          msg = message["message"]
          decide_thread = threading.Thread(target=self.decide, args=(msg, src,), daemon=True)
          decide_thread.start()
        elif header == "RESPONSE":
            context_id = message["context_id"]
            server_id = message["ballot_number"][1]
            response = message["message"]
            
            if context_id not in self.collected_responses:
                self.collected_responses[context_id] = {}
            
            self.collected_responses[context_id][server_id] = response
            
            print(f"\nReceived from server {server_id} for context {context_id}:")
            print(f"Response: {response}\n")
        else:
            logging.warning(f"ProcessServer received unknown message: {message}")
    except Exception as e:
      if self.is_running:
          logging.exception(f"ProcessServer error while listening: {e}")
    finally:
      self.socket.close()
      logging.info("ProcessServer connection closed")
  
  def handle_consensus(self):
    # leader is unknown, send proposal
    while self.is_running:
      self.operation_event.wait()
      while self.pending_operations:
        command = self.pending_operations[0]
        logging.debug(f"Current operation: {command}, total on queue: {self.pending_operations}")
        if self.leader == -1:
          self.leader_election(command)
        elif self.leader != self.id:
          self.leader_ack_event.clear()
          self.send_response("FORWARD", self.leader, float("inf"), command)
          ack_received = self.leader_ack_event.wait(timeout=10.0)
          
          if not ack_received:
            self.leader_election(command)
          else:
            self.pending_operations.popleft()
            continue
    
        self.reach_consensus(command)
        self.pending_operations.popleft()
        logging.debug(f"Done reaching consensus, pending operations: {self.pending_operations}")
      self.pending_operations.clear()
      
  def send_message(self, header, content, context_id=-1):
    for node in range(self.num_nodes):
      if node == self.id:
        continue
      
      message = {
        "header" : header,
        "message" : content,
        "ballot_number" : (self.op, self.id, self.seq_num),
        "dest" : node,
        "context_id" : context_id
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
      
  
    
  def leader_election(self, command):
    self.send_message(header="PROPOSE", content=command)
    with self.proposal_condition:
      self.proposal_condition.wait_for(lambda: self.promised_num >= self.majority)
      self.promised_num = 0
      
    self.leader = self.id # set itself as the new leader
    
  
  def reach_consensus(self, command):      
    # Send ACCEPT message:
    self.send_message(header="ACCEPT", content=command)
    with self.accepted_condition:
      self.accepted_condition.wait_for(lambda: self.accepted_num >= self.majority)
      self.accepted_num = 0
  
    # Send DECIDE message:
    self.decide(message=command, src=-1, is_leader=True)
    self.send_message(header="DECIDE", content=command)
    
  def send_response(self, header, dest, op_num, content, context_id=-1):
    if self.op > op_num:
      logging.debug(f"Did not {header} from {dest}")
      return

    message = {
      "header" : header,
      "message" : content,
      "ballot_number" : (self.op, self.id, self.seq_num),
      "dest" : dest,
      "context_id" : context_id
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
    
    if header == "PROMISE":
      self.leader = dest
      logging.debug(f"LEADER is set to {dest}")
  
  def decide(self, message, src, is_leader=False):
    """Handle consensus decisions and coordinate responses."""
    tokens = message.strip().split()
    logging.debug(f"Tokens: {tokens}")
    if not tokens:
      return
        
    command = tokens[0]
    response = ""
    context_id = -1
    
    if command == "create" and len(tokens) == 2 and tokens[1].isdigit():
      context_id = tokens[1]
      success = self.service.create_context(context_id)
      if success:
        print(f"NEW CONTEXT {context_id}")
        self.op += 1
            
    elif command == "query" and len(tokens) >= 3 and tokens[1].isdigit():
      context_id = tokens[1]
      query_string = ' '.join(tokens[2:])
      
      # Add query to local context
      if self.service.add_query_to_context(context_id, query_string):
        print(f"NEW QUERY on {context_id} with {query_string}")
        response += self.service.generate_response(context_id)
        self.op += 1
      else:
        logging.error("Failed to decide on QUERY function")
    elif command == "choose" and len(tokens) >= 3 and tokens[1].isdigit():
      context_id = tokens[1]
      chosen_answer = ' '.join(tokens[2:])
      if self.service.save_answer(context_id, chosen_answer):
        print(f"CHOSEN ANSWER on {context_id} with {chosen_answer}")
        self.op += 1
    else:
      response = "Could not decide!"
    
    if not is_leader and response:
      self.send_response(header="RESPONSE", dest=src, op_num=float("inf"), content=response, context_id=context_id)
      

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
        user_input = input("Enter command: ")
        tokens = user_input.strip().split()
        if not tokens:
          continue
        command = tokens[0]
        consensus_message = ""
        if command == "create" and len(tokens) == 2 and tokens[1].isdigit():
          context_id = tokens[1]
          consensus_message = f"{command} {context_id}"
          # start create context thread
        elif command == "query" and len(tokens) >= 3 and tokens[1].isdigit():
          context_id = tokens[1]
          query_string = ' '.join(tokens[2:])
          consensus_message = f"{command} {context_id} {query_string}"
          # start query thread
        elif command == "choose" and len(tokens) == 3 and tokens[1].isdigit() and tokens[2].isdigit():
          context_id = tokens[1]
          server_id = int(tokens[2])
          if server_id in self.collected_responses.get(context_id, {}):
            chosen_answer = self.collected_responses[context_id][server_id]
            consensus_message = f"{command} {context_id} {chosen_answer}"
            self.collected_responses.pop(context_id, None)
          else:
            logging.error(f"Cannot find context history for {context_id}")
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
        elif command == "exit":
          logging.info("ProcessServer exiting upon user request.")
          self.shutdown()
          break
        else:
          print("Invalid command.")
        
        if consensus_message:
          self.pending_operations.append(consensus_message)
          self.operation_event.set()
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
