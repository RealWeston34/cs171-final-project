import socket
import threading
import argparse
import logging
import collections
import json
import os
import struct
import time
from llm_service import LLMService
from dotenv import load_dotenv

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
    self.leader = -1 # keep track of the current leader in multi paxos
    self.num_nodes = 3 # maybe pass as an arg ?
    self.majority = (self.num_nodes // 2) # includes self
    self.collected_responses = {}  # context_id -> {server_id -> response}
    self.promised_ballot = (-1, -1, -1)
    
    self.ballot = {
      "seq_num" : 1,
      "id" : id,
      "op" : 0
    }
    
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
    
    
  def connect(self):
    """
    Establish a connection to the NetworkServer.
    """
    try:
      self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      self.socket.bind(('localhost', self.server_port))
      self.socket.connect((self.target_host, self.target_port))
      logging.info(f"ProcessServer connected to NetworkServer at {self.target_host}:{self.target_port}")

      # Start a thread to listen for incoming messages
      listener_thread = threading.Thread(target=self.listen, daemon=True)
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
          break  
        
        message_length = struct.unpack('>I', raw_length)[0]

        message_bytes = self.recvall(self.socket, message_length)
        if not message_bytes:
          break

        message = json.loads(message_bytes.decode('utf-8'))
        header = message["header"]
        ballot_number = message["ballot_number"]
        content = message["message"]
        src = message["src"]

        if "contexts" in message:
          self.service.compare_and_update_dict(message["contexts"])

        if header == "KILL":
          logging.info("ProcessServer received KILL message. Shutting down.")
          self.shutdown()
          break
        elif header == "ACCEPT":
          print(f"Received ACCEPT <{ballot_number[0]} {ballot_number[1]} {ballot_number[2]}> {content} from Server {src}")
          response_thread = threading.Thread(target=self.send_response, args=("ACCEPTED", src, ballot_number, content, -1, True,), daemon=True)
          response_thread.start()
        elif header == "ACCEPTED":
          print(f"Received {header} <{ballot_number[0]} {ballot_number[1]} {ballot_number[2]}> {content} from Server {src}")
          # maybe start in a new thread?
          with self.accepted_condition:
            self.accepted_num += 1
            self.accepted_condition.notify_all()
        elif header == "PROPOSE":
          print(f"Received PROPOSE <{ballot_number[0]} {ballot_number[1]} {ballot_number[2]}> {content} from Server {src}")
          propose_response_thread = threading.Thread(target=self.send_response, args=("PROMISE", src, ballot_number, content,-1, True,), daemon=True)
          propose_response_thread.start()
        elif header == "PROMISE":
          print(f"Received {header} <{ballot_number[0]} {ballot_number[1]} {ballot_number[2]}> {content} from Server {src}")
          with self.proposal_condition:
            self.promised_num += 1
            self.proposal_condition.notify_all()
        elif header == "FORWARD":
          ballot_number = message["ballot_number"]
          content = message["message"]
          print(f"Received {header} <{ballot_number[0]} {ballot_number[1]} {ballot_number[2]}> {content} from Server {src}")
          forward_thread = threading.Thread(target=self.send_response, args=("ACK", src, ballot_number, content,), daemon=True)
          forward_thread.start()
          self.pending_operations.append(content)
          self.operation_event.set()
        elif header == "ACK":
          content = message["message"]
          self.leader_ack_event.set()
          print(f"Received {header} <{ballot_number[0]} {ballot_number[1]} {ballot_number[2]}> {content} from Server {src}")
        elif header == "DECIDE":
          # create new decide function
          print(f"Received {header} <{ballot_number[0]} {ballot_number[1]} {ballot_number[2]}> {content} from Server {src}")
          msg = message["message"]
          decide_thread = threading.Thread(target=self.decide, args=(msg, src, ballot_number), daemon=True)
          decide_thread.start()
        elif header == "RESPONSE":
            context_id = message["context_id"]
            server_id = src
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
        ballot_number = self.ballot_to_tuple()
        if self.leader == -1:
          received_promise_majority = self.leader_election(command, ballot_number)
          if not received_promise_majority: 
            self.increment_ballot()
            self.pending_operations.popleft()
            continue
        elif self.leader != self.ballot["id"]:
          self.leader_ack_event.clear()
          self.send_response("FORWARD", self.leader, ballot_number, command)
          ack_received = self.leader_ack_event.wait(timeout=10.0)
          
          if not ack_received:
            logging.error(f"Did not receive aknowledgement from {self.leader} after 10 seconds")
            print("Starting new leader election")
            received_promise_majority = self.leader_election(command, ballot_number)
            if not received_promise_majority:
              self.increment_ballot()
              self.pending_operations.popleft()
              print("Failed to reach a majority of PROMISE messages")
              continue
          else:
            self.pending_operations.popleft()
            continue
    
        self.reach_consensus(command, ballot_number)
        self.pending_operations.popleft()
        logging.debug(f"Done reaching consensus, pending operations: {self.pending_operations}")
      self.pending_operations.clear()

  def leader_election(self, command, ballot_number):
    self.promised_ballot = self.ballot_to_tuple()
    self.send_message(header="PROPOSE", content=command, ballot_number=ballot_number)
    self.promised_num = 0
    with self.proposal_condition:
      received_promise_majority = self.proposal_condition.wait_for(lambda: self.promised_num >= self.majority, timeout=10.0)
    if received_promise_majority:
      self.leader = self.ballot["id"] # set itself as the new leader
    return received_promise_majority

  def reach_consensus(self, command, ballot_number):      
    # Send ACCEPT message:
    self.send_message(header="ACCEPT", content=command, ballot_number=ballot_number)
    self.accepted_num = 0
    with self.accepted_condition:
      received_accept_majority = self.accepted_condition.wait_for(lambda: self.accepted_num >= self.majority, timeout=10.0)
    
    if not received_accept_majority:
      print("Failed to receive a majority of ACCEPTED messages")
      return
      
    # Send DECIDE message:
    self.decide(message=command, src=-1, ballot_number=ballot_number, is_leader=True)
    self.send_message(header="DECIDE", content=command, ballot_number=ballot_number)

  # when a proposal fails, want to increment the proposal value
  def increment_ballot(self):
    n_seq_num = max(self.ballot["seq_num"], self.promised_ballot[0]) + 1
    self.ballot["seq_num"] = n_seq_num
  
  def ballot_to_tuple(self):
    return (self.ballot["seq_num"], self.ballot["id"], self.ballot["op"])
    
      
  # return true if current max ballot is greater than passed ballot_number
  def compare_ballot(self, ballot_number):
    return (self.promised_ballot[2], self.promised_ballot[0], self.promised_ballot[1]) > (ballot_number[2], ballot_number[0], ballot_number[1])

  
  def send_message(self, header, content, ballot_number, context_id=-1):
    print(f"Sending {header} <{ballot_number[0]}, {ballot_number[1]}, {ballot_number[2]}> {content} to ALL")
    for node in range(self.num_nodes):
      if node == self.ballot["id"]:
        continue
      
      message = {
        "header" : header,
        "message" : content,
        "ballot_number" : ballot_number,
        "dest" : node,
        "src" : self.ballot["id"],
        "context_id" : context_id,
        "contexts": self.service.get_all_contexts()
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

  # TODO: update this to handle leader election
  def send_response(self, header, dest, ballot_number, content, context_id=-1, requires_ballot_comparison=False):
    # print(f"curr ballot: {self.max_ballot}, received ballot: {ballot_number}, comparison result {self.compare_ballot(ballot_number)}, flag: {requires_ballot_comparison}")
    if self.compare_ballot(ballot_number) and requires_ballot_comparison:
      print(f"Did not PROMISE {header} <{ballot_number[0]}, {ballot_number[1]}, {ballot_number[2]}> {content} from Server {dest}")
      return
    
    print(f"Sending {header} <{ballot_number[0]}, {ballot_number[1]}, {ballot_number[2]}> {content} to Server {dest}")
    message = {
      "header" : header,
      "message" : content,
      "ballot_number" : ballot_number,
      "dest" : dest,
      "src" : self.ballot["id"],
      "context_id" : context_id,
      "contexts": self.service.get_all_contexts()
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
    
    if header == "PROMISE" or header == "ACCEPTED":
      self.leader = dest
      self.promised_ballot = ballot_number
      logging.debug(f"LEADER is set to {dest}")
  
  def decide(self, message, src, ballot_number, is_leader=False):
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
        self.ballot["op"] += 1
            
    elif command == "query" and len(tokens) >= 3 and tokens[1].isdigit():
      context_id = tokens[1]
      query_string = ' '.join(tokens[2:])
      
      # Add query to local context
      if self.service.add_query_to_context(context_id, query_string):
        print(f"NEW QUERY on {context_id} with {query_string}")
        response += self.service.generate_response(context_id)
        self.ballot["op"] += 1
        if context_id not in self.collected_responses:
            self.collected_responses[context_id] = {}
        self.collected_responses[context_id][self.id] = response

        print(f"\nReceived from server {self.id} for context {context_id}:")
        print(f"Response: {response}\n")
      else:
        logging.error("Failed to decide on QUERY function")
    elif command == "choose" and len(tokens) >= 3 and tokens[1].isdigit():
      context_id = tokens[1]
      chosen_answer = ' '.join(tokens[2:])
      if self.service.save_answer(context_id, chosen_answer):
        print(f"CHOSEN ANSWER on {context_id} with {chosen_answer}")
        self.ballot["op"] += 1
    else:
      response = "Could not decide!"
    
    if not is_leader and response:
      print(context_id)
      print(ballot_number)
      self.send_response(header="RESPONSE", dest=src, ballot_number=ballot_number, content=response, context_id=context_id)
      
  def shutdown(self):
    """
    Shutdown the ProcessServer gracefully.
    """
    self.is_running = False
    if self.socket:
      try:
        self.socket.shutdown(socket.SHUT_RDWR)
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
        user_input = input()
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
        elif command == "exit" and len(tokens) == 1:
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
  
def parse_args():
  parser = argparse.ArgumentParser(description="Process server with configurable logging.")
  parser.add_argument("id", type=int, help="Server ID")
  parser.add_argument("target_host", help="Target host for the server")
  parser.add_argument("target_port", type=int, help="Target port for the server")
  parser.add_argument(
    "--log-level",
    choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "OFF"],
    default="DEBUG",
    help="Set the logging level. Use 'OFF' to disable logging."
  )
  return parser.parse_args()

# Example usage
if __name__ == "__main__":
  args = parse_args()
  load_dotenv()

  # Configure logging
  if args.log_level == "OFF":
      logging.disable(logging.CRITICAL)  # Disable all logging
  else:
      logging.basicConfig(
          level=getattr(logging, args.log_level),  # Dynamically set logging level
          format="%(asctime)s - %(levelname)s - %(message)s"
      )

  # Extract arguments
  id = args.id
  target_host = args.target_host
  target_port = args.target_port

  # Create and run ProcessServer
  process_server = ProcessServer(id, target_host, target_port)
  process_server.run()
