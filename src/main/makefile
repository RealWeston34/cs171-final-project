# Note that for autograder reasons,
# the following variable must
# be spelled exactly PORT!
PORT := 9000
# Port is fixed at 9000
# Compile command:
# make compile
compile:
	# Nothing to compile

# Run command:
# make network_server
network_server:
	python3 -u network_server.py $(PORT) 3

# Run command:
# make process_server ID=1
server1:
	python3 -u process_server.py 1 localhost $(PORT)

server2:
	python3 -u process_server.py 2 localhost $(PORT)

server3:
	python3 -u process_server.py 3 localhost $(PORT)

