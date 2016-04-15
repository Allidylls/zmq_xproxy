all: 
	cc -Wall -g -o zmq_xproxy main.cpp -lzmq -lstdc++

clean:
	rm -rf zmq_xproxy
