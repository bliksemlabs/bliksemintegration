import zmq

GTFS_RT_REQ_ZMQ = "tcp://127.0.0.1:6007"

context = zmq.Context()

receiver = context.socket(zmq.REQ)
receiver.connect(GTFS_RT_REQ_ZMQ)

receiver.send('REQUEST')
# receiver.send('RESET')
print receiver.recv()
