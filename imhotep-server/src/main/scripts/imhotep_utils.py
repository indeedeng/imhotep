# make sure the protobuf file is part of the path
import os, sys
python_pb_dir = '%s/imhotep/gen-src/python' % os.environ['INDEED_PROJECT_DIR']
if python_pb_dir not in sys.path :
	sys.path += [python_pb_dir]

from Imhotep_pb2 import ImhotepRequest, ImhotepResponse

import socket
import struct

def send_request(request, host) :
	print 'opening connection to %s' % ':'.join(map(str, host))
	conn = socket.create_connection(host)
	
	req_payload = request.SerializeToString()
	print 'sending %d-byte request' % len(req_payload)
	conn.send(struct.pack('>i', len(req_payload))+req_payload)
	
	print 'waiting for response'
	resp_len = struct.unpack('>i', conn.recv(4))[0]
	
	print 'reading %d-byte response' % resp_len
	resp_payload = ''
	while len(resp_payload) < resp_len :
		resp_payload += conn.recv(4096)
	
	response = ImhotepResponse()
	response.ParseFromString(resp_payload)
	
	print 'received full response, closing connection'
	
	conn.close()
	
	return response

def read_hosts_file(filename) :
	hosts_file = open(filename)
	hosts = [line.strip().split(':') for line in hosts_file if not line.startswith('#')]
	hosts_file.close()
	return hosts

def get_status_dump(host) :
	request = ImhotepRequest(request_type=ImhotepRequest.GET_STATUS_DUMP)
	response = send_request(request, host)
	return response.status_dump

def get_shard_list(host) :
	request = ImhotepRequest(request_type=ImhotepRequest.GET_SHARD_LIST)
	response = send_request(request, host)
	return response.shard_info

def get_all_shard_ids(dataset, hosts_file='/var/imhotep/hosts.txt') :
	return sorted(reduce(lambda x,y : x | set([shard.shard_id for shard in y if shard.dataset == dataset]), [get_shard_list(host) for host in read_hosts_file(hosts_file)], set()))
