# -*- coding:utf-8 -*-
import struct
from string import ascii_lowercase as al
from string import digits
from random import choice

KEEP_ALIVE = struct.pack('!B',0)
POSITION = struct.pack('!B',1)
GET_PEERS = struct.pack('!B',2)
GET_STREAM = struct.pack('!B',3)
STOP = struct.pack('!B',4)
CLOSE = struct.pack('!B',5)
ERROR = struct.pack('!B',6)
HAVE = struct.pack('!B',7)

def construct_handshake(content_id,peer_id,port):
	''' Structure of handshake
	length|Salamatsyzby|content_id|peer_id|port
	peer_id - 20 bytes, content_id - 32 bytes
	'''
	msg = 'Salamatsyzby' + content_id + peer_id + struct.pack('!H',port)
	return struct.pack('!I',68) + msg

def generate_content_id(length=32):
	return ''.join([choice(al + digits) for i in xrange(length)])


def generate_peer_id(length=20):
	return generate_content_id(length)

class Messages:
	def __init__(self):
		self.formats = {'length' : '!I', 'position':'!H', 'ip':'!BBBB','port':'!H',
						'stream_recv':'!HIIH','stream_send':'!HI'}

	def encode(self,msg):
		pass

	def decode(self,msg):
		id = msg[0]
		payload = msg[1:]

	def keep_alive(self):
		pass

	def position(self):
		pass

	def peers(self):
		pass

	def stream(self):
		pass

	def stop(self):
		pass

	def close(self):
		pass

	def error(self):
		pass

	def have(self):
		pass

class Work:
	def __init__(self,handshake):
		self.handshake = handshake
		self.peers = []

	def connect(self,ip,port):
		sock = socket.socket()
		sock.setblocking(0)
		try:
			sock.connect((ip,port))
		except socket.error as e:
			if e.errno != 36:
				return
		return sock

	def disconnect(self,peer):
		pass

	def create_peer(self,sock):
		pass

	def connect_server(self,ip,port):
		c = self.connect(ip,port)
		if c is None:
			return
		peer = Peer()

	def send_message(self,peer,id,data):
		pass

	def get_handshaked_peers(self):
		pass

	def can_add(self):
		pass

	def close(self):
		pass


class DB:
	def __init__(self):
		pass

	def add_peer(self,content_id,ip,port):
		pass

	def remove_peer(self,content_id,ip):
		pass

	def get_peers(self,content_id):
		pass

	def close(self):
		pass










