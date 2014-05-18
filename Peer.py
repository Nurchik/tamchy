# -*- coding:utf-8 -*-

import socket,struct,time,logging,sqlite3
import messages
from StringIO import StringIO
from threading import Lock
import pickle
import random

KEEP_ALIVE = struct.pack('!B',0)
POSITION = struct.pack('!B',1)
GET_PEERS = struct.pack('!B',2)
GET_STREAM = struct.pack('!B',3)
STOP = struct.pack('!B',4)
CLOSE = struct.pack('!B',5)
ERROR = struct.pack('!B',6)
HAVE = struct.pack('!B',7)

MSG = {KEEP_ALIVE:'KEEP_ALIVE',POSITION:'POSITION',GET_PEERS:'GET_PEERS',
	   GET_STREAM:'GET_STREAM',STOP:'STOP',CLOSE:'CLOSE',ERROR:'ERROR',HAVE:'HAVE'}

STREAM_PIECE_SIZE = 16384
TIMEOUT = 20.0
REQUEST_TIMEOUT = 10.0
KEEP_ALIVE_INTERVAL = 120.0
ALIVE_TIMEOUT = 150.0
LIMIT = 60

class store:
	def __init__(self):
		self.lock = Lock()
		self.s = []

	def put(self,t):
		if t not in self.s:
			with self.lock:
				self.s.append(t)
				# we need to clear self.s from overloading with positions, so LIMIT is optimal number to do it 
				if len(self.s) > LIMIT:
					self.s.pop(0)

	def have(self,t):	
		with self.lock:
			return (t in self.s)

class Future(Exception):
	pass

class Request:
	def __init__(self,C,Buffer,pos,offset,length,seconds):
		self.t = time.time()
		self.B = Buffer
		self.C = C
		self.info = str((pos,offset,length,seconds))
		self.p = pos
		self.s = seconds
		self.len = length
		self.ps = pos + seconds
		self.buffer = {}
		self.c = {}
		self.logger = logging.getLogger('tamchy.Peer.Request')
		# setting dictionary of completion of pieces
		# self.c.__setitem__, instead of self.c[key] because last one just doesn't work in list-generator
		[self.c.__setitem__(i,False) for i in xrange(self.p,self.ps)]

	def __contains__(self,item):
		return (item >= self.p and item < self.ps)

	def __str__(self):
		return self.info

	@property
	def completed(self):
		if False not in self.c.values():
			return True
		return False 
			
	def put(self,p,o,d):
		self.t = time.time()
		b = self.buffer
		if p not in b:
			# dictionary key -> 'len' is the length of the entire block
			# float, because when we calculate percentage of completion, if we haven't float in one operand
			# we will see that percentage every time is equal to 0
			b[p] = {'len' : float(struct.unpack('!I',d[:4])[0]), 'data' : StringIO()}
		f = b[p]['data']
		try:
			f.seek(o)
			f.write(d)
		except:
			self.logger.error('Closed file at adding ' + str((p,o,len(d))))
			#raise Exception
		# checking completion of piece
		percent = int(f.len / b[p]['len'] * 100)
		if percent >= 100:
			self.B.put(p,f.getvalue())
			try:
				del self.B.pieces[p]
			except:
				pass
			self.C.tell_have(p)
			self.c[p] = True
			f.close()
			self.logger.debug('Piece ' + str(p) + ' completed')
		else:
			self.B.pieces[p] = percent

	def return_requests(self):
		# Пока будем считать, что куски заполняются по порядку и => в empty будут последовательные значения
		s = None
		p = None
		of = None
		for pos,val in self.c.items():
			# if (pos in self.c) == False => piece with this position not completed
			if not val:
				p = pos
				if pos not in self.buffer:
					of = 0
				else:
					of = self.buffer[pos]['data'].len
				# we think that pieces are downloaded successively => seconds to download = number of pieces
				# which are not downloaded after p - p
				s = self.ps - p
				break
		if p is None:
			return []
		return (p,of,self.len,s)

class Peer:
	def __init__(self,content_id,handshake,Container,Buffer,ip=None,port=None,sock=None,buf='',server=False):
		self.logger = logging.getLogger('tamchy.Peer')
		self.handshake = handshake
		self.handshaked = False
		self.store = store()
		self.server = server
		
		self.last_message_recv = time.time()
		self.last_message_sent = time.time()

		self.content_id = content_id
		self.requested = []
		self.send_buffer = ''
		
		self.read_buffer = ''
		# if we have previously recieved data from peer -> add to the read_buffer
		self.read_buffer += buf
		
		self.B = Buffer
		self.C = Container
		# request -> (pos,offset,length,seconds):generator
		self.streamer = {}
		self.temp = {}
		self.upload_speed = 0
		self.download_speed = 0
		self.logger.info('Created new peer instance')
		self.closed = False
		self.lock = Lock()
		self.handle_connect(sock,ip,port)

	def handle_connect(self,sock=None,ip=None,port=None):
		self.socket = None
		if sock is not None:
			self.socket = sock
			self.socket.setblocking(0)
			self.ip = ip
			self.raw_ip = sock.getpeername()[0]
			self.port = port
			self.raw_port = struct.unpack('!H',port)[0]
			self.initiated = False
			
			# we must send handshake-message and the current position
			self.send_buffer += (self.handshake + self.B.get_pos())
			self.C.connected_peers += 1
			self.handshaked = True
		
		else:
			sock = socket.socket()
			sock.setblocking(0)
			try:
				sock.connect((ip,port))
			except socket.error as e:
				if e.errno == 36:
					self.socket = sock
			self.ip = self.to_bytes('!B',ip)
			self.raw_ip = ip
			self.port = struct.pack('!H',port)
			self.raw_port = port
			self.initiated = True
			# if we just started our program, get_pos will return 0
			# but another peer will ignore that
			self.send_buffer += (self.handshake + self.B.get_pos())
			self.logger.debug('Wrote to send buffer handshake-message')

	def fileno(self):
		try:
			s = self.socket.fileno()
			return s
		except:
			self.handle_close()

	def to_bytes(self,fmt,value):
		val = (value.split('.') if (type(value) != int) else value)
		return ''.join([struct.pack(fmt,int(x)) for x in val])

	def handle_write(self):
		s = self.streamer
		while self.streamer:
			try : data = s[s.keys()[0]].next()
			except StopIteration:
				self.logger.debug('Stream generator finished work')
				del self.streamer[s.keys()[0]]
				continue
			if data:
				self.send_message(GET_STREAM,data)
			# data = '' => no piece in Buffer at this moment
			break
		t = time.time()
		if self.send_buffer:
			try:
				sent = self.socket.send(self.send_buffer)			
			except:
				return self.handle_close()
			self.last_message_sent = time.time()
			self.download_speed = sent / (time.time() - t)
			self.send_buffer = self.send_buffer[sent:]
		

	def handle_read(self):
		r = True
		message = ''
		t = time.time()
		while r:
			try:
				m = self.socket.recv(8192)
				if not m: 
					return self.handle_close()
				message += m
			except:
				break
		self.last_message_recv = time.time()
		self.upload_speed = len(message) / (time.time() - t)
		if not message:
			return self.handle_close()

		self.read_buffer += message
		while self.read_buffer:
			length = self.read_buffer[:4]
			if len(length) < 4:
				# this is not entire message => wait for remaining part
				break
			length = struct.unpack('!I',self.read_buffer[:4])[0]
			if length > 32*1024:
				return self.handle_close()
			msg = self.read_buffer[4:4 + length]
			if len(msg) < length:
				# this is not entire message => wait for remaining part
				break

			self.read_buffer = self.read_buffer[4 + length:]
			
			if not self.handshaked:
				if (msg[:12]).lower() == 'salamatsyzby':
					if msg[12:44] == self.content_id:
						if self.server:
							self.C.set_pos(struct.unpack('!H',msg[66:68])[0])
						self.C.connected_peers += 1
						self.handshaked = True
						self.logger.debug('Connection with peer ' + (self.raw_ip + ':' + str(self.raw_port)) + ' established')
						continue
				elif msg[0] == ERROR:
					try:
						error = pickle.loads(msg[1:])
					except:
						error = ''
					if error == 'Invalid Content ID':
						# Delete this peer from DB
						self.C.delete_peer(self.ip)
					self.logger.debug('Connection with peer not established --> ' + 'Invalid CONTENT ID')
				self.logger.debug('Connection with peer not established --> ' + 'Incorrect handshake-message')
				return self.handle_close()

			id = msg[0]
			payload = msg[1:]
			
			self.logger.debug('Got message ' + MSG.get(id,'UNKNOWN') + ' from peer ' + self.raw_ip)

			if id == KEEP_ALIVE:
				continue
			#elif id == POSITION:
			#	# this is a position request from peer
			#	if not payload:
			#		self.send_message(POSITION,self.B.get_pos())
			#	#this is a position response from peer
			#	else:
			#		self.C.start_pos = struct.unpack('!H',payload)[0]
			elif id == GET_PEERS:
				if len(payload) == 1:
					self.send_message(GET_PEERS,self.C.get_peers(struct.unpack('!B',payload)[0]))
				else:
					self.C.prepare_peers(payload)
			elif id == GET_STREAM:
				#This is a request from the peer
				if len(payload) == 12:
					pos,offset,length,seconds = struct.unpack('!HIIH',payload)
					self.logger.debug('Get Stream request ' + str((pos,offset,length,seconds)))
					self.add_stream(pos,offset,length,seconds)
				# And this is a response from peer for our request
				else:
					pos,offset = struct.unpack('!HI',payload[:6])
					payload = payload[6:]
					self.put(pos,offset,payload)
			elif id == STOP:
				pos,offset,length,seconds = struct.unpack('!HIIH',payload)
				try:
					del self.streamer[(pos,offset,length,seconds)]
				except:
					pass
				self.logger.debug('Stopped streaming ' + str((pos,offset,length,seconds)))
			elif id == HAVE:
				self.store.put(struct.unpack('!I',payload)[0])
			elif id == CLOSE:
				# deleting peer's ip,port from DB
				self.C.delete_peer(self.ip)
				return self.handle_close()
			else:
				return self.handle_close()
	
	def add_stream(self,pos,offset,length,seconds):
		self.streamer[(pos,offset,length,seconds)] = self.stream(pos,offset,length,seconds)
		self.logger.debug('Added stream generator to store for request ' + str((pos,offset,length,seconds)))

	def stream(self,pos,offset,length,seconds):
		self.logger.debug('Started generator for ' + str((pos,offset,length,seconds)))
		for i in xrange(seconds):
			pos1 = pos + i
			if pos1 >= 65536:
				pos1 = 65536 - pos1
			
			while True:
				data = self.B.get(pos1)
				if data:
					break
				# there is no piece in Buffer at this moment
				yield ''

			# it's just first position in request => we must take into account offset, 
			# for next positions -> offset = 0 => data will not be sliced
			if pos1 == pos:	
				data = data[offset:]
			# this scary line is just checking: if len(data)/length > 0 we must iterate (len(data)/length) + 1
			# just to send last part of data in last iteration
			for k in xrange((len(data) / length if not (len(data) % length) else (len(data) / length) + 1)):
				offset1 = k * length
				# offset1 + offset because we need to send absolute offset in the block
				if pos1 == pos:
					d = struct.pack('!HI',pos1,offset1+offset) + data[0+offset1 : length+offset1]
				else:
					d = struct.pack('!HI',pos1,offset1) + data[0+offset1 : length+offset1]
				self.logger.debug('Sent data for ' + str(struct.unpack('!HI',d[:6])))
				yield d
		
	def put(self,pos,offset,data):
		self.logger.debug('Put data to ' + str((pos,offset)) + ' length -> ' + str(len(data)))
		with self.lock:
			for req in self.requested:
				if pos in req:
					req.put(pos,offset,data)
					if req.completed:
						self.logger.debug('Request' + str(req) + ' completed')
						self.requested.remove(req)
					break

	def return_requests(self):
		r = []
		with self.lock:
			for req in self.requested:
				r.append(req.return_requests())
				self.requested.remove(req)
		return r

	@property
	def timeout(self):
		with self.lock:
			if self.requested:
				if time.time() - min(self.requested, key=lambda x : x.t).t <= REQUEST_TIMEOUT:
					return False
				return True
		if time.time() - self.last_message_recv >= ALIVE_TIMEOUT:
			return True

	def have(self,t):
		if self.server:
			return True
		return self.store.have(t)

	@property
	def can_request(self):
		# because we can request only one 'request' at one time until previous doesn't complete
		if self.requested:
			return False
		return True

	@property
	def need_keep_alive(self):
		return time.time() - self.last_message_sent > KEEP_ALIVE_INTERVAL

	def request_stream(self,pos,offset,length,seconds):
		self.send_message(GET_STREAM,struct.pack('!HIIH',pos,offset,length,seconds))
		self.requested.append(Request(self.C,self.B,pos,offset,length,seconds))
		self.logger.debug(str((pos,offset,length,seconds)) + ' requested')

	#def request_position(self):
	#	self.send_message(POSITION)
	#	self.logger.debug('POSITION requested')

	def request_peers(self,num=35):
		self.send_message(GET_PEERS,struct.pack('!B',num))
		self.logger.debug('List of peers requested')

	def send_have(self,t):
		self.send_message(HAVE,struct.pack('!I',t))
		self.logger.debug('Sended HAVE -> ' + str(t) + ' to ' + self.raw_ip)

	def send_stop(p,o,l,s):
		self.send_message(STOP,struct.pack('!HIIH',p,o,l,s))
		self.logger.debug('Sended STOP for ' + str((p,o,l,s)))

	def send_message(self,id,data=''):
		length = struct.pack('!I',len(id+data))
		self.send_buffer += (length+id+data)

	def send_keep_alive(self):
		try : 
			self.socket.send(struct.pack('!I',1) + KEEP_ALIVE)
			self.last_message_sent = time.time()
			self.logger.debug('Sended KEEP-ALIVE')
		except : 
			return self.handle_close()
	
	# This method for safe closing our program		
	def notify_closing(self):
		self.socket.send(struct.pack('!I',1) + CLOSE)
		self.handle_close()
		self.logger.info('Gracefully closing our program ... )')

	def handle_close(self):
		self.streamer = []
		self.read_buffer = ''
		self.send_buffer = ''
		r = self.return_requests()
		if r:
			self.C.return_reqs(r)
		self.C.remove(self)
		self.socket.close()
		self.closed = True
		if self.handshaked:
			self.C.connected_peers -= 1
		self.logger.info('Closing connection with peer')
		#if (not self.initiated and self.handshaked) or self.initiated:
		#	self.C.delete_peer(self.ip,self.port)
		
'''
We will implement different types of Peer by inheriting Peer class ->
Just need to reimplement next methods if needed:
handle_connect, handle_read, handle_write, send_message, send_keep_alive, notify_closing 
'''

class EncryptedPeer(Peer):
	pass

class UDPPeer(Peer):
	pass

# and so on .... :)

# Testing
class sock:
	def __init__(self):
		self.s_buf = ''
		self.r_buf = []
		self.closed = False

	def setblocking(self,num):
		pass

	def getpeername(self):
		return ('0.0.0.0',123)

	def fileno(self):
		return 0

	def send(self,data):
		self.s_buf += data
		return len(data)

	def recv(self,num):
		msg = self.r_buf.pop(0)
		if msg == 'except':
			raise Exception
		return msg

	def close(self):
		self.closed = True

class ContaineR:
	def __init__(self):
		self.connected_peers = 0
		self.start_pos = 0
		self.deleted = False
		self.prepared = ''
		self.h = []

	def add_new_peer(self,ip,port):
		pass

	def set_pos(self,pos):
		self.start_pos = pos

	def delete_peer(self,ip):
		self.deleted = True

	def prepare_peer(self,ip,port):
		pass

	def prepare_peers(self,peers):
		self.prepared = peers

	def can_add_peer(self):
		return True

	def remove(self,peer):
		pass

	def tell_have(self,pos):
		self.h.append(pos)

	def return_reqs(self,reqs):
		pass

	def get_peers(self,num):
		return 'peers_list'

class BuffeR:
	def __init__(self):
		self.buffer = {}
		self.pos = 0
		self.pieces = {}

	def put(self,pos,data):
		self.buffer[pos] = data

	def get(self,pos):
		return self.buffer.get(pos,'')

	def get_pos(self):
		return struct.pack('!H',self.pos)

def test_read():
	s = sock()
	c = ContaineR()
	b = BuffeR()
	p = Peer('content_id1234567890123456789012','handshake',c,b,sock=s,ip='127.0.0.1',port=struct.pack('!H',7668))
	p.send_buffer = ''
	p.handshaked = False
	s.r_buf = [struct.pack('!I',68)+'Salamatsyzby' + 'content_id1234567890123456789012' \
				+ 'peeeeeerrrrrr_iiiidd12' + struct.pack('!H',78), 'except']
	p.handle_read()
	assert c.start_pos == 0
	assert not s.closed
	assert not p.streamer
	p = Peer('content_id1234567890123456789012','handshake',c,b,sock=s,ip='127.0.0.1',port=struct.pack('!H',7668),server=True)
	p.send_buffer = ''
	p.handshaked = False
	s.r_buf = [struct.pack('!I',68)+'Salamatsyzby' + 'content_id1234567890123456789012' \
				+ 'peeeeeerrrrrr_iiiidd12' + struct.pack('!H',78), 'except']
	p.handle_read()
	assert c.start_pos == 78
	assert not s.closed
	assert not p.streamer
	p.send_buffer = ''
	p.requested = [Request(c,b,1,0,15,2)]
	s.r_buf = [struct.pack('!I',2) + GET_PEERS + struct.pack('!B',12), struct.pack('!I',12) + GET_PEERS + 'peers_list1',\
	struct.pack('!I',13) + GET_STREAM + struct.pack('!HIIH',1,3,45,3), struct.pack('!I',14) + GET_STREAM + struct.pack('!HI',1,0)+ \
	'payload', struct.pack('!I',15) + GET_STREAM + struct.pack('!HI',2,0) + 'payload1', struct.pack('!I',15) + GET_STREAM +\
	 struct.pack('!HI',3,0) + 'payload2','except']
	p.handle_read()
	assert p.send_buffer[5:] == 'peers_list'
	assert c.prepared == 'peers_list1'
	assert len(p.streamer) == 1
	assert len(p.requested[0].buffer) == 2
	assert not p.store.have(23)
	c.connected_peers = 2
	s.r_buf = [struct.pack('!I',13) + STOP + struct.pack('!HIIH',1,0,15,2), struct.pack('!I',5) + HAVE + struct.pack('!I',23),struct.pack('!I',1) + CLOSE, 'except']
	p.handle_read()
	assert not p.streamer
	assert p.store.have(23)
	assert c.deleted
	assert c.connected_peers == 1

def test_write():
	def g(n):
		for i in range(n):
			yield str(i)
	c = ContaineR()
	b = BuffeR()
	s = sock()
	p = Peer('content_id','handshake',c,b,sock=s,ip='127.0.0.1',port=struct.pack('!H',7668))
	p.handle_write()
	assert s.s_buf
	p.send_buffer = 'datadata'
	p.handle_write()
	assert not p.send_buffer
	assert s.s_buf == 'handshake' + struct.pack('!H',0) + 'datadata'
	p.streamer = {0:g(2), 1:g(3)}
	s.s_buf = ''
	p.send_buffer = 'data12'
	p.handle_write()
	assert s.s_buf == 'data12' + struct.pack('!I',2) + GET_STREAM + '0'
	s.s_buf = ''
	assert len(p.streamer) == 2
	p.handle_write()
	assert s.s_buf == struct.pack('!I',2) + GET_STREAM + '1'
	s.s_buf = ''
	p.handle_write()
	assert len(p.streamer) == 1
	p.handle_write()
	p.handle_write()
	assert s.s_buf == struct.pack('!I',2) + GET_STREAM + '0' + struct.pack('!I',2) + GET_STREAM + '1' +\
					  struct.pack('!I',2) + GET_STREAM + '2'
	p.handle_write()
	assert s.s_buf == struct.pack('!I',2) + GET_STREAM + '0' + struct.pack('!I',2) + GET_STREAM + '1' +\
					  struct.pack('!I',2) + GET_STREAM + '2'
	assert not p.streamer


#def test_close():
#	pass
#
#def test_add_stream():
#	pass
#
def test_stream():
	c = ContaineR()
	b = BuffeR()
	s = sock()
	p = Peer('content_id','handshake',c,b,sock=s,ip='127.0.0.1',port=struct.pack('!H',7668))
	b.buffer = {1:'data1data1', 2:'data2data2', 3:'data3data3', 4:'data4data4'}
	f = p.stream(5,0,15,1)
	try : d = f.next()
	except StopIteration : d = ''
	assert not d
	f = p.stream(3,4,3,2)
	assert f.next() == struct.pack('!HI',3,4) + '3da'
	assert f.next() == struct.pack('!HI',3,7) + 'ta3'
	assert f.next() == struct.pack('!HI',4,0) + 'dat'
	assert f.next() == struct.pack('!HI',4,3) + 'a4d'
	assert f.next() == struct.pack('!HI',4,6) + 'ata'
	assert f.next() == struct.pack('!HI',4,9) + '4'
	try : n = f.next()
	except StopIteration : n = ''
	assert not n
	del n
	f = p.stream(3,4,8,2)
	assert f.next() == struct.pack('!HI',3,4) + '3data3'
	assert f.next() == struct.pack('!HI',4,0) + 'data4dat'
	assert f.next() == struct.pack('!HI',4,8) + 'a4'
	try : n = f.next()
	except StopIteration : n = ''
	assert not n

def test_put():
	c = ContaineR()
	b = BuffeR()
	s = sock()
	p = Peer('content_id','handshake',c,b,sock=s,ip='127.0.0.1',port=struct.pack('!H',7668))
	p.put(1,0,'data')
	assert not p.requested
	p.requested = [Request(c,b,2,0,15,2)]
	p.put(2,0,struct.pack('!I',15) + 'data')
	p.put(4,0,'data4')
	assert len(p.requested[0].buffer) == 1
	p.put(2,4,'datadata')
	# not finished
	assert not p.requested[0].c[2]
	p.put(2,12,'dat')
	assert p.requested[0].buffer[2]['data'].len == 15
	# finished
	assert p.requested[0].c[2]
	assert 2 in b.buffer.keys()
	p.put(3,0,struct.pack('!I',5) + 'data1')
	assert 3 in b.buffer.keys()
	assert not p.requested
	assert c.h == [2,3]

#
def test_return():
	c = ContaineR()
	b = BuffeR()
	s = sock()
	p = Peer('content_id','handshake',c,b,sock=s,ip='127.0.0.1',port=struct.pack('!H',7668))
	r = Request(c,b,1,0,15,3)
	p.requested = [r]
	ret = p.return_requests()
	assert ret == [(1,0,15,3)]
	class data:
		def __init__(self,l):
			self.len = l

	r = Request(c,b,1,0,15,3)
	r.buffer[1] = {'data':data(10)}
	p.requested = [r]
	ret = p.return_requests()
	assert ret == [(1,10,15,3)]
	r = Request(c,b,1,0,15,3)
	r.c[1] = True
	r.buffer[1] = {'data':data(10)}
	r.buffer[2] = {'data':data(8)}
	p.requested = [r]
	ret = p.return_requests()
	assert ret == [(2,8,15,2)]
	p.requested = []
	ret = p.return_requests()
	assert ret == []
	r = Request(c,b,1,0,15,1)
	r.buffer[1] = {'data':data(10)}
	p.requested = [r]
	ret = p.return_requests()
	assert ret == [(1,10,15,1)]
#
#def test_timeout():
#	pass
#
def test():
	c = ContaineR()
	b = BuffeR()
	s = sock()
	p = Peer('content_id','handshake',c,b,sock=s,ip='127.0.0.1',port=struct.pack('!H',7668))
	p.handshaked = True
	p.request_peers()
	assert p.send_buffer == 'handshake' + struct.pack('!H',0) + struct.pack('!I',2) + GET_PEERS + struct.pack('!B',35)
	p.send_buffer = ''
	p.request_position()
	assert p.send_buffer == struct.pack('!I',1) + POSITION
	p.send_buffer = ''
	p.send_message(KEEP_ALIVE)
	assert p.send_buffer == struct.pack('!I',1) + KEEP_ALIVE





















