import socket,struct,pickle,logging,time
from Peer import Peer
from Reactor import Reactor

KEEP_ALIVE = struct.pack('!B',0)
POSITION = struct.pack('!B',1)
GET_PEERS = struct.pack('!B',2)
GET_STREAM = struct.pack('!B',3)
STOP = struct.pack('!B',4)
CLOSE = struct.pack('!B',5)
ERROR = struct.pack('!B',6)
HAVE = struct.pack('!B',7)

class TempPeer:
	def __init__(self,ip,sock,Server):
		self.logger = logging.getLogger('tamchy.Server.TempPeer')
		self.socket = sock
		self.Server = Server
		self.read_buffer = ''
		self.ip = ip
		self.time = time.time()

	def fileno(self):
		try:
			s = self.socket.fileno()
			return s
		except:
			return self.handle_close()

	def handle_read(self):
		message = ''
		while True:
			try:
				m = self.socket.recv(8192)
				if not m: 
					return self.handle_close()
				message += m
			except:
				break

		if not message:
			return self.handle_close()

		self.read_buffer += message
		length = self.read_buffer[:4]

		if len(length) < 4:
			# this is not entire message => wait for remaining part
			return 
		length = struct.unpack('!I',length[:4])[0]
		if length > 32*1024:
			return self.handle_close()
		msg = self.read_buffer[4:4 + length]
		if len(msg) < length:
			# this is not entire message => wait for remaining part
			return 

		self.read_buffer = self.read_buffer[4 + length:]
		#
		# Start of main logic to handle messages from peer
		#
		if (msg[:12]).lower() == 'salamatsyzby':
			content_id = msg[12:44]
			ip = ''.join([struct.pack('!B',int(x)) for x in self.ip.split('.')])
			port = msg[64:66]
			if content_id not in self.Server.streams:
				# try to send message, but we must close the connection anyway -> whether error or not
				try:
					self.socket.send(struct.pack('!I',19) + ERROR + pickle.dumps('Invalid Content ID'))
				except:
					pass
				self.logger.debug('Peer (%s) disconnected' % (self.ip))
				return self.handle_close()
			else:
				# Everything is good with this peer => we must add this peer to the Container's peers list
				self.logger.debug('Peer (%s) successfully connected' % (self.ip))
				self.Server.accept(self.socket,ip,port,content_id,self.read_buffer,self)
		else:
			self.logger.debug('Peer (%s) disconnected' % (self.ip))
			return self.handle_close()

	def handle_write(self):
		pass

	def handle_close(self):
		self.socket.close()
		self.Server.remove(self)

class Server:
	def __init__(self):
		pass

	def fileno(self):
		return self.socket.fileno()

	def handle_read(self):
		cl,addr = self.socket.accept()
		self.logger.debug('Got connection from new peer (' + addr[0] + ')')
		if self.C.can_add_peer():
			self.C.prepare_peer(sock=cl)
		else:
			cl.send(self.build_message('\x07',pickle.dumps('Reached Peers Limit')))
			cl.close()
			self.logger.debug('Rejected connection of new peer (' + addr[0] + ')')

	def build_message(self,id,data=''):
		length = struct.pack('!I',len(id+data))
		return length+id+data

	def close(self):
		self.Reactor.close()
		self.socket.close()

		
class MultiServer(Server):
	def __init__(self,port,debug=False):
		self.logger = logging.getLogger('tamchy.Server')
		# !!! self.socket = sock
		self.Reactor = Reactor(self)
		self.work = True
		self.raw_ip = 'SERVER'
		self.raw_port = port
		if not debug:
			self.socket = self.create_socket(port)
			self.Reactor.start()
		# 'content_id':StreamContainer instance
		self.streams = {}

		self.logger.info('Server on port ' + str(port) + ' started')

	def create_socket(self,port):
		sock = socket.socket()
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind(('',port))
		sock.setblocking(0)
		sock.listen(5)
		return sock

	def handle_read(self):
		cl,addr = self.socket.accept()
		self.logger.debug('Got connection from new peer (' + addr[0] + ')')
		peer = TempPeer(addr[0],cl,self)
		self.Reactor.add(peer)

		# checking for dead TempPeers
		for peer in self.Reactor.peers:
			if isinstance(peer,TempPeer):
				# 300 seconds = 5 minutes
				if time.time() - peer.time > 300.0:
					self.Reactor.remove(peer)

	def accept(self,sock,ip,port,content_id,buf,peer):
		C = self.streams[content_id]
		if C.can_add_peer():
			self.remove(peer)
			C.prepare_peer(ip=ip,port=port,sock=sock,buf=buf)
		else:
			peer.handle_close()

	def add(self,peer):
		self.Reactor.add(peer)

	def remove(self,peer):
		self.Reactor.remove(peer)

	def register_stream(self,container):
		self.streams[container.content_id] = container
		self.logger.debug('Stream Container ({0}) registered'.format(container.content_id,))

	def unregister_stream(self,container):
		try:
			del self.streams[container.content_id]
			self.logger.debug('Stream Container ({0}) unregistered'.format(container.content_id,))
		except:
			pass


# Testing

class sock:
	def __init__(self):
		self.closed = False
		self.buffer = []
		self.s_buf = ''
		self.r_buf = []

	def close(self):
		self.closed = True
	
	def fileno(self):
		return 0
	
	def accept(self):
		return (sock(),('127.0.0.1',7654))

	def getpeername(self):
		return ('0.0.0.0',123)

	def send(self,data):
		self.s_buf += data
		return len(data)

	def recv(self,num):
		msg = self.r_buf.pop(0)
		if msg == 'except':
			raise Exception
		return msg

class C:
	def __init__(self,c_id):
		self.can = True
		self.prepared = []
		self.content_id = c_id
	def can_add_peer(self):
		return self.can
	def prepare_peer(self,sock=None,ip=None,port=None,buf=''):
		self.prepared.append((ip,port))

class PeeR:
	def __init__(self):
		self.closed = False
	def handle_close(self):
		self.closed = True

class SeRver:
	def __init__(self):
		self.streams = {}
		self.accepted = []
	def remove(self,peer):
		pass
	def accept(self,sock,ip,port,content_id,buf,peer):
		self.accepted.append(sock)


def test_server():
	s = MultiServer(7668,debug=True)
	c1 = C('content_id1')
	c2 = C('content_id2')
	c3 = C('content_id3')
	c4 = C('content_id4')
	s.register_stream(c1)
	s.register_stream(c2)
	s.register_stream(c3)
	s.register_stream(c4)
	assert len(s.streams) == 4
	s.unregister_stream(c3)
	assert len(s.streams) == 3
	assert 'content_id2' in s.streams
	assert 'content_id3' not in s.streams
	s.unregister_stream(c1)
	assert len(s.streams) == 2
	assert 'content_id1' not in s.streams
	# test_accept
	sct = sock()
	assert not c2.prepared
	s.accept(sct,'127.0.0.1',7665,'content_id2','',PeeR())
	assert c2.prepared
	assert c2.prepared[0] == ('127.0.0.1',7665)
	p = PeeR()
	s.accept(sct,'127.0.0.1',7667,'content_id2','',p)
	s.accept(sct,'127.0.0.1',7668,'content_id4','',PeeR())
	assert not p.closed
	assert c2.prepared[1] == ('127.0.0.1',7667)
	assert c4.prepared[0] == ('127.0.0.1',7668)
	p1 = PeeR()
	p2 = PeeR()
	c2.can = False
	assert len(c2.prepared) == 2
	assert not p.closed
	s.accept(sct,'127.0.0.1',7669,'content_id2','',p1)
	s.accept(sct,'127.0.0.1',7678,'content_id2','',p2)
	assert len(c2.prepared) == 2
	assert p1.closed
	assert p2.closed

def test_temp_peer():
	server = SeRver()
	s = sock()
	p = TempPeer('127.0.0.1',s,server)
	s.r_buf.append('except')
	p.handle_read()
	assert s.closed
	s.closed = False
	s.r_buf = ['abc', 'except'] 
	p.handle_read()
	assert p.read_buffer == 'abc'
	s.closed = False
	s.r_buf = ['', 'except'] 
	p.handle_read()
	assert s.closed
	s.closed = False
	s.r_buf = [struct.pack('!I',33*1024),'except']
	p.handle_read()
	assert s.closed
	s.closed = False
	p.read_buffer = ''
	s.r_buf = [struct.pack('!I',4)+'abc','except']
	p.handle_read()
	assert len(p.read_buffer) == 7
	assert not s.closed
	s.r_buf = [struct.pack('!I',4)+'abcd','except']
	p.handle_read()
	assert s.closed
	s.closed = False
	p.send_buffer = ''
	p = TempPeer('127.0.0.1',s,server)
	s.r_buf = [struct.pack('!I',23)+'Salamatsyzby'+'_content_id','except']
	p.handle_read()
	assert s.closed

	server.streams = {'content_id1234567890123456789014':'StreamContainer'}
	s = sock()
	p = TempPeer('127.0.0.1',s,server)
	s.r_buf = [struct.pack('!I',68)+'Salamatsyzby' + 'content_id1234567890123456789012' + 'peeeeeerrrrrr_iiiidd' + '1234','except']
	p.handle_read()
	assert s.closed
	assert s.s_buf[7:25] == 'Invalid Content ID'
	
	s = sock()
	p = TempPeer('127.0.0.1',s,server)
	s.r_buf = [struct.pack('!I',68)+'Salamatsyzby' + 'content_id1234567890123456789012' + 'peeeeeerrrrrr_iiiidd12' + '34',\
				struct.pack('!I',2) ,'except']
	assert not s.s_buf
	p.handle_read()
	assert s.closed
	assert s.s_buf
	assert not server.accepted

	s = sock()
	p = TempPeer('127.0.0.1',s,server)
	s.r_buf = [struct.pack('!I',68)+'Salamatsyzby' + 'content_id1234567890123456789014' + 'peeeeeerrrrrr_iiiidd12' + '45',\
				struct.pack('!I',2) ,'except']
	p.handle_read()
	assert not s.closed
	assert not s.s_buf
	assert server.accepted == [s]



















