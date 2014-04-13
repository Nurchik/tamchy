import socket,struct,pickle,logging
from Peer import Peer

class Server:
	def __init__(self,Container,port=6590):
		self.logger = logging.getLogger('tamchy.Server')
		sock = socket.socket()
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind(('',port))
		sock.setblocking(0)
		sock.listen(5)
		self.socket = sock
		self.work = True
		self.C = Container
		self.raw_ip = 'SERVER'
		self.logger.info('Server on port ' + str(port) + ' started')

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

	#def register(self,container):
	#	pass
#
	#def unregister(self,container):
	#	pass

# Testing

class sock:
	def __init__(self):
		self.closed = False
		self.buffer = []
	def send(self,msg):
		self.buffer.append(msg)
	def close(self):
		self.closed = True
	def fileno(self):
		return 0
	def accept(self):
		return sock(),''

class C:
	def __init__(self):
		self.can = True
	def can_add_new_peer(self):
		return self.can
	def prepare_peer(self,sock=None,ip=None):
		pass


def test():
	pass