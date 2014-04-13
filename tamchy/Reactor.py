import threading,select,logging

TIMEOUT = 0.5

class Reactor(threading.Thread):
	def __init__(self,server):
		self.logger = logging.getLogger('tamchy.Reactor')
		threading.Thread.__init__(self)
		self.daemon = True
		self.peers = [server]
		self.lock = threading.Lock()
		self.work = True

	# raw_methods is created just to avoid logging
	def raw_add(self,peer):
		self.peers.append(peer)

	def raw_remove(self,peer):
		self.peers.remove(peer)

	def add(self,peer):
		with self.lock:
			self.peers.append(peer)
			self.logger.debug('Added peer (' + peer.raw_ip + ':' + str(peer.raw_port) + ')')

	def remove(self,peer):
		with self.lock:
			self.peers.remove(peer)
			self.logger.debug('Peer (' + peer.raw_ip + ':' + str(peer.raw_port) + ') removed')

	def run(self):
		self.logger.info('Reactor started')
		while self.work:
			#with self.lock:
			peers = self.peers
			try:
				r,w,e = select.select(peers,peers,peers,TIMEOUT)
			except:
				pass
			#r,w,e = select.select(peers,peers,peers,TIMEOUT)
			for peer in r:
				peer.handle_read()
			for peer in w:
				peer.handle_write()
			for peer in e:
				peer.handle_close()

	def close(self):
		self.work = False
		self.logger.info('Reactor terminated')

