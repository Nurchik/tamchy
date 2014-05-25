# -*- coding:utf-8 -*-

import sqlite3
import threading
import urllib2
import socket
import logging
import select
import pickle
from Reactor import Reactor
from Buffer import StreamBuffer
from Peer import Peer
from Server import Server
import messages
from time import time,sleep
from struct import pack,unpack
from StringIO import StringIO
import Input

DEFAULT_BUFFERING_SIZE = 128 * 1024
WRITE_WAIT_TIMEOUT = 15.0
READ_WAIT_TIMEOUT = 15.0
STREAM_PIECE_SIZE = 16384
COUNTER = (2 ** 16) - 1

def hex(t):
	return pack('!I',t)

def num(t):
	return unpack('!I',t)[0]


'''
:source is a http source for video to stream
'''

class StreamContainer:
	def __init__(self,grenade,info,peer_id,port,Server,max_connections=50,is_server=False,source='',bitrate=0,ext_ip='',debug=False):
		self.logger = logging.getLogger('tamchy.StreamContainer')
		#threading.Thread.__init__(self)
		self.daemon = True
		self.info = info
		self.grenade = grenade
		self.name = info['name']
		self.ext_ip = ext_ip
		self.peer_id = peer_id
		self.port = port
		self.is_server = is_server
		self.bitrate = bitrate
		self.content_id = info['content_id']
		self.handshake = messages.construct_handshake(self.content_id,peer_id,port)
		self.lock = threading.Lock()
		self.Server = Server
		self.B = StreamBuffer(self,grenade)
		self.work = True
		self.connected_peers = 0
		self.max_connections = max_connections
		self.peers = []
		self.start_pos = None
		self.add_new_stream()
		self.requests = []
		self.logger.debug('StreamContainer (' + self.content_id + ') created')
		# after receiving peers list, connection with server will be used in requesting stream
		# but program will not send any statistics to the server any more

		# this is source's initialisation -> we should check if it works
		self.source = self.select_for(source)
		if not is_server and not debug:
			if not self.connect_server(info['ip'],info['port']):
				self.grenade.pull('Cannot Connect to Server',self)

	def connect_server(self,ip,port):
		s = Peer(self.content_id,self.handshake,\
				 self,self.B,ip=ip,port=port,server=True)
		if s.socket is not None:
			# waiting for connection with timeout
			r,w,e = select.select([],[s],[],WRITE_WAIT_TIMEOUT)
			if not w:
				self.logger.error('Server connection timeout')
				return False
			s.request_peers()
			s.handle_write()
			# waiting for response from server to our request
			# we use select.select because we need unblocking method with timeout :)
			r,w,e = select.select([s],[],[],READ_WAIT_TIMEOUT)
			if not r:
				self.logger.error('Server does not respond')
				return False
			s.handle_read()
			if not s.handshaked:
				self.logger.error('Server incorrectly responded')
				return False
			self.logger.info('Server connection success')
			self.add(s)
			return True
		self.logger.error('Cannot connect to the server')
		return False

	def select_for(self,source):
		s = source.split(':')[0]
		if s == 'http':
			i = Input.HTTPInput(source)
			if i.con == None:
				self.grenade.pull('Cannot connect to source',self)
				return 
			# we must close connection to source until we need it later to prevent source's buffer overflow
			i.con.close()
			return i
		else:
			self.grenade.pull('Cannot Recognize Input source',self)

	def prepare_peers(self,peers):
		for i in xrange(len(peers)/6):
			raw = peers[0+i*6:6+i*6]
			ip,port = raw[0:4],raw[4:6]
			if '.'.join([str(i) for i in unpack('!BBBB',ip)]) == self.ext_ip \
						and unpack('!H',port)[0] == self.port:
				continue
			if self.can_add_peer():
				self.prepare_peer(ip=ip,port=port)	

	def prepare_peer(self,ip=None,port=None,sock=None,buf=''):
		if sock is None:
			ip = '.'.join([str(x) for x in unpack('!BBBB',ip)])
			port = unpack('!H',port)[0]
			p = Peer(self.content_id,self.handshake,self,self.B,ip=ip,port=port)
		else:
			p = Peer(self.content_id,self.handshake,self,self.B,ip=ip,port=port,sock=sock,buf=buf)
		self.add_new_peer(ip,port)
		self.add(p)

	def get_file(self):
		return StringIO(pickle.dumps(self.info))

	def add(self,peer):
		# if it's first peer's connection -> start working
		if not self.peers:
			self.work = True
			thr = threading.Thread(target=self.run,daemon=True)
			thr.start()

		self.peers.append(peer)
		self.Server.add(peer)
		self.logger.debug('Peer (' + peer.raw_ip + ':' + str(peer.raw_port) + ') added')

	def remove(self,peer):
		try:
			self.peers.remove(peer)
			self.Server.remove(peer)
		except:
			pass

		# if last peer is disconnected -> pause working
		if not self.peers:
			self.work = False

		self.logger.debug('Peer (' + peer.raw_ip + ':' + str(peer.raw_port) + ') removed')

	def can_add_peer(self):
		if self.is_server:
			return True
		return self.connected_peers < self.max_connections

	def get_seconds(self,peer):
		# for connection to the server we need request not more than 3 seconds at once,
		# to eliminate server overloading
		if peer.server:
			return 3
		
		speed = peer.upload_speed
		if speed < 100:
			return 3
		if speed >= 100 and speed < 300:
			return 5
		if speed >= 300 and speed < 500:
			return 8
		if speed >= 500 and speed < 750:
			return 12
		if speed >= 750 and speed < 1024:
			return 15
		if speed >= 1024 and speed < 2048:
			return 20
		if speed >= 2048 and speed < 3072:
			return 25
		if speed >= 3072 and speed < 4096:
			return 30
		if speed >= 4096 and speed < 5120:
			return 40
		if speed >= 5120:
			return 60

	def connect_nodes(self):
		for ip,port in self.info['nodes']:
			self.connect_server(ip,port)

	def run(self):
		if self.is_server:
			source = self.source
			# This is done to be able to watch http-stream in server machine
			# maybe it's not necessary but will be good )
			self.B.inited = True
			recon_tried = 0
			source.connect()
			i = 0
			while self.work:
				d = source.read()

				# there is a problem with source
				if not d:
					# we must reconnect 3 times before closing Container
					if recon_tried < 3:
						source.reconnect()
						recon_tried += 1
						continue
					else:
						self.grenade.pull('Cannot fetch data from Source',self)
						break

				# why len(d) + 4 ? Because we must to take into account 4 bytes of length
				# and if we didn't do that, everytime a piece transferred, trailing 4 bytes of data 
				# will not be recieved by peer
				d = pack('!I',len(d) + 4) + d				
				self.B.put(i,d)
				# i.e. 65535
				if i == COUNTER:
					i = 0
				else:
					i += 1
			# exiting
			source.close()

		else:
			requests = self.requests
			
			while self.start_pos is None:
				pass

			pos = self.start_pos
			
			# starting connection to nodes
			if self.info.get('nodes',[]):
				threading.Thread(target=self.connect_nodes,daemon=True).start()				
			
			self.logger.info('Started main loop')
			while self.work:
				# selecting only handshaked peers
				peers = filter(lambda x : x.handshaked,self.peers)
				for peer in sorted(peers,key=lambda x : x.upload_speed,reverse=True):
					if peer.closed:
						continue	
					elif peer.need_keep_alive:
						peer.send_keep_alive()
					elif peer.can_request:
						# peer has not job in queue and it's disconnected
						if peer.timeout:
							self.logger.debug('Connection with peer (' + peer.raw_ip + ':' \
								+ str(peer.raw_port) + ') timed out')
							peer.handle_close()	
							continue
						'''
						!!! Основная логика !!!
						'''
						c = True
						for req in requests:
							if peer.have(req[0]):
								self.logger.debug('Retry Request ' + str(req))
								peer.request_stream(req[0],req[1],req[2],req[3])
								requests.remove(req)
								c = False
								break
						# because we have to request only one "request" 
						if not c:
							continue
						# if there are no appropriate request in self.requests
						# or there are no requests in self.requests => try to request stream by position
						elif peer.have(pos):	
							s = self.get_seconds(peer)
							peer.request_stream(pos,0,STREAM_PIECE_SIZE,s)
							pos = pos + s
							# because we reset our counter after COUNTER => 
							# if we have pos = 65536 and COUNTER = 65536 => new pos will be 
							# equal to pos = 0, because pos = 65536 will not be appropriate for struct
							# format '!I' (max is 65535, you can check it!)
							if pos >= COUNTER:
								pos = pos - COUNTER	
					else:
						# peer has job in queue, but there is a timeout
						if peer.timeout:
							self.logger.debug('Request timeout of peer (' + peer.raw_ip + ':' \
								+ str(peer.raw_port) + ') has reached. Retrying request')
							requests.extend(peer.return_requests())

			else:
				self.grenade.pull('Cannot connect to server',self)

		self.logger.debug('Main loop stopped')

	def return_reqs(self,reqs):
		self.requests.extend(reqs)

	def set_pos(self,pos):
		if self.start_pos is None:
			self.start_pos = pos
			self.B.got_pos()

	def tell_have(self,t):
		for peer in self.peers:
			# don't send HAVE to server because it won't need this
			if peer.handshaked and not peer.server:
				peer.send_have(t)

#----------------------------------- DB methods ----------------------------

	def add_new_stream(self):
		db = sqlite3.connect('DataBase.db')
		try:
			db.execute('create table \"'+self.content_id+'\" (id integer primary key,ip text unique,port int)')
			db.commit()
			self.logger.debug('Table for stream (' + self.content_id + ') created in DB')
		except sqlite3.OperationalError:
			db.execute('drop table \"'+self.content_id+'\"')
			self.logger.debug('Previous table for stream (' + self.content_id + ') removed from DB')
			db.execute('create table \"'+self.content_id+'\" (id integer primary key,ip text unique,port int)')
			self.logger.debug('Table for stream (' + self.content_id + ') created in DB')
		db.close()

	def remove_stream(self):
		db = sqlite3.connect('DataBase.db')
		try:
			db.execute('drop table \"'+self.content_id+'\"')
		except:
			pass
		self.logger.debug('Table for stream (' + self.content_id + ') removed from DB')
		db.close()

	def add_new_peer(self,ip,port):
		db = sqlite3.connect('DataBase.db')
		db.text_factory = str
		try:
			db.execute('insert into \"'+self.content_id+'\" values(null,?,?)',(ip,port))
			db.commit()
			self.logger.info('Peer added to DB')
			self.logger.debug('Peer (' + ip + ':' + port + ') added to DB')
		# maybe ip is exist
		except sqlite3.IntegrityError:
			self.logger.info('Peer already exists')
		db.close()

	def delete_peer(self,ip):
		db = sqlite3.connect('DataBase.db')
		db.text_factory = str
		try:
			db.execute('delete from \"'+self.content_id+'\" where ip=?',(ip,))
			db.commit()
			self.logger.info('Peer removed from DB')
			self.logger.debug('Peer (' + ip + ') removed from DB')
		except sqlite3.OperationalError:
			self.logger.info('Peer does not exist')
		db.close()

	def get_peers(self,qty):
		db = sqlite3.connect('DataBase.db')
		db.text_factory = str
		"""
		Берилмелер Базасынан Content ID-ге ылайык peers_list тизмесин БЕРYY.
		syntax: [(peer_ip,port)]

		"""
		try:
			peers = [(ip + port) for id,ip,port in db.execute('select * from \"'+self.content_id+'\"').fetchmany(size=qty)]
		except sqlite3.OperationalError:
			peers=[]
		peers_list = ''.join(peers)
		db.close()
		return peers_list

#-------------------------------------------------------------------------------------------

	def close(self):
		self.work = False
		self.remove_stream()
		for peer in self.peers:
			peer.notify_closing()
		self.B.close()
		self.logger.debug('StreamContainer (' + self.content_id + ') terminated')

# Testing

class PeeR:
	def __init__(self,id):
		self.id = id
		self.handshaked = False
		self.upload_speed = 0
		self.req = []
		self.server = False
		self.h = []
		self.closed = False
		self.need = False
		self.ka = False
		self.can = True
		self.t = False
		self.closed = False
		self.p = []
		self.raw_ip = 'ip'
		self.raw_port = 345

	@property
	def timeout(self):
		return self.t

	@property
	def can_request(self):
		return self.can

	@property	
	def need_keep_alive(self):
		return self.need

	def send_keep_alive(self):
		self.ka = True

	def handle_close(self):
		self.closed = True

	def have(self,pos):
		return pos in self.p

	def request_stream(self,pos,offset,length,seconds):
		self.req.append((pos,offset,length,seconds))
		self.can = False

	def return_requests(self):
		r = self.req[:]
		self.req = []
		return r


def g(i,p):
	return True

def test_run():
	s = StreamContainer('grenade',{'content_id':'content_id','ip':'123','port':'233','name':'wewf'},'peer_id',6590,'Reactor',debug=True)
	s.connect_server = g
	s.start_pos = 65534
	
	p1 = PeeR(1)
	p1.handshaked = True
	p1.upload_speed = 2
	p1.t = True
	
	p2 = PeeR(2)
	p2.handshaked = True
	p2.upload_speed = 6
	p2.can = False
	p2.req = [(2,34,65,12)]
	p2.t = True
	
	p3 = PeeR(3)
	p3.handshaked = True
	p3.p = [65535,65536,0,1,2]
	
	p4 = PeeR(4)
	p4.handshaked = True
	p4.upload_speed = 5
	p4.p = [2]
	
	p5 = PeeR(5)
	p5.handshaked = True
	p5.upload_speed = 1
	p5.can = False
	p5.need = True
	
	p6 = PeeR(6)
	p6.handshaked = True
	p6.upload_speed = 7
	p6.can = False
	p6.need = True
	
	p7 = PeeR(7)
	p7.handshaked = True
	p7.p = [2]
	p7.upload_speed = 4
	
	p8 = PeeR(8)
	p8.handshaked = True
	p8.upload_speed = 8
	p8.p = [1]
	
	p9 = PeeR(9)
	p9.handshaked = True
	p9.upload_speed = 3
	p9.can = False
	p9.t = True
	
	p10 = PeeR(10)
	
	p11 = PeeR(11)

	p12 = PeeR(12)
	p12.handshaked = True
	p12.upload_speed = 9
	p12.p = [65534]
	
	s.peers = [p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12]
	s.run()
	#sleep(1.0)
	s.work = False
	assert p12.req == [(65534,0,STREAM_PIECE_SIZE,3)]
	assert not p8.closed
	assert p8.req == [(1,0,STREAM_PIECE_SIZE,3)]
	assert p6.ka
	assert not p2.req
	assert p4.req == [(2,34,65,12)]
	assert not p7.ka
	assert not p9.closed
	assert p1.closed
	assert p5.ka





def test_DB():
	pass





















