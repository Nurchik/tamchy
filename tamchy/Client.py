import multiprocessing,io,pickle,sqlite3,socket,logging,sys
import messages
from StreamBuffer import StreamBuffer
from Reactor import Reactor
from Peer import Peer
from Container import StreamContainer
import messages
from pickle import dump
from urllib2 import urlopen
from string import ascii_lowercase as al
from string import digits
from random import choice
from threading import Thread
from network import HTTPServer


IP_CHECKER = 'http://wtfismyip.com/text'

class Client:
	def __init__(self,buffering_units=3,debug=False):
		'''
		buffering_units -> This is a number of video stream units for buffering
		all peers list is saved in DB
		'''
		#Thread.__init__(self)
		#self.daemon = True
		self.logger = logging.getLogger('tamchy')
		self.logger.setLevel(logging.DEBUG)
		f = logging.FileHandler('tamchy.log')
		f.setLevel(logging.DEBUG if debug else logging.INFO)
		formatter = logging.Formatter('%(asctime)s -- %(name)s ( %(filename)s : %(lineno)d) -- %(message)s')
		f.setFormatter(formatter)
		self.logger.addHandler(f)
		self.peer_id = messages.generate_peer_id()
		self.work = True
		self.peers = []
		# content_id : Stream Container
		self.streams = {}
		# this dict will hold port:Server instance for this port
		self.ports = {}
		self.db = sqlite3.connect('DataBase.db')
		self.logger.info('Client started')
		# getting our external ip
		#try:
		#	ip = urlopen(IP_CHECKER).read().strip()
		#	self.logger.info('Obtained external IP')
		#	self.logger.debug('Obtained external IP - ' + ip)
		#except:
		#	self.logger.error('Cannot obtain external url')
		#	raise Exception("Cannot check url")
		self.ip = '127.0.0.1'
		self.http_server = HTTPServer(7668,self)

	def start_stream(self,name,source,filename,content_id=None,path='',bitrate=0,port=7889,nodes=[]):
		'''
		content_id, ip,port (additional ip,port -> [(ip1,port1),(ip2,port2)])
		'''
		payload = {}
		content_id = (content_id if content_id is not None else generate_c_id())
		payload['name'] = name
		payload['content_id'] = content_id
		payload['ip'] = self.ip
		payload['port'] = port
		payload['nodes'] = nodes
		try:
			with open(filename + '.tamchy','wb') as file:	
				# path + content_id + '.tamchy'
				#dump(payload, file)
				dump(payload, file)
			#self.logger.debug('Created tamchy file (' + (content_id + '.tamchy') + ') ' + 'at ' + path)
			self.logger.debug('Created tamchy file (' + filename + '.tamchy' + ')')
		except:
			#self.logger.error('Cannot create tamchy file (' + (content_id + '.tamchy') + ' ) ' + 'at ' + path)
			self.logger.error('Cannot create tamchy file (' + filename + '.tamchy' + ' )')
			raise Exception('Cannot create tamchy file')
		s = StreamContainer(payload,self.peer_id,port,source=source,is_server=True,ext_ip=self.ip)
		self.streams[content_id] = s
		self.logger.debug('New StreamContainer (' + content_id + ') added to streams')
		s.start()

	def open_stream(self,file,port=7889):
		try:
			with open(file,'rb') as file:
				info = pickle.load(file)
				self.logger.info('Successfully loaded tamchy file')
		except: 
			self.logger.error('Cannot open file')
			return
		s = StreamContainer(info,self.peer_id,port,ext_ip=self.ip)
		self.streams[info['content_id']] = s
		self.logger.debug('New StreamContainer (' + info['content_id'] + ') added to streams')
		s.start()

	def close(self):
		for i in self.streams.values():
			i.close()
		self.http_server.close()
		self.logger.info('Client terminated')

	def __contains__(self,stream_id):
		return stream_id in self.streams

	def get_stream(self,stream_id,buf_seconds):
		return self.streams[stream_id].B.get_stream(buf_seconds)

	def get_list_of_streams(self):
		d = []
		for id,container in self.streams.items():
			d.append((container.name,id))
		return d

	#def run(self):
	#	while self.work:
	#		pass


	#def connect_server(self,ip,port,handshake):
	#	sock = socket.create_connection((ip,port),20.0)
	#	p = Peer(sock=sock)

def generate_c_id(length=32):
	return ''.join([choice(al + digits) for i in xrange(length)])


#if __name__ == '__main__':
#	c = Client()
#	c.start_stream()
#	c.open_stream()
#	work = True
#	while work:
#		try: pass
#		except KeyboardInterrupt:
#			work = False
#			c.close()#
if __name__ == '__main__':
	t = sys.argv[1]
	c=Client(debug=True)
	if t == 's':
		c.start_stream('test','http://127.0.0.1:8080','umut',content_id='w5vi59e7iysc3uu60pn7gasxkwf3hecc')
	else:
		c.open_stream('umut.tamchy',port=6590)
	while True:
		pass