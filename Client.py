# -*- coding:utf-8 -*-
import multiprocessing,io,pickle,sqlite3,socket,logging,sys,os
import messages
from StringIO import StringIO
from Reactor import Reactor
from Container import StreamContainer
from Server import MultiServer as Server
import messages
from pickle import dump
from urllib2 import urlopen
from string import ascii_lowercase as al
from string import digits
from random import choice
from threading import Thread
from network import HTTPServer
import cherrypy
from jinja2 import Environment, FileSystemLoader
from cherrypy.lib.static import serve_fileobj

import os.path

STATIC_CONFIG = {
				'/' : {'tools.staticdir.root' : os.path.abspath('static')},
				'/css' : {'tools.staticdir.on' : True , 'tools.staticdir.dir' : 'css'},
				'/js' : {'tools.staticdir.on' : True , 'tools.staticdir.dir' : 'js'},
				'/images' : {'tools.staticdir.on' : True , 'tools.staticdir.dir' : 'images'}
				}


IP_CHECKER = 'http://wtfismyip.com/text'
BUFFERING_SECONDS = 30
HTTP_PORT = 8001


# this class will catch any fatal error while program is running
# and then it will gracefully close Container instance

class grenade:
	def __init__(self,Client):
		self.C = Client
		self.pulled = False
		self.logger = logging.getLogger('tamchy.GRENADE')
		self.error = ''

	def pull(self,error,container):
		server = container.Server
		server.unregister_stream(container)
		container.close()
		self.pulled = True
		# maybe container is not created
		try:
			del self.C._streams[container.content_id]
		except:
			pass

		# if there are no more StreamContainers
		if not server.streams:
			server.close()
			del self.C.ports[server.raw_port]

		self.error = error
		self.logger.error(error)

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
		self._streams = {}
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
		#self.http_server = HTTPServer(7668,self)
		# 'port':Server instance
		self.ports = {}
		self.env = Environment(loader=FileSystemLoader('templates'))
		self.urls = {}
		if not debug:
			self.start_http_server()
		self.grenade = grenade(self)

	def create_urls_tree(self):
		urls = {}
		host = cherrypy.config['server.socket_host']
		port = cherrypy.config['server.socket_port']

		urls['index'] = 'http://{0}:{1}/'.format(host,port)
		urls['create_stream'] = 'http://{0}:{1}/create_stream'.format(host,port)
		urls['open_stream'] = 'http://{0}:{1}/open_stream'.format(host,port)
		urls['list'] = 'http://{0}:{1}/streams'.format(host,port)
		urls['exit'] = 'http://{0}:{1}/exit'.format(host,port)

		return urls

	def validate(self,c_id):
		if len(c_id) == 32:
			return True
		return False

	def _create_stream(self,name,source,content_id='',bitrate=0,port=7889,nodes=[]):
		'''
		content_id, ip,port (additional ip,port -> [(ip1,port1),(ip2,port2)])
		'''
		payload = {}
		content_id = (content_id if self.validate(content_id) else generate_c_id())
		payload['name'] = name
		payload['content_id'] = content_id
		payload['ip'] = self.ip
		payload['port'] = port
		payload['nodes'] = nodes
		
		server = self.ports.get(port,'')
		if not server:
			try:
				server = Server(port)
			# we cannot create socket on given port
			except:
				raise Exception('Cannot Create Server on given port. Give another port')
			self.ports[port] = server

		s = StreamContainer(self.grenade,payload,self.peer_id,port,server,source=source,is_server=True,ext_ip=self.ip)
		# if source is None => something went wrong with connection to source
		# and grenade was pulled
		if not self.grenade.pulled:
			self._streams[content_id] = s
			
			server.register_stream(s)

			self.logger.debug('New StreamContainer (' + content_id + ') added to streams')
			#s.start()
		else:
			raise Exception(self.grenade.error)
	
	def _open_stream(self,file,port=7889):
		#try:
		#	file = open(file,'rb')
		try:
			info = pickle.load(file)
			self.logger.info('Successfully loaded tamchy-file')
		except:
			raise Exception('Error. Corrupted tamchy-file')
		#except: 
		#	self.logger.error('Cannot open file')
		#	raise Exception('Cannot open tamchy-file')
		
		server = self.ports.get(port,'')
		if not server:
			try:
				server = Server(port)
			# we cannot create socket on given port
			except:
				raise Exception('Cannot Create Server on given port. Give another port')
			self.ports[port] = server
		
		s = StreamContainer(self.grenade,info,self.peer_id,port,server,ext_ip=self.ip)
		if not self.grenade.pulled:
			self._streams[info['content_id']] = s
			
			server.register_stream(s)
	
			self.logger.debug('New StreamContainer (' + info['content_id'] + ') added to streams')
			#s.start()
		else:
			raise Exception(self.grenade.error)

	def close(self):
		for i in self._streams.values():
			i.close()
		self.stop_http_server()
		self.logger.info('Client terminated')

	def __contains__(self,stream_id):
		return stream_id in self._streams

	def get_stream(self,stream_id,buf_seconds):
		return self._streams[stream_id].B.get_stream(buf_seconds)

	def get_list_of_streams(self):
		d = []
		for id,container in self._streams.items():
			d.append((id,container.name))
		return d

	def start_http_server(self):
		#cherrypy.config.update({'server.socket_host': '127.0.0.1','server.socket_port': HTTP_PORT})
		cherrypy.config.update({'server.socket_host': '127.0.0.1','server.socket_port': HTTP_PORT,'environment': 'production'})
		cherrypy.tree.mount(self,'/',config=STATIC_CONFIG)

		#self.urls = self.create_urls_tree()

		cherrypy.engine.start()

	def stop_http_server(self):
		cherrypy.engine.exit()

	# -------------------------- CherryPy HTTP methods ---------------------------------------
	
	@cherrypy.expose
	def index(self):
		tmpl = self.env.get_template('tamchy/index.html')
		return tmpl.render(errors=[])

	@cherrypy.expose
	def streams(self,id=None):
		tmpl = self.env.get_template('tamchy/streams.html')
		if id is None:
			return tmpl.render(streams=self.get_list_of_streams(),errors=[])
		
		stream = self._streams.get(id,None)
		if stream is None:
			raise cherrypy.HTTPError(404,'No matching stream')
		
		tmpl = self.env.get_template('tamchy/stream.html')
		return tmpl.render(stream=stream,errors=[])

	@cherrypy.expose
	def open_stream(self,stream_file=None):
		tmpl = self.env.get_template('tamchy/open_stream.html')
		if cherrypy.request.method == 'GET':
			return tmpl.render(errors=[],success=False)
		# POST
		else:
			if stream_file is not None:
				# checking extension of a file
				if stream_file.filename.split('.')[-1] != 'tamchy':
					return tmpl.render(errors=['Unknown type of the file. Please check the extension'],success=False)
				try:
					self._open_stream(stream_file.file)
				except Exception as e:
					return tmpl.render(errors=[e.message],success=False)
				return tmpl.render(errors=[],success=True)
			else:
				return tmpl.render(errors=['Please select tamchy-file'],success=False)

	@cherrypy.expose
	def create_stream(self,name=None,source=None,content_id='',port=7889):
		tmpl = self.env.get_template('tamchy/create_stream.html')
		if cherrypy.request.method == 'GET':
			return tmpl.render(errors=[],success=False)
		
		else:
			# form-filling check!
			for arg in (name,source):
				if not arg:
					return tmpl.render(errors=['Please fill all of the fields'],success=False)

			try:
				self._create_stream(name,source,content_id,int(port))
			except Exception as e:
				return tmpl.render(errors=[e.message],success=False)
			return tmpl.render(errors=[],success=True)	

	@cherrypy.expose
	def exit(self):
		self.work = False
		return 'Goodbye!'

	@cherrypy.expose
	def delete(self,id):
		tmpl = self.env.get_template('tamchy/delete.html')
		container = self._streams.get(id,None)
		if container is None:
			raise cherrypy.HTTPError(404,'No matching stream')
		self.grenade.pull('User deleted Stream',container)
		return tmpl.render(errors=[])

	@cherrypy.expose
	def file(self,id=None,fmt='tamchy'):		
		stream = self._streams.get(id,None)
		if stream is None:
			raise cherrypy.HTTPError(404,'No matching stream')

		if fmt == 'tamchy':
			return serve_fileobj(stream.get_file(), "application/x-download", disposition='attachment',name=stream.name + '.tamchy')

		if fmt == 'playlist':
			playlist = '''#EXTM3U\n#EXTINF:-1, {0}\n{1}'''.format(stream.name,'http://127.0.0.1:' + str(HTTP_PORT) + '/stream/' + id)
			return serve_fileobj(StringIO(playlist), "application/x-download", disposition='attachment',name=stream.name + '.m3u')

	@cherrypy.expose
	def stream(self,id):
		cherrypy.response.headers['Content-Type'] = 'application/octet-stream'
		cherrypy.response.stream = True

		if id not in self._streams:
			raise cherrypy.HTTPError(404,'No matching stream')

		stream = self.get_stream(id,BUFFERING_SECONDS)
		while True:
			# this is http video stream
			return stream
#
	# -*-*-*-*-*-*-*-*-*-*-*-*-* CherryPy HTTP methods -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-


def generate_c_id(length=32):
	return ''.join([choice(al + digits) for i in xrange(length)])

# ---------------------------------------- Testing ----------------------------------------------------
class SC:
	def __init__(self,grenade,payload,peer_id,port,Reactor,source,is_server,ext_ip):
		self.content_id = 'content_id'
		if payload['name'] == 'success':
			grenade.pulled =  False
		else:
			grenade.pulled = True

def test():
	# this is a little hack to replace imported StreamContainer with another class to make tests
	global StreamContainer
	StreamContainer = SC
	
	c = Client(debug=True)
	assert not c.ports
	assert not c._streams
	c._create_stream('success','source')
	assert c._streams
	assert len(c.ports) == 1
	c._create_stream('success','source1')
	assert len(c._streams) == 2
	assert len(c.ports) == 1
	c._create_stream('success','source2')
	assert len(c._streams) == 3
	assert len(c.ports) == 1
	c._create_stream('success','source3',port=5463)
	assert len(c._streams) == 4
	assert len(c.ports) == 2
	try:
		c._create_stream('fail','source3',port=5463)
	except:
		pass
	assert len(c._streams) == 4
	assert len(c.ports) == 2
	try:
		c._create_stream('fail','source3',port=5465)
	except:
		pass
	assert len(c._streams) == 4
	assert len(c.ports) == 3
	err = False
	try:
		c._create_stream('success','source3',port=5465435345)
	except:
		err = True
	assert len(c._streams) == 4
	assert len(c.ports) == 3
	assert err
# -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* Testing -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-

if __name__ == '__main__':
	c = Client()
	while c.work:
		pass
	# user clicked exit button
	c.close()
	# 1 --> SIGHUP
	os.kill(os.getppid(),1)

	#t = sys.argv[1]
	#c=Client(debug=True)
	#if t == 's':
	#	c._create_stream('test','http://127.0.0.1:8080',content_id='w5vi59e7iysc3uu60pn7gasxkwf3hecc')
	#else:
	#	file = open('umut.tamchy','rb')
	#	c._open_stream(file,port=6590)
	#while True:
	#	pass