# -*- coding:utf-8 -*-
import multiprocessing,io,pickle,sqlite3,socket,logging,sys,random,re
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
import cherrypy
from jinja2 import Environment, FileSystemLoader
from cherrypy.lib.static import serve_fileobj
import os.path
from upnp import UPNP
from Server import MultiServer

STATIC_CONFIG = {
                '/' : {'tools.staticdir.root' : os.path.abspath('static')},
                '/css' : {'tools.staticdir.on' : True , 'tools.staticdir.dir' : 'css'},
                '/js' : {'tools.staticdir.on' : True , 'tools.staticdir.dir' : 'js'},
                '/images' : {'tools.staticdir.on' : True , 'tools.staticdir.dir' : 'images'}
                }


pattern = re.compile(r'[a-zA-Z0-9]{32}')

class PeerStorage:
    def __init__(self,max_con = 200,max_con_by_stream=50):
        self.peers = []
        self.MC = max_con
        self.MCBS = max_con_by_stream
        self.logger = logging.getLogger('tamchy.PeerStorage')

    def get_peers(self,content_id):
        to_remove = []
        for peer in self.peers:
            if peer.closed:
                to_remove.append(peer)
                continue
            elif peer.timeout:
                peer.handle_close()
                to_remove.append(peer)
                self.logger.debug('Connection with peer (%s) timed out' % (peer))
                continue
            else:
                if peer.content_id == content_id:
                    yield peer
        for peer in to_remove:
            self.remove(peer)

    def add(self,peer):
        # if it's first peer's connection -> start working
        self.peers.append(peer)
        self.logger.debug('Peer (%s) added' % (peer))

    def remove(self,peer):
        try:
            self.peers.remove(peer)
        except:
            pass
        self.logger.debug('Peer (%s) removed' % (peer))

    def can_add_peer(self,content_id=None):
        # if content_id is not specified we will limit number of peers by mx_con
        # else - by max_con_by_stream
        if not content_id:
            return len(self.peers) < self.MC
        else:
            return len(filter(lambda x : x.content_id == content_id,self.peers)) < self.MCBS

    @property
    def connected_peers(self):
        return len(self.peers)

    def run(self):
        self.logger.info('PeerStorage Reactor started')
        while self.work:
            #with self.lock:
            peers = self.peers
            try:
                r,w,e = select.select(peers,peers,peers,TIMEOUT)
            except:
                continue
            #r,w,e = select.select(peers,peers,peers,TIMEOUT)
            for peer in r:
                peer.handle_read()
            for peer in w:
                peer.handle_write()
            for peer in e:
                peer.handle_close()

    def start_serving(self):
        t = threading.Thread(target=self.run)
        t.daemon = True
        t.start()
        
    def close(self):
        self.work = False
        self.logger.info('PeerStorage Reactor terminated')


class ConfigLoader:
    '''
    Configuration file must be stored in tamchy.conf file in same folder as tamchy.py
    format must be -> KEY = VALUE <- and each key,value must be separated with newline(\n)
    '''
    def __init__(self):
        self.default_config = {
            'INCOMING_PORT' : 7890,
            'HTTP_HOST' : '127.0.0.1',
            'HTTP_PORT' : 8001,
            'BUFFERING_SECONDS' : 30,
            'IP_CHECKER' : 'http://wtfismyip.com/text',
            'DEBUG' : 0,
            'LOG_FILE' : 'tamchy.log',
            }
        self.config = self.parse_config()

    def parse_config(self):
        try:
            f = io.open('tamchy.conf','r')
        # maybe conf-file does not exist or it's not accessible
        except:
            return self.default_config
        c = {}
        for line in f:
            # trimming all whitespaces
            line = line.replace(' ','')
            # why if? maybe line is empty (last line)
            if line:
                key,value = line.split('=')
                # just making uppercase
                key = key.upper()
                c[key] = value
        f.close()
        return c

    def __setitem__(self,key,value):
        self.config[key.upper()] = value

    def __getitem__(self,key):
        if key in self.config:
            return self.config[key]
        # maybe key is left default in config-file
        else:
            self.default_config.get(key,'')

    def __delitem__(self,key):
        if key in self.config:
            del self.config[key]

class Client:
    def __init__(self,debug=False):
        '''
        buffering_units -> This is a number of video stream units for buffering
        all peers list is saved in DB
        '''
        self.config = ConfigLoader()
        
        self.logger = logging.getLogger('tamchy')
        self.logger.setLevel(self.config['DEBUG'])
        f = logging.FileHandler(self.config['LOG_FILE'])
        f.setLevel(self.config['DEBUG'])
        formatter = logging.Formatter('%(asctime)s -- %(name)s ( %(filename)s : %(lineno)d) -- %(message)s')
        f.setFormatter(formatter)
        self.logger.addHandler(f)
        
        #self.peer_id = messages.generate_peer_id()
        self.work = True
        # content_id : Stream Container
        self._streams = {}
        # this dict will hold port:Server instance for this port
        self.ports = {}
        self.logger.info('Client started')

        # getting our external ip
        self.ip = self.get_ext_ip() if not debug else 'ip'
        self.http = HTTPEngine(self)
        self.PStorage = PeerStorage()
        #self.Reactor = Reactor(self.PStorage)
        if not debug:    
            self.http.start_http_server()
            
            u = UPNP()
            port = self.config['INCOMING_PORT']
            # we will try to map same external port to internal port
            u.add_port_mapping(port,port)

            self.PStorage.start_serving()

    def get_ext_ip(self):
        u = UPNP()
        ip = u.get_external_ip()
        if not ip:
            try:
                ip = urlopen(self.config['IP_CHECKER']).read().strip()
            except:
                self.logger.error('Cannot obtain external ip')
                raise Exception("Cannot obtain external ip")
        self.logger.debug('Obtained external IP - ' + ip)
        return ip

    def validate(self,c_id):
        if (len(c_id) == 32) and pattern.match(c_id):
            return True
        return False

    def check(self,source,port,nodes):
        errors = ''
        if port not in xrange(0,65536):
            errors += 'Incorrect incoming port\n'
        return errors

    def create_stream(self,name,source,content_id='',bitrate=0,port=7889,nodes=[],chunk_length=16384):
        '''
        content_id, ip,port (additional ip,port -> [(ip1,port1),(ip2,port2)])
        '''
        errors = self.check(source,port,nodes)
        if errors:
            raise Exception(errors)
        payload = {}
        content_id = (content_id if self.validate(content_id) else generate_c_id())
        payload['name'] = name
        payload['content_id'] = content_id
        payload['ip'] = self.ip
        payload['port'] = port
        payload['nodes'] = nodes
        payload['chunk_length'] = chunk_length
        
        server = self.ports.get(port,'')
        if not server:
            try:
                server = Server(port,self.PStorage)
                self.PStorage.add(server)
            # we cannot create socket on given port
            except:
                raise Exception('Cannot Create Server on given port. Give another port')
            self.ports[port] = server

        s = StreamContainer(self,self.PStorage,payload,port,source=source,is_server=True,ext_ip=self.ip)
        # if exception was raised the code below will not run
        self._streams[content_id] = s
        server.register_stream(s)
        self.logger.debug('New StreamContainer (' + content_id + ') added to streams')
    
    def open_stream(self,file,port=7889):
        #try:
        #   file = open(file,'rb')
        try:
            info = pickle.load(file)
            self.logger.info('Successfully loaded tamchy-file')
        except:
            raise Exception('Error. Corrupted tamchy-file')
        #except: 
        #   self.logger.error('Cannot open file')
        #   raise Exception('Cannot open tamchy-file')
        
        server = self.ports.get(port,'')
        if not server:
            try:
                server = Server(port,self.PStorage)
                self.PStorage.add(server)
            # we cannot create socket on given port
            except:
                raise Exception('Cannot Create Server on given port. Give another port')
            self.ports[port] = server
        
        s = StreamContainer(self,self.PStorage,info,port,ext_ip=self.ip)
        self._streams[info['content_id']] = s
        server.register_stream(s)
        self.logger.debug('New StreamContainer (' + info['content_id'] + ') added to streams')

    def close_container(self,container):
        server = container.Server
        server.unregister_stream(container)
        container.close()
        # maybe container is not created
        try:
            del self._streams[container.content_id]
        except:
            pass

        # if there are no more StreamContainers
        if not server.streams:
            # closing incoming port
            server.close()
            del self.ports[server.port]

        self.logger.debug('StreamContainer (%s) closed.' % (container.content_id))

    def close(self):
        for i in self._streams.values():
            i.close()
        self.stop_http_server()
        self.logger.info('Client terminated')

    def __contains__(self,stream_id):
        return stream_id in self._streams

    def get(self,id):
        return self._streams.get(id,None)

    def get_stream(self,stream_id,buf_seconds):
        return self._streams[stream_id].B.get_stream(buf_seconds)

    def get_list_of_streams(self):
        d = []
        for id,container in self._streams.items():
            d.append((id,container.name))
        return d

    
class HTTPEngine:
    def __init__(self,Client):
        self.Client = Client
        self.config = Client.config
        
        self.logger = logging.getLogger('tamchy.HTTPEngine')
        self.logger.setLevel(self.config['DEBUG'])
        f = logging.FileHandler(self.config['LOG_FILE'])
        f.setLevel(self.config['DEBUG'])
        formatter = logging.Formatter('%(asctime)s -- %(name)s ( %(filename)s : %(lineno)d) -- %(message)s')
        f.setFormatter(formatter)
        self.logger.addHandler(f)

        self.logger.info('HTTPEngine started')
        self.env = Environment(loader=FileSystemLoader('templates'))

    def start_http_server(self):
        cherrypy.config.update({'server.socket_host': self.config['HTTP_HOST'],'server.socket_port': self.config['HTTP_PORT'],'environment': 'production'})
        cherrypy.tree.mount(self,'/',config=STATIC_CONFIG)
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
            return tmpl.render(streams=self.Client.get_list_of_streams(),errors=[])
        
        stream = self.Client.get(id)
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
                    self.Client.open_stream(stream_file.file)
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
                self.Client.create_stream(name,source,content_id,int(port))
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
        container = self.Client.get(id)
        if container is None:
            raise cherrypy.HTTPError(404,'No matching stream')
        self.Client.delete_stream(container)
        return tmpl.render(errors=[])

    @cherrypy.expose
    def file(self,id=None,fmt='tamchy'):        
        stream = self.Client.get(id)
        if stream is None:
            raise cherrypy.HTTPError(404,'No matching stream')

        if fmt == 'tamchy':
            return serve_fileobj(stream.get_file(), "application/x-download", disposition='attachment',name=stream.name + '.tamchy')

        if fmt == 'playlist':
            playlist = '''#EXTM3U\n#EXTINF:-1, {0}\n{1}'''.format(stream.name,self.config['HTTP_HOST'] + ':' + str(self.config['HTTP_PORT']) + '/stream/' + id)
            return serve_fileobj(StringIO(playlist), "application/x-download", disposition='attachment',name=stream.name + '.m3u')

    @cherrypy.expose
    def stream(self,id):
        cherrypy.response.headers['Content-Type'] = 'application/octet-stream'
        cherrypy.response.stream = True

        if id not in self.Client:
            raise cherrypy.HTTPError(404,'No matching stream')

        stream = self.Client.get_stream(id,BUFFERING_SECONDS)
        while True:
            # this is http video stream
            return stream
# -*-*-*-*-*-*-*-*-*-*-*-*-* CherryPy HTTP methods -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-


def generate_c_id(length=32):
    return ''.join([choice(al + digits) for i in xrange(length)])

# ---------------------------------------- Testing ----------------------------------------------------
class SC:
    def __init__(self,Client,Ps,payload,port,source='',bitrate=0,max_connections=50,is_server=True,ext_ip=''):
        self.content_id = payload['content_id']
        self.Client = Client
        #self.Server = server
        if payload['name'] != 'success':
            raise Exception('Error')

    def close(self):
        pass

class ClienT:
    def __init__(self):
        self.streams = {}

class PeeR:
    def __init__(self,id,c_id='sssss'):
        self.content_id = c_id
        self.closed = False
        self.timeout = False
        self.id = id

    def handle_close(self):
        self.closed = True

def test():
    # this is a little hack to replace imported StreamContainer with another class to make tests
    global StreamContainer
    StreamContainer = SC
    
    c = Client(debug=True)
    assert not c.ports
    assert not c._streams
    c.create_stream('success','source')
    assert c._streams
    assert len(c.ports) == 1
    c.create_stream('success','source1')
    assert len(c._streams) == 2
    assert len(c.ports) == 1
    c.create_stream('success','source2')
    assert len(c._streams) == 3
    assert len(c.ports) == 1
    c.create_stream('success','source3',port=5463)
    assert len(c._streams) == 4
    assert len(c.ports) == 2
    try:
        c.create_stream('fail','source3',port=5463)
    except:
        pass
    assert len(c._streams) == 4
    assert len(c.ports) == 2
    try:
        c.create_stream('fail','source3',port=5465)
    except:
        pass
    assert len(c._streams) == 4
    assert len(c.ports) == 3
    err = False
    try:
        c.create_stream('success','source3',port=5465435345)
    except:
        err = True
    assert len(c._streams) == 4
    assert len(c.ports) == 3
    assert err
    print(c.ports[7889].streams)
    s = c.ports[7889].streams.keys()
    s1 = c.ports[7889].streams[s[0]]

def test_PeerStorage():
    ps = PeerStorage(max_con=15,max_con_by_stream=5)
    peers = []
    for i in xrange(8):
        peer = PeeR(i)
        peers.append(peer)
        ps.add(peer)
    assert ps.can_add_peer()
    assert ps.can_add_peer(content_id='content_id')
    for peer in peers:
        peer.content_id = 'content_id'
        if peer.id == 3:
            break
    assert ps.can_add_peer()
    assert ps.can_add_peer(content_id='content_id')
    assert not list(ps.get_peers('buuhin'))
    peers[4].content_id = 'content_id'
    assert ps.can_add_peer()
    assert not ps.can_add_peer(content_id='content_id')
    assert len(ps.peers) == 8
    v = list(ps.get_peers('content_id'))
    assert len(v) == 5
    peers[2].closed = True
    peers[4].closed = True
    v = list(ps.get_peers('content_id'))
    assert len(v) == 3
    assert len(ps.peers) == 6
    peers[0].timeout = True
    assert not peers[0].closed
    v = list(ps.get_peers('content_id'))
    assert len(v) == 2
    assert len(ps.peers) == 5
    assert v[0].id == 1
    assert v[1].id == 3
    assert peers[0].closed

# -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* Testing -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-

if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'debug':
        t = sys.argv[2]
        c=Client(debug=True)
        if t == 's':
            c._create_stream('test','http://127.0.0.1:8080',content_id='w5vi59e7iysc3uu60pn7gasxkwf3hecc')
        else:
            file = open('umut.tamchy','rb')
            c._open_stream(file,port=6590)
        while True:
            pass    
        #t = sys.argv[1]
        #c=Client(debug=True)
        #if t == 's':
        #   c._create_stream('test','http://127.0.0.1:8080',content_id='w5vi59e7iysc3uu60pn7gasxkwf3hecc')
        #else:
        #   file = open('umut.tamchy','rb')
        #   c._open_stream(file,port=6590)
        #while True:
        #   pass    