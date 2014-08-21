# -*- coding:utf-8 -*-

import sqlite3
import threading
import urllib2
import socket
import logging
import select
import pickle
from Buffer import StreamBuffer
from Peer import Peer
from Server import Server,encode,decode
import messages
from time import time,sleep
from struct import pack,unpack
from StringIO import StringIO
import Input
from Request import Request

DEFAULT_BUFFERING_SIZE = 128 * 1024
WRITE_WAIT_TIMEOUT = 15.0
READ_WAIT_TIMEOUT = 15.0
SOCKET_TIMEOUT = 20.0
STREAM_PIECE_SIZE = 16384
COUNTER = (2 ** 16) - 1
   

'''
:source is a http source for video to stream
'''

class StreamContainer:
    def __init__(self,Client,PStorage,info,port,max_connections=50,is_server=False,source='',bitrate=0,ext_ip='',debug=False):
        self.logger = logging.getLogger('tamchy.StreamContainer')
        
        #threading.Thread.__init__(self)
        #self.daemon = True
        self.work = True
        self.paused = False

        self.info = info
        self.ext_ip = ext_ip
        self.port = port
        self.max_connections = max_connections
        self.is_server = is_server
        self.bitrate = bitrate
        self.Client = Client
        self.PStorage = PStorage
        self.debug = debug

        self.B = StreamBuffer(info['content_id'],self.close,self.tell_have)
        self.lock = threading.Lock()

        self.name = info['name']
        self.content_id = info['content_id']
        self.chunk_length = info['chunk_length']
        
        self.peers = []
        self.requests = []    

        self.logger.debug('StreamContainer (%s) created' % (self.content_id))
        # after receiving peers list, connection with server will be used in requesting stream
        # but program will not send any statistics to the server any more

        self.add_new_stream()
        if not debug:
            if not is_server:
                self.source = ''
                server = self.connect_server(info['ip'],info['port'])
                if server is not None:
                    self.pos = server.pos
                    self.piece_length = server.piece_length
                    self.PStorage.add(server)
                else:
                    self.close()
                    raise Exception('Cannot Connect to Server')
            else:
                # this is source's initialisation -> we should check if it works
                self.source = self.select_for(source,self.chunk_length)
                self.pos = 0
                self.piece_length = self.source.piece_length
            
    #def get_piece_length(self):
    #    return struct.pack('!I',self.piece_length)

    def build_handshake(self):
        if self.is_server:
            handshake = 'Salamatsyzby' + self.content_id + struct.pack('!H',self.port) + pack('!HI',self.pos,self.piece_length)
        else:
            handshake = 'Salamatsyzby' + self.content_id + struct.pack('!H',self.port)
        handshake = struct.pack('!I',len(handshake)) + handshake
        return handshake

    def connect_server(self,ip,port,node=False):
        handshake = 'Salamatsyzby' + self.content_id + self.peer_id + struct.pack('!H',self.port)
        handshake = pack('!I',len(handshake)) + handshake
        stream_data = {'content_id':self.content_id,'chunk_length':0,'piece_length':0, 'handshake':handshake}
        p = Peer(stream_data,self,self.Buffer,ip=ip,port=port,server=True,node=node)
        if p.socket is not None:
            r,w,e = select.select([p],[p],[p],SOCKET_TIMEOUT)
            if not w:
                self.logger.error('Server (%s:%s) is not available' % (ip,port))
                return
            p.request_peers()
            p.handle_write()
            r,w,e = select.select([p],[p],[p],SOCKET_TIMEOUT)
            if not r:
                self.logger.error('Server (%s:%s) does not respond' % (ip,port))
                return
            p.handle_read()
            if not p.handshaked:
                self.logger.error('Could not connect to Server (%s:%s)' % (ip,port))
                return
            self.logger.info('Connection with Server (%s:%s) established' % (ip,port))
            return p
        else:
            self.logger.error('Server (%s:%s) is not available' % (ip,port))
            return

        #s = socket.socket()
        #s.settimeout(SOCKET_TIMEOUT)
        #try:
        #    s.connect((ip,port))
        #except (socket.error,socket.timeout) as e:
        #    self.logger.error('%s - %s' % (e.errno,e.message))
        #    return 
        #handshake = 'Salamatsyzby' + self.content_id + self.peer_id + struct.pack('!H',self.port) + pack('!HI',0,0)
        #handshake = pack('!I',len(handshake)) + handshake
        #msg = GET_PEERS + pack('!B',35)
        #msg = pack('!I',len(msg)) + msg
        #s.send(handshake)
        #s.send(msg)
        #try:
        #    data = s.read(4096)
        #except (socket.error,socket.timeout) as e:
        #    self.logger.error('%s - %s' % (e.errno,e.message))
        #    return 
        #while data:
        #   length = unpack('!I',data[:4])[0]
        #   if length > 

    def select_for(self,source,chunk_length):
        s = source.split(':')[0]
        if s == 'http':
            i = Input.HTTPInput(source,chunk_length)
            if i.con == None:
                self.close()
                raise Exception('Cannot connect to source')
            # we must close connection to source until we need it later to prevent source's buffer overflow
            i.con.close()
            return i
        else:
            self.close()
            raise Exception('Cannot Recognize Input source')

    def prepare_peers(self,peers):
        for i in xrange(len(peers)/6):
            raw = peers[0+i*6:6+i*6]
            ip,port = decode(ip=raw[0:4],port=raw[4:6])
            if ip == self.ext_ip and port == self.port:
                continue
            if self.PStorage.can_add_peer(content_id=self.content_id):
                self.prepare_peer(ip=ip,port=port)  

    def prepare_peer(self,ip,port,sock=None,buf=''):
        stream_data = {'content_id':self.content_id,'chunk_length':self.chunk_length,'piece_length':self.piece_length, 'handshake':self.build_handshake()}
        if sock is None:
            #ip = '.'.join([str(x) for x in unpack('!BBBB',ip)])
            #port = unpack('!H',port)[0]
            p = Peer(stream_data,self,self.B,ip=ip,port=port,buf=buf)
        else:
            p = Peer(stream_data,self,self.B,ip=ip,port=port,sock=sock,buf=buf)
        self.add_new_peer(ip,port)
        self.PStorage.add(p)
        #if self.paused: 
        #    t = threading.Thread(target=self.run)
        #    t.daemon = True
        #    t.start()
        #    self.paused = False

    def get_file(self):
        return StringIO(pickle.dumps(self.info))

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
            node = self.connect_server(ip,port,node=True)
            if node:
                self.PStorage.add(node)

    def run(self):
        if self.is_server:
            source = self.source
            # This is done to be able to watch http-stream in server machine
            # maybe it's not necessary but will be good )
            self.B.inited = True
            recon_tried = 0
            source.connect()
            #i = 0
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
                        self.close()
                        self.logger.error('Cannot fetch data from source')
                        break

                # why len(d) + 4 ? Because we must to take into account 4 bytes of length
                # and if we didn't do that, everytime a piece transferred, trailing 4 bytes of data 
                # will not be recieved by peer
                #d = pack('!I',len(d) + 4) + d               
                self.B.put(self.pos,d)
                # i.e. 65535
                if self.pos == COUNTER:
                    self.pos = 0
                else:
                    self.pos += 1
            # exiting
            source.close()

        else:            
            # starting connection to nodes
            if self.info.get('nodes',[]):
                t = threading.Thread(target=self.connect_nodes)
                t.daemon = True
                t.start()             
            
            self.logger.info('Started main loop')
            pos = self.pos

            while self.work:
                # selecting only handshaked peers
                peers = self.PStorage.get_peers(self.content_id)
                
                # if there are no peers - pause working
                # the work will be resumed by self.prepare_peer
                #if not peers:
                #    self.paused = True
                #    return 
                
                for peer in sorted(peers,key=lambda x : x.upload_speed,reverse=True): 
                    if peer.need_keep_alive:
                        peer.send_keep_alive()
                    # peer doesn't have pending requests
                    elif peer.can_request:
                        c = True
                        # selecting only idle requests
                        for request in self.requests:
                            # if peer have first piece we will request that piece
                            # in hope that other pieces will arrive to the peer while sending
                            # first one
                            if peer.have(request.pos):
                                self.logger.debug('Retry Request %s' % (request))
                                self.requests.remove(request)
                                peer.request_stream(request)
                                c = False
                                break
                        # because we have to request only one "request" 
                        if not c:
                            continue
                        # if there are no appropriate request in self.requests
                        # or there are no requests in self.requests => try to request stream by position
                        elif peer.have(pos):  
                            s = self.get_seconds(peer)
                            request = Request(self.B,self.piece_length,self.chunk_length,pos,s)
                            peer.request_stream(request)
                        #   peer.request_stream(pos,STREAM_PIECE_SIZE,s)
                            pos = pos + s
                            # because we reset our counter after COUNTER => 
                            # if we have pos = 65536 and COUNTER = 65536 => new pos will be 
                            # equal to pos = 0, because pos = 65536 will not be appropriate for struct
                            # format '!I' (max is 65535, you can check it!)
                            if pos >= COUNTER:
                                pos = pos - COUNTER
                    else:
                        # peer has job in queue, but there is a timeout
                        if peer.request_timeout:
                            self.logger.debug('Request timeout of peer (%s) has reached. Retrying request' % (peer))
                            self.requests.extend(peer.return_requests())

                if self.debug:
                    break

            else:
                self.close()
                self.logger.error('Cannot connect to server')

        self.logger.debug('Main loop stopped')

    def return_reqs(self,reqs):
        self.requests.extend(reqs)

    #def set(self,pos,piece_length):
    #    if self.pos is None:
    #        #self.start_pos = pos
    #        self.B.got_pos()
    #        self.pos = pos
    #        self.piece_length = piece_length
    #        # starting work
    #        t = threading.Thread(target=self.run)
    #        t.daemon = True
    #        t.start()

    def tell_have(self,t):
        for peer in self.PStorage.get_peers(self.content_id):
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
            # if exists
            db.execute('drop table \"'+self.content_id+'\"')
        except:
            pass
        self.logger.debug('Table for stream (' + self.content_id + ') removed from DB')
        db.close()

    def add_new_peer(self,ip,port):
        db = sqlite3.connect('DataBase.db')
        db.text_factory = str
        ip,port = self.encode(ip=ip,port=port)
        try:
            db.execute('insert into \"'+self.content_id+'\" values(null,?,?)',(ip,port))
            db.commit()
            self.logger.info('Peer added to DB')
            self.logger.debug('Peer (%s:%s) added to DB' % (ip,port))
        # maybe ip is exist
        except sqlite3.IntegrityError:
            self.logger.info('Peer already exists')
        db.close()

    def delete_peer(self,ip):
        db = sqlite3.connect('DataBase.db')
        db.text_factory = str
        ip = self.encode(ip=ip)
        try:
            db.execute('delete from \"'+self.content_id+'\" where ip=?',(ip,))
            db.commit()
            self.logger.info('Peer removed from DB')
            self.logger.debug('Peer (%s) removed from DB' % (ip))
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
        self.Client.close_container(self)
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
        self.ip = 'ip'
        self.port = 345

    @property
    def timeout(self):
        return self.t

    @property
    def request_timeout(self):
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

    def request_stream(self,request):
        self.req.append(request)
        self.can = False

    def return_requests(self):
        r = self.req[:]
        self.req = []
        return r

class PStorage:
    def __init__(self):
        self.peers = []

    def get_peers(self,content_id):
        return self.peers

def g(i,p):
    return True

def test_run():
    ps = PStorage()
    s = StreamContainer('client',ps,{'content_id':'content_id','ip':'123','port':'233','name':'wewf','chunk_length':12},6590,debug=True)
    #s.connect_server = g
    #s.start_pos = 65534
    s.pos = 65534
    s.piece_length = 17
    
    p1 = PeeR(1)
    p1.handshaked = True
    p1.upload_speed = 2
    p1.t = True
    
    p2 = PeeR(2)
    p2.handshaked = True
    p2.upload_speed = 6
    p2.can = False
    p2.req = [Request('buffer',16,2,2,12)]
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
    p8.p = [2]
    
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
    
    ps.peers = [p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12]
    s.run()
    assert p12.req[0].info == '(65534, -1, 3)'
    assert not p8.closed
    assert p8.req[0].info == '(2, -1, 3)'
    assert p6.ka
    assert not p2.req
    assert p4.req[0].info == '(2, -1, 12)'
    assert not p7.ka
    assert not p9.closed
    assert p5.ka
