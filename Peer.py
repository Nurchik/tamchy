# -*- coding:utf-8 -*-

import socket,struct,time,logging,sqlite3
from StringIO import StringIO
from threading import Lock
import pickle
import random
from bitstring import BitStream
import Request

KEEP_ALIVE = struct.pack('!B',0)
POSITION = struct.pack('!B',1)
GET_PEERS = struct.pack('!B',2)
GET_STREAM = struct.pack('!B',3)
STOP = struct.pack('!B',4)
CLOSE = struct.pack('!B',5)
ERROR = struct.pack('!B',6)
HAVE = struct.pack('!B',7)
BITFIELD = struct.pack('!B',8)

MSG = {KEEP_ALIVE:'KEEP_ALIVE',POSITION:'POSITION',GET_PEERS:'GET_PEERS',
       GET_STREAM:'GET_STREAM',STOP:'STOP',CLOSE:'CLOSE',ERROR:'ERROR',HAVE:'HAVE'}

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

class Peer:
    def __init__(self,stream_data,Container,Buffer,ip=None,port=None,sock=None,buf='',server=False,node=False):
        self.logger = logging.getLogger('tamchy.Peer')

        self.store = store()
        self.lock = Lock()
        self.last_message_recv = time.time()
        self.last_message_sent = time.time()

        self.content_id = stream_data['content_id']
        self.handshake = stream_data['handshake']
        self.chunk_length = stream_data['chunk_length']
        self.piece_length = stream_data['piece_length']

        self.ip = ip
        self.port = port
        self.requested = []
        self.send_buffer = ''
        self.read_buffer = ''
        # if we have previously recieved data from peer -> add to the read_buffer
        self.read_buffer += buf
        self.upload_speed = 0
        self.download_speed = 0
        # request -> (pos,offset,length,seconds):generator
        self.streamer = {}
        self.temp = {}
        
        self.B = Buffer
        self.C = Container
        self.closed = False
        self.handshaked = False
        self.server = server
        self.node = node
        
        self.logger.info('Created new peer instance')
        
        self.handle_connect(sock,ip,port)

    def __str__(self):
        return '%s:%s' % (self.ip, self.port)

    def handle_connect(self,sock=None,ip=None,port=None):
        self.socket = None

        if sock is not None:
            self.socket = sock
            self.socket.setblocking(0)
            self.send_handshake()
            self.handshaked = True
        else:
            sock = socket.socket()
            sock.setblocking(0)
            try:
                sock.connect((ip,port))
            except socket.error as e:
                if e.errno == 36:
                    self.socket = sock
            self.send_handshake()
            self.logger.debug('Wrote to send buffer handshake-message')

    def fileno(self):
        try:
            s = self.socket.fileno()
            return s
        except:
            self.handle_close()

    #def to_bytes(self,fmt,value):
    #    val = (value.split('.') if (type(value) != int) else value)
    #    return ''.join([struct.pack(fmt,int(x)) for x in val])

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
        
    def recv(self):
        message = ''
        t = time.time()
        sock = self.socket
        while True:
            try:
                m = sock.recv(8192)
                message += m
            except:
                break
        self.last_message_recv = time.time()
        self.upload_speed = len(message) / (self.last_message_recv - t)
        return message

    def parse_messages(self,msg):
        messages = []
        self.read_buffer += msg
        while self.read_buffer:
            length = self.read_buffer[:4]
            if len(length) < 4:
                # this is not entire message => wait for remaining part
                break
            length = struct.unpack('!I',self.read_buffer[:4])[0]
            if length > 32*1024:
                self.handle_close()
                break
            msg = self.read_buffer[4:4 + length]
            if len(msg) < length:
                # this is not entire message => wait for remaining part
                break
            self.read_buffer = self.read_buffer[4 + length:]
            messages.append(msg)
        return messages

    def send_handshake(self):
        #msg = 'Salamatsyzby' + self.content_id + struct.pack('!H',self.port) + struct.pack('!HI',self.pos, self.piece_length)
        #msg = struct.pack('!I',52) + msg
        self.send_buffer += self.handshake

    def process_handshake(self,msg):
        if (msg[:12]).lower() == 'salamatsyzby':
            if msg[12:44] == self.content_id:
                if self.server and not self.node:
                    self.pos, self.piece_length = struct.unpack('!HI',msg[46:52])
                    #self.C.set(pos,piece_length)
                self.handshaked = True
                self.logger.debug('Connection with peer %s established' % (self))
                return
        elif msg[0] == ERROR:
            try:
                error = pickle.loads(msg[1:])
            except:
                error = ''
            #if error == 'Invalid Content ID':
            #    ## Delete this peer from DB
            #    #self.C.delete_peer(self.ip)
            self.logger.debug('Connection with peer not established --> ' + error)
            return self.handle_close()
        self.logger.debug('Connection with peer not established --> Incorrect handshake-message')
        return self.handle_close()

    def process_message(self,msg):
        id = msg[0]
        payload = msg[1:]
        self.logger.debug('Got message %s from peer %s' % (MSG.get(id,'UNKNOWN'),self))

        if id == KEEP_ALIVE:
            pass
        elif id == GET_PEERS:
            if len(payload) == 1:
                self.send_message(GET_PEERS,self.C.get_peers(struct.unpack('!B',payload)[0]))
            else:
                self.C.prepare_peers(payload)
        elif id == GET_STREAM:
            pos,offset = struct.unpack('!Hh',payload[:4])
            payload = payload[4:]
            #This is a request from the peer
            if offset == -1:
                seconds = struct.unpack('!B',payload[:1])
                payload = payload[1:]
                # there are bitfields
                if payload:
                    bitfields = self.decode_bitfields(payload)
                else:
                    bitfields = {}
                self.logger.debug('Get Stream request ' + str((pos,offset,seconds)))
                self.add_stream(pos,offset,seconds,bitfields)
            else:
                self.put(pos,offset,payload)
        elif id == STOP:
            pos,offset,seconds = struct.unpack('!HhB',payload)
            try:
                del self.streamer[(pos,offset,seconds)]
            except:
                pass
            self.logger.debug('Stopped streaming ' + str((pos,offset,seconds)))
        elif id == HAVE:
            self.store.put(struct.unpack('!I',payload)[0])
        elif id == CLOSE:
            #self.DBEngine.delete_peer(self.content_id,self.ip)
            self.handle_close()
            return False
        else:
            self.handle_close()
            return False
        return True

    def decode_bitfileds(self,bitfields,chunk_length):
        bfs = {}
        #num_offsets = self.piece_length / chunk_length + (1 if self.piece_length % chunk_length else 0)
        # when sending request, if bitfield does not have length which can be divided by 8 without remain 
        # spare zeroes will be added to the end of bitfield. We must delete them
        #added_bits = (num_offsets / 8 + (1 if num_offsets % 8 else 0)) * 8 - num_offsets
        while bitfields:
            pos,bitfield_length = struct.unpack('!HH',bitfields[ : 4])

            if bitfield_length == 0:
                bitfield = ''
            else:
                bitfield = bitstring.BitStream(bytes=bitfields[4 : 4 + bitfield_length])
            
            bitfields = bitfields[4 + bitfield_length : ]
            bfs[pos] = bitfield
        return bfs

    def handle_read(self):
        message = self.recv()
        if not message:
            return self.handle_close()
        messages = self.parse_messages(message)
        
        for msg in messages:
            if not self.handshaked:
                self.process_handshake(msg)
                # handshake error
                if not self.handshaked:
                    break
                else:
                    continue
            p = self.process_message(msg)
            if not p:
                break
    
    def add_stream(self,pos,offset,seconds,bitfields):
        if pos in self.B:
            self.streamer[(pos,offset,seconds)] = self.stream(pos,seconds,bitfields)
            self.logger.debug('Added stream generator to store for request ' + str((pos,seconds)))

    def stream(self,pos,seconds,bitfields):
        self.logger.debug('Started generator for ' + str((pos,seconds)))
        p = pos
        for i in xrange(seconds):
            pos = p + i
            if pos >= 65536:
                pos = 65536 - pos

            bitfield = bitfields.get(pos,None)
            # piece for pos is already completed
            if bitfield == '':
                continue
            
            while True:
                data = self.B.get(pos)
                if data:
                    break
                # there is no piece in Buffer at this moment
                yield ''

            chunk_length = self.chunk_length

            for k in xrange(self.piece_length / self.chunk_length):
                if bitfield is not None:
                    # this chunk of piece we already have downloaded so skip it
                    if bitfield[k] == True:
                        continue
                offset = k * chunk_length
                # we will use k as offset because we don't need real offset 
                d = struct.pack('!Hh',pos,k) + data[0+offset : chunk_length+offset]
                self.logger.debug('Sent data for ' + str((pos,k)))
                yield d
        
    def put(self,pos,offset,data):
        self.logger.debug('Put data to %s, length -> %s' % (str((pos,offset)),len(data)))
        with self.lock:
            for req in self.requested:
                if pos in req:
                    req.put(pos,offset,data)
                    if req.completed:
                        self.logger.debug('Request %s completed' % (req))
                        self.requested.remove(req)
                    break

    def return_requests(self):
        r = [i for i in self.requested]
        self.requested = []
        return r
        #r = []
        #with self.lock:
        #    for req in self.requested:
        #        r.append(req.return_requests())
        #        self.requested.remove(req)
        #return r

    @property
    def timeout(self):
        if time.time() - self.last_message_recv >= ALIVE_TIMEOUT:
            return True
        return False

    @property
    def request_timeout(self):
        if self.requested:
            if time.time() - min(self.requested, key=lambda x : x.t).t > REQUEST_TIMEOUT:
                return True
        return False

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

    def request_stream(self,request):
        #request = Request(self.C,self.B,pos,offset,length,seconds)
        self.send_buffer += request.get_request()
        self.requested.append(request)
        self.logger.debug('%s requested' % (request))

    #def request_position(self):
    #   self.send_message(POSITION)
    #   self.logger.debug('POSITION requested')

    def request_peers(self,num=35):
        self.send_message(GET_PEERS,struct.pack('!B',num))
        self.logger.debug('List of peers requested')

    def send_have(self,t):
        self.send_message(HAVE,struct.pack('!I',t))
        self.logger.debug('Sended HAVE -> %s to %s' % (t,self))

    def send_stop(p,o,s):
        self.send_message(STOP,struct.pack('!HIH',p,o,s))
        self.logger.debug('Sended STOP for ' + str((p,o,s)))

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
        self.streamer = {}
        self.read_buffer = ''
        self.send_buffer = ''
        r = self.return_requests()
        if r:
            self.C.return_reqs(r)
        self.C.delete_peer(self.ip)
        self.socket.close()
        self.closed = True
        self.logger.info('Closing connection with peer')
        
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
        self.pieces = []

    def put(self,pos,data):
        self.buffer[pos] = data

    def get(self,pos):
        return self.buffer.get(pos,'')

    def get_pos(self):
        return struct.pack('!H',self.pos)

    def __contains__(self,pos):
        return pos in self.pieces

def test_read():
    s = sock()
    c = ContaineR()
    b = BuffeR()
    s_data = {'content_id':'content_id1234567890123456789012','handshake':'handshake','piece_length':0,'chunk_length':0}
    
    p = Peer(s_data,c,b,sock=s,ip='127.0.0.1',port=7668)
    p.send_buffer = ''
    p.handshaked = False
    s.r_buf = [struct.pack('!I',46)+'Salamatsyzby' + 'content_id1234567890123456789012' + struct.pack('!H',78), 'except']
    p.handle_read()
    #assert c.start_pos == 0
    assert not s.closed
    assert not p.streamer
    
    p = Peer(s_data,c,b,sock=s,ip='127.0.0.1',port=7668,server=True)
    p.send_buffer = ''
    p.handshaked = False
    s.r_buf = [struct.pack('!I',52)+'Salamatsyzby' + 'content_id1234567890123456789012' + struct.pack('!H',78)\
        + struct.pack('!HI',32,16384), 'except']
    assert p.piece_length == 0
    assert not hasattr(p,'pos')
    p.handle_read()
    #assert c.start_pos == 0
    assert not s.closed
    assert not p.streamer
    assert p.pos == 32
    assert p.piece_length == 16384

    p = Peer(s_data,c,b,sock=s,ip='127.0.0.1',port=7668,server=True,node=True)
    p.send_buffer = ''
    p.handshaked = False
    s.r_buf = [struct.pack('!I',52)+'Salamatsyzby' + 'content_id1234567890123456789012' + struct.pack('!H',78)\
        + struct.pack('!HI',32,16384), 'except']
    assert p.piece_length == 0
    assert not hasattr(p,'pos')
    p.handle_read()
    #assert c.start_pos == 0
    assert not s.closed
    assert not p.streamer
    assert not hasattr(p,'pos')
    assert p.piece_length == 0

    p = Peer(s_data,c,b,sock=s,ip='127.0.0.1',port=7668)
    p.send_buffer = ''
    p.handshaked = False
    s.r_buf = [struct.pack('!I',52)+'Salamatsyzby' + 'content_id1234567890123456789012' + struct.pack('!H',78)\
        + struct.pack('!HI',32,16384), 'except']
    assert p.piece_length == 0
    assert not hasattr(p,'pos')
    p.handle_read()
    #assert c.start_pos == 0
    assert not s.closed
    assert p.handshaked
    assert not p.streamer
    assert not hasattr(p,'pos')
    assert p.piece_length == 0

    p = Peer(s_data,c,b,sock=s,ip='127.0.0.1',port=7668,server=True)
    p.send_buffer = ''
    p.handshaked = False
    s.r_buf = [struct.pack('!I',52)+'Salamatsyzby' + 'content_id1234567890123456789012' + struct.pack('!H',78)\
                + struct.pack('!HI',32,16384), 'except']
    p.handle_read()
    assert not s.closed
    assert not p.streamer
    p.send_buffer = ''
    p.requested = [Request.Request(b,24,6,15,2)]
    s.r_buf = [struct.pack('!I',2) + GET_PEERS + struct.pack('!B',12), struct.pack('!I',12) + GET_PEERS + 'peers_list1','except']
    p.handle_read()
    assert p.send_buffer[5:] == 'peers_list'
    assert c.prepared == 'peers_list1'
    assert not p.closed
    
    p = Peer(s_data,c,b,sock=s,ip='127.0.0.1',port=7668,server=True)
    p.send_buffer = ''
    p.handshaked = True
    s.r_buf = [struct.pack('!I',13) + GET_STREAM + struct.pack('!HIIH',1,3,45,3),'except']
    assert len(p.streamer) == 0
    
    p = Peer(s_data,c,b,sock=s,ip='127.0.0.1',port=7668,server=True)
    p.send_buffer = ''
    p.handshaked = True
    p.requested = [Request.Request(b,24,8,2,2)]
    b.pieces.append(1)
    s.r_buf = [struct.pack('!I',6) + GET_STREAM + struct.pack('!HhB',1,-1,3),struct.pack('!I',13) + GET_STREAM + struct.pack('!Hh',2,0)+ \
    'payload0', struct.pack('!I',13) + GET_STREAM + struct.pack('!Hh',3,0) + 'payload1', struct.pack('!I',13) + GET_STREAM +\
    struct.pack('!Hh',4,0) + 'payload2','except']
    p.handle_read()
    assert len(p.streamer) == 1
    assert len(p.requested[0].buffer) == 2
    assert not p.store.have(23)
    assert not p.closed
    s.r_buf = [struct.pack('!I',6) + STOP + struct.pack('!HhB',1,-1,3), struct.pack('!I',5) + HAVE + struct.pack('!I',23),struct.pack('!I',1) + CLOSE, 'except']
    p.handle_read()
    assert not p.streamer
    assert p.store.have(23)
    assert c.deleted

def test_write():
    s_data = {'content_id':'content_id1234567890123456789012','handshake':'handshake','piece_length':0,'chunk_length':0}
    def g(n):
        for i in range(n):
            yield str(i)
    c = ContaineR()
    b = BuffeR()
    s = sock()
    p = Peer(s_data,c,b,sock=s,ip='127.0.0.1',port=struct.pack('!H',7668))
    p.handle_write()
    assert s.s_buf
    p.send_buffer = 'datadata'
    p.handle_write()
    assert not p.send_buffer
    assert s.s_buf == 'handshakedatadata'
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


##def test_close():
##   pass
##
##def test_add_stream():
##   pass
##
def test_stream():
    s_data = {'content_id':'content_id1234567890123456789012','handshake':'handshake','piece_length':16,'chunk_length':2}    
    c = ContaineR()
    b = BuffeR()
    s = sock()
    p = Peer(s_data,c,b,sock=s,ip='127.0.0.1',port=struct.pack('!H',7668))
    b.buffer = {1:'thisissimpletext', 2:'bvcgdfhsgtrfdvcg', 3:'thisissimpletext', 4:'testingpurpose45',5:'loremipsumdolors',6:'bulbulyrdasajurg'}
    f = p.stream(7,1,{})
    try : d = f.next()
    except StopIteration : d = ''
    assert not d
    f = p.stream(3,3,{3:BitStream('0b10100100'),4:''})
    assert f.next() == struct.pack('!Hh',3,1) + 'is'
    assert f.next() == struct.pack('!Hh',3,3) + 'si'
    assert f.next() == struct.pack('!Hh',3,4) + 'mp'
    assert f.next() == struct.pack('!Hh',3,6) + 'te'
    assert f.next() == struct.pack('!Hh',3,7) + 'xt'
    assert f.next() == struct.pack('!Hh',5,0) + 'lo'
    assert f.next() == struct.pack('!Hh',5,1) + 're'
    assert f.next() == struct.pack('!Hh',5,2) + 'mi'
    assert f.next() == struct.pack('!Hh',5,3) + 'ps'
    assert f.next() == struct.pack('!Hh',5,4) + 'um'
    assert f.next() == struct.pack('!Hh',5,5) + 'do'
    assert f.next() == struct.pack('!Hh',5,6) + 'lo'
    assert f.next() == struct.pack('!Hh',5,7) + 'rs'
    try : n = f.next()
    except StopIteration : n = ''
    assert not n

def test_put():
    s_data = {'content_id':'content_id1234567890123456789012','handshake':'handshake','piece_length':16,'chunk_length':2} 
    c = ContaineR()
    b = BuffeR()
    s = sock()
    p = Peer(s_data,c,b,sock=s,ip='127.0.0.1',port=struct.pack('!H',7668))
    p.put(1,0,'data')
    assert not p.requested
    p.requested = [Request.Request(b,8,1,15,2)]
    p.put(15,0,'b')
    p.put(15,1,'u')
    assert p.requested[0].buffer[15][1] == 'u'
    p.put(15,1,'e')
    assert p.requested[0].buffer[15][1] == 'u'
    p.put(15,2,'l')
    p.put(15,3,'b')
    p.put(17,0,'t')
    assert len(p.requested[0].buffer) == 1
    p.put(15,4,'u')
    p.put(15,5,'l')
    p.put(15,6,'y')
    # not finished
    assert not p.requested[0].is_completed(15)
    assert not b.buffer
    assert 15 in p.requested[0].to_do
    p.put(15,7,'r')
    # finished
    assert len(b.buffer) == 1
    assert 15 not in p.requested[0].to_do
    assert 15 in b.buffer
    assert p.requested[0].is_completed(15)
    p.requested[0].pieces[16] = BitStream('0b01011111')
    p.requested[0].buffer[16] = {}
    p.put(16,0,'e')
    assert 16 in p.requested[0].buffer
    p.put(16,2,'t')
    assert 16 in b.buffer
    assert not p.requested

##
#def test_return():
#    c = ContaineR()
#    b = BuffeR()
#    s = sock()
#    p = Peer('content_id','handshake',c,b,sock=s,ip='127.0.0.1',port=struct.pack('!H',7668))
#    r = Request(c,b,1,0,15,3)
#    p.requested = [r]
#    ret = p.return_requests()
#    assert ret == [(1,0,15,3)]
#    class data:
#        def __init__(self,l):
#            self.len = l
#
#    r = Request(c,b,1,0,15,3)
#    r.buffer[1] = {'data':data(10)}
#    p.requested = [r]
#    ret = p.return_requests()
#    assert ret == [(1,10,15,3)]
#    r = Request(c,b,1,0,15,3)
#    r.c[1] = True
#    r.buffer[1] = {'data':data(10)}
#    r.buffer[2] = {'data':data(8)}
#    p.requested = [r]
#    ret = p.return_requests()
#    assert ret == [(2,8,15,2)]
#    p.requested = []
#    ret = p.return_requests()
#    assert ret == []
#    r = Request(c,b,1,0,15,1)
#    r.buffer[1] = {'data':data(10)}
#    p.requested = [r]
#    ret = p.return_requests()
#    assert ret == [(1,10,15,1)]
##
##def test_timeout():
##   pass
##
#def test():
#    c = ContaineR()
#    b = BuffeR()
#    s = sock()
#    p = Peer('content_id','handshake',c,b,sock=s,ip='127.0.0.1',port=struct.pack('!H',7668))
#    p.handshaked = True
#    p.request_peers()
#    assert p.send_buffer == 'handshake' + struct.pack('!H',0) + struct.pack('!I',2) + GET_PEERS + struct.pack('!B',35)
#    p.send_buffer = ''
#    p.request_position()
#    assert p.send_buffer == struct.pack('!I',1) + POSITION
#    p.send_buffer = ''
#    p.send_message(KEEP_ALIVE)
#    assert p.send_buffer == struct.pack('!I',1) + KEEP_ALIVE
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#