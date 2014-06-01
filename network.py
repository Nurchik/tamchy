from threading import Thread
from Reactor import Reactor
import socket
import re

BUFFERING_SECONDS = 30

class HTTPServer:
    def __init__(self,port,Client):
        self.C = Client
        self.socket = self.create_socket(port)
        # Reactor(self) -> because Reactor must listen http-server socket
        self.R = Reactor(self)
        self.R.start()

    def create_socket(self,port):
        s = socket.socket()
        s.bind(('',port))
        s.listen(5)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setblocking(0)
        return s

    def fileno(self):
        try:
            s = self.socket.fileno()
            return s
        except:
            self.handle_close()

    def handle_read(self):
        cl,addr = self.socket.accept()
        cl.setblocking(0)
        self.R.raw_add(Consumer(self.C,self,cl))

    def remove(self,consumer):
        self.R.raw_remove(consumer)

    def close(self):
        self.R.close()
        for consumer in self.R.peers:
            # if consumer is not ourself -> because in Reactor's peers always stored HTTPServer instance
            if consumer != self:
                consumer.handle_close()
        self.socket.close()

    
class Consumer:
    def __init__(self,Client,Server,sock):
        self.C = Client
        self.S = Server
        self.socket = sock
        self.buffer = ''
        self.gen = ''
        self.urls = {
                re.compile(r'^$') : self.index,
                re.compile(r'^streams/(?P<stream_id>\w{32})/$') : self.stream,
                re.compile(r'^open_stream/$') : self.open_stream,
                re.compile(r'^start_stream/$') : self.start_stream,
            }
        # this will indicate must we close connection after handle_write i.e. 
        # Connection: close in request to the client
        self.need_closing = False

    # Http - methods
    def index(self,request=''):
        d = 'HTTP/1.1 200 OK\r\n' +\
         'Content-Type: text/html; charset=utf-8\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n'
        body = '''
        <!DOCTYPE html>
        <html>
            <head>
                <title>Tamchy</title>
            </head>
            <body>
            %s
            </body>
        </html>''' % \
    (''.join(['<p><a href="/streams/%s/">%s</a></p>' % (id,name) for name,id in self.C.get_list_of_streams()]))
        self.buffer += (d + body)
        # because we sent header -> Connection: close
        self.need_closing = True

    def stream(self,stream_id,request=''):
        self.buffer += 'HTTP/1.1 200 OK\r\n' +\
         'Content-Type: application/octet-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n'
        self.gen = self.C.get_stream(stream_id,BUFFERING_SECONDS)

    def start_stream(self,request=''):
        pass

    def open_stream(self,request=''):
        pass

    def return_404(self):
        try:
            self.socket.send('HTTP/1.1 404 Not Found\r\n' +\
         'Connection: close\r\nContent-Type: text/html; charset=utf-8\r\n\r\n' + 'Stream Does Not Exist :(')
        except:
            pass

    # End of Http - methods
    
    def fileno(self):
        try:
            return self.socket.fileno()
        except:
            self.handle_close()

    def handle_read(self):
        d = ''
        while True:
            try:
                part = self.socket.recv(8192)
                if not part:
                    return self.handle_close()
                d += part
            except:
                break
        if not d:
            self.handle_close()
        self.process_request(d)

    def process_request(self,request):
        # may be there is a problem on client side
        if not request:
            return self.handle_close()
        # just replacing \r,\r\n to \n
        request = ''.join((x + '\n') for x in request.splitlines())
        try:
            head,body = request.split('\n\n',1)
        # we have no body
        except:
            head = request
            body = ''
        head = head.splitlines()
        headline = head[0]
        headers = dict(x.split(': ',1) for x in head[1:])
        method, uri, proto = headline.split(' ',3)
        # we are taking all part after first slash('/')
        uri = uri.split('/',1)[1]
        # uri == '' when client requesting index page
        if uri:
            # if uri does not end with / -> add / to the end
            uri = uri if uri[-1] == '/' else uri + '/'
        self.dispatch(uri,body)

    def dispatch(self,url):
        for pattern in self.urls:
            p = pattern.match(url)
            if p:
                # func is function corresponding to pattern
                func = self.urls[pattern]
                # *p.groups is simple unpacking of tuple
                # for more info -> http://stackoverflow.com/questions/36901/what-does-double-star-and-star-do-for-python-parameters
                # just trying to invoke corresponding function with arguments 
                return func(*p.groups(),request=body)
        # if no pattern is matched
        self.return_404()
        self.handle_close()

    def handle_write(self):
        if self.gen:
            try:
                self.buffer += self.gen.next()
            except StopIteration:
                self.need_closing = True
        if self.buffer:
            try: 
                self.socket.send(self.buffer)
            except: 
                return self.handle_close()
            self.buffer = ''
        if self.need_closing:
            self.handle_close()

    def handle_close(self):
        self.socket.close()
        self.S.remove(self)


# Testing
class CliEnt:
    def __init__(self):
        self.list = []

    def get_stream(self,s_id,s):
        return 'starting_stream ...'

    def __contains__(self,item):
        return item in self.list

    def get_list_of_streams(self):
        return ()

class SErver:
    def remove(self,peer):
        pass

class sock:
    def __init__(self):
        self.buffer = ''
        self.msgs = []
        self.closed = False

    def send(self,msg):
        self.buffer += msg

    def recv(self,num):
        msg = self.msgs[0]
        del self.msgs[0]
        if msg == 'except':
            raise Exception
        return msg

    def fileno(self):
        return 1

    def close(self):
        self.closed = True

def test_consumer():
    cl = CliEnt()
    s = sock()
    server = SErver()
    c = Consumer(cl,server,s)
    s.msgs = ['except']
    c.handle_read()
    assert s.closed
    s.closed = False
    s.msgs = ['GET /test/ HTTP/1.1\r\nHost: 127.0.0.1:7668\r\n\
    User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:28.0) Gecko/20100101 Firefox/28.0\r\n\
    Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n\
    Accept-Language: ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3\r\nAccept-Encoding: gzip, deflate\r\n\
    Connection: keep-alive\r\n\r\n','except']
    c.handle_read()
    assert s.closed
    s.closed = False
    s.msgs = ['GET /test/ HTTP/1.1\r\nHost: 127.0.0.1:7668\r\n\
    Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n\
    Cookie: csrftoken=AAxxPlC1slRidHtIp21qvCGqWbSLoA0I\r\n\
    User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9) AppleWebKit/537.71 (KHTML, like Gecko) Version/7.0 Safari/537.71\r\n\
    Accept-Language: ru\r\nAccept-Encoding: gzip, deflate\r\nConnection: keep-alive\r\n\r\n','except']
    c.handle_read()
    assert s.closed
    s.closed = False
    s.msgs = ['GET /streams/ HTTP/1.1\nAccept: text/html\n\n','except']
    c.handle_read()
    assert s.closed
    s.closed = False
    s.msgs = ['GET /streams/ HTTP/1.1\nAccept: text/html\n\n','except']
    c.handle_read()
    assert s.closed
    s.closed = False
    s.msgs = ['GET /stream/342342/ HTTP/1.1\nAccept: text/html\n\n','except']
    cl.list = ['342342']
    c.handle_read()
    assert s.closed
    s.closed = False
    s.msgs = ['GET /streams/1234/ HTTP/1.1\nAccept: text/html\n\n','except']
    cl.list = ['123']
    c.handle_read()
    assert s.closed
    s.closed = False
    s.msgs = ['GET /streams/12345678901234567890123456789012/ HTTP/1.1\nAccept: text/html\n\n','except']
    cl.list = ['12345678901234567890123456789012'] # len() -> 32
    s.buffer = ''
    c.handle_read()
    assert c.buffer
    assert not s.closed
    assert c.gen == 'starting_stream ...'

    # testing handle_write
    c.buffer = 'message1message2'
    def gen():
        for i in xrange(4):
            if i == 2:
                yield ''
                continue
            yield 'data' + str(i)

    c.gen = ''
    s.buffer = ''
    c.handle_write()
    assert s.buffer == 'message1message2'
    assert not c.buffer
    s.buffer = ''
    c.handle_write()
    assert not s.buffer
    c.buffer = 'message1message2'
    c.gen = gen()
    c.handle_write()
    assert s.buffer == 'message1message2data0'
    s.buffer = ''
    c.handle_write()
    assert s.buffer == 'data1'
    c.handle_write()
    assert s.buffer == 'data1'
    c.handle_write()
    assert s.buffer == 'data1data3'
    c.handle_write()
    assert s.buffer == 'data1data3'
    c.handle_write()
    assert s.closed























