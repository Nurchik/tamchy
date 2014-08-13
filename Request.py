import struct
import bitstring
import Peer

class Request:
    def __init__(self,Buffer,piece_length,chunk_length,pos,seconds):
        self.Buffer = Buffer
        #self.piece_length = piece_length
        self.pos = pos
        self.seconds = seconds
        self.ps = pos + seconds
        self.chunk_length = chunk_length
        # in all requests offset = -1
        offset = -1
        self.info = str((pos,offset,seconds))
        self.request = Peer.GET_STREAM + struct.pack('!HhB',pos,offset,seconds)
        # self.buffer's structure
        #     /------/--POS--\-------\-------\
        #     |      |       |       |       |
        # offset1 offset2 offset3 offset4 offsetN
        #    |       |       |       |       |
        #   data    data    data    data    data
        #
        self.buffer = {}
        # in this dict pos <-> bitfield will be saved
        self.pieces = {}
        self.num = piece_length / chunk_length
        self.to_do = self._prepare(pos,seconds)

    def __contains__(self,item):
        return (item >= self.pos and item < self.ps)

    def __str__(self):
        return self.info

    def _prepare(self,pos,seconds):
        d = []
        for i in xrange(pos,pos + seconds):
            # at initialisation there will be '' , but when the pieces will arrive it will be replaced with bitstring.BitStream()
            d.append(i)
        return d

    def put(self,pos,offset,data):
        if not self.check(pos,offset,data):
            return
        if pos not in self.pieces:
            self.pieces[pos] = self.construct_bitfield()
            self.buffer[pos] = {}

        self.pieces[pos][offset] = True
        self.buffer[pos][offset] = data

        if self.is_completed(pos):
            D = ''.join(piece for piece in self.buffer[pos].itervalues())
            # clearing memory ... There is no need to have 2 copies of data, one in self.buffer, one in self.Buffer
            del self.buffer[pos]
            self.Buffer.put(pos,D)
            self.to_do.remove(pos)

    def check(self,pos,offset,data):
        # we already downloaded a data
        # get(pos,{}) -> because if we does not already have pos in self.buffer
        # we will get error while checking offset
        if offset in self.buffer.get(pos,{}) or (offset > self.num - 1):
            return False
        if len(data) != self.chunk_length:
            ## last chunk => it can have any size
            #if offset != self.num - 1:
            return False
        return True


    @property
    def completed(self):
        # if self.to_do is not empty it will mean that we have not completed => not True == False :)
        return not self.to_do

    def is_completed(self,pos):
        for i in self.pieces[pos]:
            # if data for given offset is not recieved from peers
            if not i:
                return False
        return True

    def construct_bitfield(self):
        bitfield = bitstring.BitStream(self.num)
        return bitfield 

    def get_request(self):
        # request's structure 
        # ID + pos + offset + length + seconds + (pos + bitfield_length + bitfield)
        # if piece not finished but started filling, we will send bitfield
        bitfields = ''
        for pos,bitfield in self.pieces.iteritems():
            if self.is_completed(pos):
                # if piece completed, we will tell that bitfield's length = 0
                bitfield = ''
            else:
                #bitfield = bitfield.tobytes()
                bitfield = bitfield.bytes()
            bitfields += struct.pack('!HH',pos,len(bitfield)) + bitfield

        msg = self.request + bitfields
        msg_length = struct.pack('!I',len(msg))

        return msg_length + msg

#Testing
class BuffeR:
    def __init__(self):
        self.b = {}

    def put(self,pos,data):
        self.b[pos] = data

def test():
    b = BuffeR()
    r = Request(b,16,2,3,2)
    assert r.num == 8
    assert r.to_do == [3,4]
    r.put(3,2,'da')
    assert r.buffer[3][2] == 'da'
    assert r.pieces[3][2]
    assert len(r.buffer) == 1
    assert len(r.buffer[3]) == 1
    r.put(3,2,'tt')
    assert r.buffer[3][2] == 'da'
    assert len(r.buffer) == 1
    assert len(r.buffer[3]) == 1
    r.put(3,8,'tt')
    assert len(r.buffer[3]) == 1
    assert r.pieces[3].bin == '00100000'
    r.put(3,7,'ttf')
    assert len(r.buffer[3]) == 1
    assert r.pieces[3].bin == '00100000'
    r.put(3,7,'ts')
    assert len(r.buffer[3]) == 2
    assert len(r.buffer) == 1
    assert r.pieces[3].bin == '00100001'
    assert not r.is_completed(3)
    assert not r.completed
    r.put(3,0,'th')
    r.put(3,1,'is')
    r.put(3,3,'ta')
    r.put(3,4,'fo')
    r.put(3,5,'rt')
    assert r.pieces[3].bin == '11111101'
    assert not r.is_completed(3)
    r.put(3,6,'es')
    assert not r.buffer
    assert b.b[3] == 'thisdatafortests'
    assert r.is_completed(3)
    assert not r.completed
    assert r.pieces[3].bin == '11111111'
    r.put(4,0,'th')
    r.put(4,1,'is')
    assert len(r.buffer[4]) == 2
    assert not r.is_completed(4)
    r.put(4,2,'da')
    r.put(4,3,'ta')
    r.put(4,4,'fo')
    r.put(4,5,'rt')
    r.put(4,6,'es')
    r.put(4,7,'ts')
    assert r.pieces[4].bin == '11111111'
    assert not r.buffer
    assert b.b[4] == 'thisdatafortests'
    assert r.is_completed(4)
    assert not r.to_do
    assert r.completed



















