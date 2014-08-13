from time import time
import urllib2

class Input:
    def __init__(self,source,chunk_length):
        self.source = source
        self.chunk_length = chunk_length
        self.con = self.connect()
        self.piece_length = self.calculate_piece_length()

    def connect(self):
        pass

    def read(self):
        pass

    def reconnect(self):
        con = self.connect()
        self.con = con

    def calculate_piece_length(self):
        lengthes = []
        for i in range(3):
            d = ''
            t = time()
            while time() - t < 1:
                d += self.con.read(4096)
            lengthes.append(len(d))
        # we will take the biggest number in sizes and will use this size as piece length
        # but (piece length / chunk size) must to be able to be divided by 8 without remain, because if we don't do that 
        # we will add trailing zeroes to bitfield to properly transport piece's one when requesting :(

        # @!! SIMPLY MAGIC !!@
        length = max(lengthes)
        number_of_chunks = length / self.chunk_length
        length = 8 * ((number_of_chunks / 8) + (1 if number_of_chunks % 8 else 0)) * self.chunk_length
        return length

    def close(self):
        if self.con:
            self.con.close()


class HTTPInput(Input):
    def connect(self):
        try:
            con = urllib2.urlopen(self.source)
        except:
            return
        return con

    def read(self):
        source = self.con

        if source is None:
            return ''

        #t = time()
        d = source.read(self.piece_length)
        while d < self.piece_length:
            d += source.read(self.piece_length - len(d))
        return d
        #while (time() - t) < 1:
        #    try : 
        #        d += source.read(1024)
        #    except:
        #        return ''
        #return d

