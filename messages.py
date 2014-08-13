# -*- coding:utf-8 -*-
import struct
from string import ascii_lowercase as al
from string import digits
from random import choice

KEEP_ALIVE = struct.pack('!B',0)
POSITION = struct.pack('!B',1)
GET_PEERS = struct.pack('!B',2)
GET_STREAM = struct.pack('!B',3)
STOP = struct.pack('!B',4)
CLOSE = struct.pack('!B',5)
ERROR = struct.pack('!B',6)
HAVE = struct.pack('!B',7)
BITFIELD = struct.pack('!B',8)

def construct_handshake(content_id,peer_id,port):
    ''' Structure of handshake
    length|Salamatsyzby|content_id|peer_id|port
    peer_id - 20 bytes, content_id - 32 bytes
    '''
    msg = 'Salamatsyzby' + content_id + peer_id + struct.pack('!H',port)
    return struct.pack('!I',68) + msg

def generate_content_id(length=32):
    return ''.join([choice(al + digits) for i in xrange(length)])


def generate_peer_id(length=20):
    return generate_content_id(length)