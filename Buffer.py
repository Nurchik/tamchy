# -*- coding:utf-8 -*-
from time import time
from struct import pack,unpack
import io,logging,sqlite3,os
from os.path import exists,isfile,isdir
from collections import deque
from threading import Lock

CACHING_TIME = 10
# 10 minutes
BUFFERING_TIME = 5 * 60
MIN_SIZE = 512
# because struct's max int for "!I" = 65535
COUNTER = 2 ** 16 - 1


#class storage:
#	def __init__(self):
#		self.temp = []
#		self.store = []
#
#	def put(self,el):
#		store = self.store
#		if el not in store:
#			if not store:
#				store.append(el)
#				store.sort()
#				return 
#			# последовательное значение
#			if ((pos - 1) == store[-1]) or ((pos + 1) == store[0]):
#				store.append(el)
#				store.sort()
#			else:
#				self.temp.append(el)
#
#	def get(self,el):
#		pass
#
#	def __len__(self):
#		pass



'''
This is a simple DB-based temporary-file implementation
Why DB? Because I'm lazy :)
Seriously, temporary-file can be implemented with anything (file,memcached,cloud...) but in a future
But this implementation was very easy to develop and work with, so i chose it for the first time )
'''
class DBStorage:
	def __init__(self,Container,grenade):
		self.C = Container
		self.c_id = Container.content_id
		self.positions = deque()
		self.lock = Lock()
		self.grenade = grenade
		# try to create table with pos,data
		self._init()

	def _init(self):
		# we check is there a file
		# if file exists -> remove, because we can meet problems with sqlite3 when working with this file
		# else -> pass
		if exists(self.c_id):
			try:os.remove(self.c_id)
			except:self.grenade.pull('Cannot Create Temp File',self.C)
		with sqlite3.connect(self.c_id) as db:
			try:
				db.execute('create table "{0}" (pos int primary key,data text) '.format(self.c_id))
			except:
				db.execute('drop table "{0}"'.format(self.c_id))
				db.execute('create table "{0}" (pos int primary key,data text) '.format(self.c_id))
			db.commit()

	# this function will be used for selecting position (which was added earlier than others)
	# to remove from storage
	def get_min(self):
		with self.lock:	
			try:
				pos = self.positions.popleft()
			except:
				pos = None
		return pos

	def __setitem__(self,key,value):
		db = sqlite3.connect(self.c_id)
		# text_factory = str -> because by default sqlite3 doesn't work with binary data and we make it work with one
		db.text_factory = str
		# this position not in DB => we must create it
		# otherwise we must renew data of this position
		if key not in self.positions:
			with self.lock:
				self.positions.append(key)
			db.execute('insert into "{0}" values (?,?)'.format(self.c_id),(key,value))
		else:
			db.execute('update "{0}" set data=? where pos=?'.format(self.c_id), (value,key))
		db.commit()

	def __getitem__(self,key):
		db = sqlite3.connect(self.c_id)
		db.text_factory = str
		if key not in self.positions:
			return ''
		data = db.execute('select data from "{0}" where pos=?'.format(self.c_id),(key,)).fetchone()[0]
		return data

	def __delitem__(self,key):
		db = sqlite3.connect(self.c_id)
		db.text_factory = str
		if key not in self.positions:
			return
		db.execute('delete from "{0}" where pos=?'.format(self.c_id),(key,))
		db.commit()
		with self.lock:
			try:self.positions.remove(key)
			except:pass

	def __len__(self):
		return len(self.positions)

	def __contains__(self,item):
		return item in self.positions

	def close(self):
		pass

class StreamBuffer:
	def __init__(self,Container,grenade):
		self.logger = logging.getLogger('tamchy.Buffer')
		self.pos = 0
		self.buffer = {}
		self.inited = False
		# There can be any storage
		self.storage = DBStorage(Container,grenade)
		# in this dict will be stored pos and % of completion
		# we will need it later when piece will not be done yet for streaming with get_stream()
		self.pieces = {}

	def get_pos(self):
		return pack('!H',self.pos)

	def got_pos(self):
		# setting position is a signal to start buffering stream and to stream it on our machine
		if not self.inited:
			self.inited = True

	def put(self,t,data):
		if len(self.buffer) == CACHING_TIME:
			t1 = min(self.buffer.keys())
			d = self.buffer.pop(t1)
			self._put(t1,d)
		self.buffer[t] = data
		self.pos = t

	def get(self,t):
		if t not in self.buffer.keys():
			return self._get(t)
		return self.buffer[t]

	def _put(self,t,data):
		self.storage[t] = data
		if len(self.storage) == BUFFERING_TIME:
			pos = self.storage.get_min()
			if pos is not None:
				del self.storage[pos]

	def _get(self,t):
		return self.storage[t]

	'''
	buf_sec -> number of seconds to buffer before yelding data of stream
	but until buf_sec not reached, this function will send to consumer information about percentage of buffered data
	and after reaching -> continious data of stream until program's closing

	When new piece is put to buffer -> this function will be informated about it
	'''
	def get_stream_future(self,buf_sec):
		buf_sec = float(buf_sec)
		while not self.inited:
			yield 'Connecting to Peers'
		pos = self.pos
		while True:
			p = int((self.pos - pos) / buf_sec * 100)
			if p >= 100:
				break
			# number is a percent of completion of prebuffering
			yield 'Prebuffering: {0}%'.format(p)
		while True:
			d = self.get(pos)
			if not d:
				# buffering percentage
				# if piece not in self.pieces -> return 0.0
				yield 'Buffering: {0}%'.format(self.pieces.get(pos,0))
				continue 
			# else 0, because we need to reset counter if everything is OK -> just add up 1
			pos = pos + 1 if pos < COUNTER else 0
			yield d

	def get_stream(self,buf_sec):
		buf_sec = float(buf_sec)
		while not self.inited:
			yield None
		pos = self.pos
		while True:
			d = self.get(pos)
			if not d:
				yield None
				continue 
			# else 0, because we need to reset counter if everything is OK -> just add up 1
			pos = pos + 1 if pos < COUNTER else 0
			yield d

	def close(self):
		self.buffer = {}
		self.storage.close()
		self.logger.info('File buffer flushed and closed')


# Testing
def test_storage():
	s = DBStorage('content_id1')
	db = sqlite3.connect('content_id1')
	assert not s.positions
	data = pack('!III',1,2,3)
	s[1] = data
	data1 = pack('!III',2,2,3)
	data2 = pack('!III',3,2,3)
	data3 = pack('!III',4,2,3)
	s[2] = data1
	s[3] = data2
	s[4] = data3
	assert 1 in s.positions
	d = db.execute('select data from "content_id1" where pos=1').fetchone()[0]
	assert d == data
	assert s[1] == data
	data = pack('!III',4,2,1)
	assert len(s) == 4
	s[1] = data
	assert len(s) == 4
	assert s[1] == data
	assert s[5] == ''
	del s[5]
	assert len(s) == 4
	del s[1]
	assert len(s) == 3
	d = db.execute('select data from "content_id1" where pos=1').fetchone()
	assert d == None
	assert s.get_min() == 2


def test():
	s = StreamBuffer('iaudan')
	for i in range(10):
		if i != 2:
			s.put(i,'data'+str(i))
		else:
			s.put(i,'longdata2')
	assert s.pos == 9
	assert len(s.buffer) == 10
	s.put(10,'data10')
	assert len(s.buffer) == 10
	assert 0 not in s.buffer.keys()

def test_get_stream():
	s = StreamBuffer('test_c_id')
	h = s.get_stream(4)
	assert h.send(None) == 'Connecting to Peers'
	assert h.send(None) == 'Connecting to Peers'
	assert h.send(None) == 'Connecting to Peers'
	s.inited = True
	s.pos = 4
	assert h.send(None) == 'Prebuffering: 0%'
	s.pos = 6
	assert h.send(None) == 'Prebuffering: 50%'
	s.pos = 8
	s.buffer = {4:'4data4',5:'5data5',8:'8data8'}
	s.pieces = {7:35}
	assert h.send(None) == '4data4'
	assert h.send(None) == '5data5'
	assert h.send(None) == 'Buffering: 0%'
	assert h.send(None) == 'Buffering: 0%'
	s.buffer[6] = '6data6'
	assert h.send(None) == '6data6'
	assert h.send(None) == 'Buffering: 35%'
	s.buffer[7] = '7data7'
	assert h.send(None) == '7data7'
	assert h.send(None) == '8data8'
	assert h.send(None) == 'Buffering: 0%'
	assert h.send(None) == 'Buffering: 0%'
	s.pieces[9] = 34
	assert h.send(None) == 'Buffering: 34%'
	s.pieces[9] = 68
	assert h.send(None) == 'Buffering: 68%'
	s.buffer[9] = '9data9'
	assert h.send(None) == '9data9'


