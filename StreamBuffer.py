# -*- coding: utf-8 -*-

import Queue
import urllib2
import io
import socket
import pickle
import StringIO

class StreamBuffer:
	def __init__(self,is_server=False,unit_size=256*1024):
	#buffering -> is the amount of buffering units (1 unit = 512 kb)
	#stream_buffer - is the buffer with the structure taken from the Kyrgyz National game named Toguz Korgool.
	#Буфердин тузулушу -> 18 слот (9- ылдыйкы тарабынан , 9-ойдонку тарабынан ). Ар бир слотто 9 видео бирдиги бар.
		self.stream_buffer = {'b':([0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0]),
							  
							  't':([0,0,0,0,0,0,0,0,0],
							  	   [0,0,0,0,0,0,0,0,0],
							  	   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0],
								   [0,0,0,0,0,0,0,0,0])}
		self.run = True
		self.current_pos = '00b'
		self.bitRate = None
		self.unit_size = unit_size

	def get_pos(self):
		pos = ''
		for i in self.current_pos:
			if x == ('b' or 't'):
				pos += struct.pack('!B',(1 if x == 't' else 0))
			else:
				pos += struct.pack('!B',int(x))
		return pos

	def set_pos(self,pos):
		pos1 = ''
		for p,i in enumerate(pos):
			if p == 2:
				pos1 += ('b' if struct.unpack('!B',i)[0] == 0 else 't')
			else:
				pos1 += struct.unpack('!B',i)[0]
		self.current_pos = pos1


	def get(self,pos,offset,length):
		pit,num,side = int(pos[0]),int(pos[1]),('b' if (int(pos[2]) == 0) else 't')
		s = self.stream_buffer[side][pit][num]
		s.seek(offset)
		data = s.read(length)
		return data

	def put(self,data,pos=0,offset=0):
		if self.is_server:
			pos = self.current_pos
			self.stream_buffer[pos[2]][int(pos[0])][int(pos[1])] = data
			self.current_pos = self.get_plused_position(pos,1)


		pit,num,side = int(pos[0]),int(pos[1]),('b' if (int(pos[2]) == 0) else 't')
		if not self.stream_buffer[side][pit][num]:
			self.stream_buffer[side][pit][num] = StringIO.StringIO()
		self.stream_buffer[side][pit][num].seek(offset)
		self.stream_buffer[side][pit][num].write(data)

	def get_plused_position(self,pos,offset):
		pit = int(pos[0])
		num = int(pos[1])
		side = pos[2]
		x = int(offset)/9
		y = int(offset) - x*9
		for i in range(x):
			if pit+1 == 9:
				side = ('t' if side == 'b' else 'b')
				pit = 0
			else:
				pit = pit+1
		if num+y >= 9:
			if pit+1 == 9:
				side = ('t' if side == 'b' else 'b')
				pit = 0
				num = num+y-9
			else:
				pit = pit+1
				num = num+y-9
		else:
			num = num+y
		return (str(pit)+str(num)+side)

	def get_minused_position(self,pos,offset):
		pit = int(pos[0])
		num = int(pos[1])
		side = pos[2]
		x = int(offset)/9
		y = int(offset) - x*9
		for i in range(x):
			if pit-1 == -1:
				side = ('b' if side == 't' else 't')
				pit = 8
			else:
				pit = pit-1
		if num-y <= -1:
			if pit-1 == -1:
				side = ('b' if side == 't' else 't')
				pit = 8
				num = num-y+9
			else:
				pit = pit-1
				num = num-y+9
		else:
			num = num-y
		return (str(pit)+str(num)+side)
