from time import time
import urllib2

class Input:
	def __init__(self,source):
		self.source = source
		self.con = self.connect()

	def connect(self):
		pass

	def read(self):
		pass

	def reconnect(self):
		con = self.connect()
		self.con = con

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

		t = time()
		d = ''
		while (time() - t) < 1:
			try : 
				d += source.read(1024)
			except:
				return ''
		return d

