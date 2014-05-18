from django.db import models

class UP(models.Model):
	username = models.CharField(max_length=30, unique=True, blank=False)
	password = models.CharField(max_length=30, blank=False)

class URLS(models.Model):
	user = models.ForeignKey(UP)
	url = models.URLField(unique=True)
	expire = models.DateTimeField()
#class User(models.Model):
#	email = models.EmailField(max_length=50, unique=True)
#	password = models.CharField(max_length=30)
#	
#class Profile(models.Model):
#	username = models.ForeignKey('User')
#	