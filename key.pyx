from cryptography.fernet import Fernet

def fetchkey(pwd):
	key = ''
	cipher = Fernet(key)
	return cipher.decrypt(pwd)
	
	
