from cryptography.fernet import Fernet

def fetchkey(pwd):
	key = 'wkVimmLEvBYEcV7osDo3VDZWxqiDq_HL5SyHROwhccw='
	cipher = Fernet(key)
	return cipher.decrypt(pwd)
	
	
