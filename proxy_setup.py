import os

args = {
    'KeyPair' : "/Users/alexei/docs/mykeypairP4.pem",
    'dns' : "ec2-user@ec2-3-139-59-143.us-east-2.compute.amazonaws.com",
    'port' : 8157
}


command = '''ssh -i {KeyPair} -N -D {port} {dns}'''.format(**args)


#print(command)

os.system(command)
