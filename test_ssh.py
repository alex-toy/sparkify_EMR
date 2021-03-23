import os

args = {
    'KeyPair' : "/Users/alexei/docs/mykeypairP4.pem",
    'dns' : "hadoop@ec2-3-16-155-88.us-east-2.compute.amazonaws.com"
}


command = '''ssh -i {KeyPair} {dns}'''.format(**args)


print('*'*20)
print(command)
print('*'*20)

os.system(command)

