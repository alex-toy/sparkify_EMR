import os

args = {
    'KeyPair' : "/Users/alexei/docs/mykeypairP4.pem",
    'source_file' : "/Users/alexei/docs/mykeypairP4.pem",
    'dns' : "ec2-user@ec2-18-220-88-190.us-east-2.compute.amazonaws.com",
    'local_path' : "/home/hadoop"
}


command = '''sudo scp -i {KeyPair} {source_file} {dns}:{local_path}'''.format(**args)


#print(command)

os.system(command)

