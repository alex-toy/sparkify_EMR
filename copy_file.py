import os

args = {
    'KeyPair' : "/Users/alexei/docs/mykeypairP4.pem",
    'source_file1' : "/Users/alexei/sparkify_EMR/etl.py",
    'source_file2' : "/Users/alexei/docs/dl.cfg",
    'dns' : "hadoop@ec2-3-16-155-88.us-east-2.compute.amazonaws.com",
    'local_path' : "/home/hadoop"
}


command = '''scp -i {KeyPair} {source_file1} {dns}:{local_path}'''.format(**args)

print('*'*20)
print(command)
print('*'*20)

os.system(command)


command = '''scp -i {KeyPair} {source_file2} {dns}:{local_path}'''.format(**args)

print('*'*20)
print(command)
print('*'*20)

os.system(command)