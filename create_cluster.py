import os

args = {
    'name' : 'clusterP4',
    'KeyPair' : "mykeypairP4",
    'Nb_instance' : 3,
    'application_name' : "Spark",
    #'terminate' : "--auto-terminate"
    'terminate' : ""
}

command = '''
aws emr create-cluster --name {name} --use-default-roles --release-label emr-5.28.0 \
--instance-count {Nb_instance} --applications Name={application_name} \
--ec2-attributes KeyName={KeyPair} \
--instance-type m5.xlarge {terminate} 
'''.format(**args)

print('*'*20)
print(command)
print('*'*20)

os.system(command)

 