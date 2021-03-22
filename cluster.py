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

#print(command)

os.system(command)


connect = "ssh -i "mykeypairP4.pem" root@ec2-18-220-88-190.us-east-2.compute.amazonaws.com"


ssh -i /Users/alexei/docs/mykeypairP4.pem -N -D 8157 root@ec2-18-220-88-190.us-east-2.compute.amazonaws.com



scp -i /Users/alexei/docs/mykeypairP4.pem /Users/alexei/docs/mykeypairP4.pem ec2-user@ec2-18-220-88-190.us-east-2.compute.amazonaws.com:/home/hadoop

