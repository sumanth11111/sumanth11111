import boto3
import json

import psycopg2

s3 = boto3.client("s3")
#rds = boto3.client("rds")
def read_data_from_s3(event): 
    employee_list=[]
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    resp = s3.get_object(Bucket=bucket,Key=key)
    data = resp['Body'].read().decode('utf-8').split("\r\n")
    for row in data:
        print(row)
        employee_list.append(row.split(","))
    
    #return data  
        
    return employee_list

  
def lambda_handler(event, context):
    conn = psycopg2.connect(user="postgres", password="postgres",
                                  host="sumanth-database-1.c8qb617opkkd.ap-south-1.rds.amazonaws.com", port="5432",
                                  database="postgres")


    #Creating a cursor object using the cursor() method
    cur = conn.cursor()

    #Doping EMPLOYEE table if already exists.
    cur.execute("DROP TABLE IF EXISTS sumanth_database")
    
    cur = conn.cursor()
    cur.execute("CREATE TABLE sumanth_database(EName CHAR(50), EID CHAR(50), ESalary CHAR(50), ELoc CHAR(50), ECellno CHAR(50), EJobid CHAR(50), EDept CHAR(50))")
    conn.commit()
   
    #cursor.execute()
    print("Table created successfully........")

    #Closing the connection
    #conn.close()
    

    data = read_data_from_s3(event)

    new_list=[]
    # print(data)
    for row in range(1,len(data)-1):
        print(row)
        d={}
        d['Ename']=data[row][0]
        d['EID']=data[row][1]
        d['ESalary']=data[row][2]
        d['ELoc']=data[row][3]
        d['ECellNo']=data[row][4]
        d['EJobid']=data[row][5]
        d['EDept']=data[row][6]
        new_list.append(d)
        
    print(new_list)
    
    for row in new_list:
      ab = """INSERT INTO sumanth_database VALUES('{ename}','{eid}','{esal}','{eloc}','{ecell}','{ejob}','{edept}');""".format(ename=row['Ename'],eid=row['EID'],esal=row['ESalary'],eloc=row['ELoc'],ecell=row['ECellNo'],ejob=row['EJobid'],edept=row['EDept'])
      cur.execute(ab) 
      #print(row['Ename'])
    print("rows inserted")
   
    if conn:
        conn.commit()
        

#def main():
    #topicArn = 'arn:aws:sns:ap-south-1:078337804161:sumanth-sns'
    #endpoint = 'arn:aws:lambda:ap-south-1:078337804161:function:sumanth'
    #sns = boto3.client('sns', 
                  #      aws_access_key_id = 'AKIAIKHXMUWMRRQZ23CA',
                   #     aws_secret_access_key = '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',
                    #    region_name = 'ap-south-1')

    #publishobject = "sumanth"

    #responce = sns.publish(TopicArn = topicArn,
     #                       #Endpoint=endpoint,
      #                      Message = publishobject,
       #                     Subject = 'EMPLOYEE-DATA')
                            #MessageAttributes = {"Ename, EID, ESalary, ELoc, ECellno, EJobid, EDept"})

    #print(responce['ResponceMetadata']['HTTPStatusCode'])                             
    
            
    
#read_data_from_s3('event') 
event = {
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "aws:s3",
      "awsRegion": "ap-south-1",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "responseElements": {
        "x-amz-request-id": "EXAMPLE123456789",
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "sumanth",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          },
          "arn": "arn:aws:s3:::example-bucket"
        },
        "object": {
          "key": "aws.csv",
          "size": 1024,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}
#main()
lambda_handler(event, 'context')