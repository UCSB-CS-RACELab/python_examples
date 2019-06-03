import boto3, json, argparse, time, requests, uuid

############## detect_labels ###############
'''
   detect_labels(service_handle, folder_name, file_name, max_labels, min_confidence_desired, AWS_region)

   Pass an image in AWS S3 (bucket and name) to AWS rekognition
   and print out the labels returned from the image processing 
   service. Returns a list of labels or None (upon error)
   - The folders in S3 are called buckets, so bucket name is the folder 
   name in which the file is stored
   - file_name is the name of the file in the bucket/folder
   - max_labels and min_confidence are rekognition arguments
   learn more about rekogition here: https://aws.amazon.com/rekognition/
	- use the defaults if you are unsure
   - region is the region in which your bucket and lambda live
	- use us-west-2 (cheapest) for everything if you are unsure
   - rekog.detect_labels returns a list of labels which we print out
'''
def detect_labels(rekog, folder_name, file_name, max_labels=10, min_confidence=90, region="us-west-2"):
    try:
        response = rekog.detect_labels(
            Image={
                "S3Object": {
                    "Bucket": folder_name,
                    "Name": file_name,
                }
            },
            MaxLabels=max_labels,
            MinConfidence=min_confidence,
	)
        return response['Labels']
    except:
        print("Unable to find folder {} or filename {}.  Please retry.".format(folder_name,file_name))
        return None

############## handler - Python Lambda entry point #############
def handler(event, context): #can be called directly or triggered by AWS
    entry = time.time() * 1000
    profile = None
    region = 'us-west-2'
    if event: #set the values using passed in event
        if 'region' in event:
            region = event['region']
        if 'profile' in event:
            profile = event['profile']

    if not context: #calling from main so set the profile we want
        assert profile is not None #cannot continue if profile is not set!
        boto3.setup_default_session(profile_name=profile)
        session = boto3.Session(profile_name=profile)

    #create a handler to the AWS recognition service
    rekog = boto3.client("rekognition", region)

    bktname = None
    key = None
    if event:
            #here's what an example S3-triggered event looks like:
            #s3 event: {'Records': [{'awsRegion': 'us-west-2', 'eventName': 'ObjectCreated:Put', 'eventSource': 'aws:s3', 'eventTime': '2017-08-30T20:30:35.581Z', 'eventVersion': '2.0', 'requestParameters': {'sourceIPAddress': '98.171.178.234'}, 'responseElements': {'x-amz-id-2': 'xw4/vqjUwiRLOXwqRNAsSBiPcd72QamenQnDI/2sm/IYXm+72A1S+TQIJYjAv2oyiq3TsY6SuYQ=', 'x-amz-request-id': '4D69F866BA76CA70'}, 's3': {'bucket': {'arn': 'arn:aws:s3:::cjktestbkt', 'name': 'cjktestbkt', 'ownerIdentity': {'principalId': 'A13UVRJM0LZTMZ'}}, 'configurationId': '3debbff2-99b6-48d0-92df-6fba9b5ddda5', 'object': {'eTag': '9f2e3e584c7c8ee4866669e2d1694703', 'key': 'imgProc/deer.jpg', 'sequencer': '0059A7206B7A3C594C', 'size': 392689}, 's3SchemaVersion': '1.0'}, 'userIdentity': {'principalId': 'AWS:AIDAJQRLZF5NITGU76JME'}}]}
        #if triggered, extract the bucket/folder, dir, and filename
        if 'Records' in event:
            recs = event['Records']
            obj = recs[0]
            if 'eventSource' in obj and 'aws:s3' in obj['eventSource']:
                #s3 triggered
                assert 's3' in obj
                s3obj = obj['s3'] #get the s3 object from the record
                assert 'bucket' in s3obj
                bkt = s3obj['bucket'] #get the bucket obj from the s3 obj
                assert 'name' in bkt
                bktname = bkt['name'] #get the bucket name from the bucket obj

                assert 'object' in s3obj
                keyobj = s3obj['object'] #get the file object from the s3 obj
                assert 'key' in keyobj
                key = keyobj['key'] #get the file name from the file object (dir/fname)

        elif 'eventSource' in event and 'ext:invokeCLI' in event['eventSource']:
            #invoked via main, see main for format of event  passed in
            assert 'name' in event
            bktname = event['name'] #bucket name
            assert 'key' in event
            key = event['key'] #file name (with directory: dir/fname)

    assert bktname is not None and key is not None #we are ready to go if True!
    labels = detect_labels(rekog, bktname, key) #will be None if none
    if labels:
        print('Labels: {}'.format(json.dumps(labels)))
    else: 
        print('No labels found!')

    #post to website, for now this is just a random http server (example usage)
    key = str(uuid.uuid4())[:4]
    val = 17
    r = requests.post('http://httpbin.org/post', data = {key:val})
    print('HTTP POST status: {}'.format(r.status_code))

    delta = (time.time() * 1000) - entry
    retn_str = 'TIMER:CALL:{}'.format(delta)
    print(retn_str)
    return retn_str 

############## main - so we can call/test this directly ###############
def main():
    parser = argparse.ArgumentParser(description='imageProc tool: passes an AWS S3 image file (bucket_name/key) to AWS Rekognition and prints out the list of labels that the service returns (or None).')
    parser.add_argument('folder',action='store',help='s3 bucket/folder name')
    parser.add_argument('dir',action='store',help='file name prefix/directory in S3')
    parser.add_argument('fname',action='store',help='S3 file name')
    parser.add_argument('profile',action='store',help='AWS profile to use for credentials in ~/.aws/credentials')
    parser.add_argument('--region',action='store',default="us-west-2",help='S3 region')
    args = parser.parse_args()

    '''
    Set up a fake event (as if this was a triggered Lambda)
    The eventSource argument lets the handler know that it was triggered via 
    the command line, do not change the string ext:invokeCLI its checked in handler(): 
      python imageProc S3_Bucket S3_Directory S3_FileName myprofile

    The default region is us-west-2, use --region new_region to change
    '''
    event = {'eventSource':'ext:invokeCLI','name':args.folder,'key':'{}/{}'.format(args.dir,args.fname),'profile':args.profile}
    if args.region != 'us-west-2':
        event['region'] = args.region

    #invoke the Lambda entry point (handler)
    handler(event,None)

######## route python entry point to main() function ########
if __name__ == "__main__":
    main()

