import json, argparse, boto3 

#########################
def main():
    parser = argparse.ArgumentParser(description='post a message to a topic in AWS SNS. It assumes that your AWS credentials are in the correct place ~/.aws/credentials')
    parser.add_argument('arn',action='store',help='Topic ARN')
    parser.add_argument('msg',action='store',type=str,help='SNS message')
    parser.add_argument('sub',action='store',type=str,help='SNS subject')
    args = parser.parse_args()
    arn = args.arn
    msg = args.msg
    sub = args.sub

    return post(arn,msg,sub)

#########################
def post(topic,msg,subject):

    message = {"foo": "bar"}
    boto3.setup_default_session(profile_name='racelab')
    client = boto3.client('sns',region_name='us-west-2')
    response = client.publish(
        TargetArn=topic,
        Message=json.dumps({'default': json.dumps(message),
                            'email': msg}),
        Subject=subject,
        MessageStructure='json'
    )
    return response

#########################
if __name__ == "__main__":
    main()

