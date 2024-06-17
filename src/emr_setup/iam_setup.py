import boto3
import json
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class IAMSetup:
    def __init__(self):
        self.iam_client = boto3.client('iam')

    def create_iam_roles(self):
        try:
            assume_role_policy_document = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "elasticmapreduce.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }

            emr_role = self.iam_client.create_role(
                RoleName='EMR_DefaultRole',
                AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
            )
            logging.info(f"Role created with name: EMR_DefaultRole")

            emr_ec2_role = self.iam_client.create_role(
                RoleName='EMR_EC2_DefaultRole',
                AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
            )
            logging.info(f"Role created with name: EMR_EC2_DefaultRole")

            # Attach the AmazonElasticMapReduceRole policy to the EMR role
            self.iam_client.attach_role_policy(
                RoleName='EMR_DefaultRole',
                PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
            )

            # Attach the AmazonElasticMapReduceforEC2Role policy to the EMR EC2 role
            self.iam_client.attach_role_policy(
                RoleName='EMR_EC2_DefaultRole',
                PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role'
            )

            # Create a custom policy for S3 access
            s3_access_policy_document = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:ListBucket"
                        ],
                        "Resource": [
                            "arn:aws:s3:::spark-etl-rvm",
                            "arn:aws:s3:::spark-etl-rvm/*"
                        ]
                    }
                ]
            }

            s3_access_policy = self.iam_client.create_policy(
                PolicyName='EMR_S3_Access_Policy',
                PolicyDocument=json.dumps(s3_access_policy_document)
            )
            logging.info(f"S3 access policy created with ARN: {s3_access_policy['Policy']['Arn']}")

            # Attach the S3 access policy to both roles
            self.iam_client.attach_role_policy(
                RoleName='EMR_DefaultRole',
                PolicyArn=s3_access_policy['Policy']['Arn']
            )

            self.iam_client.attach_role_policy(
                RoleName='EMR_EC2_DefaultRole',
                PolicyArn=s3_access_policy['Policy']['Arn']
            )

            return emr_role, emr_ec2_role

        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                logging.info("Role already exists. Skipping creation.")
            else:
                logging.error(e)
                raise


if __name__ == "__main__":
    iam_setup = IAMSetup()
    iam_setup.create_iam_roles()
