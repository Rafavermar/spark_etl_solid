import boto3
import json
import logging
from src.config.config import Config

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class IAMSetup:
    def __init__(self):
        self.iam_client = boto3.client('iam')

    def create_or_update_iam_roles(self):
        roles = {
            'EMR_EC2_DefaultRole': 'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role'
        }
        policies = [
            {
                'PolicyName': 'EMR_S3_Access_Policy',
                'PolicyDocument': {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:ListBucket",

                            ],
                            "Resource": [
                                f"arn:aws:s3:::{Config.AWS_S3_BUCKET}",
                                f"arn:aws:s3:::{Config.AWS_S3_BUCKET}/*"
                            ]
                        }
                    ]
                }
            },
            {
                'PolicyName': 'EMR_EC2_Permissions',
                'PolicyDocument': {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "ec2:Describe*",
                                "ec2:CreateSecurityGroup",
                                "ec2:AuthorizeSecurityGroupIngress",
                                "ec2:AuthorizeSecurityGroupEgress",
                                "ec2:DeleteSecurityGroup",
                                "ec2:CreateTags",
                                "ec2:RunInstances",
                                "ec2:TerminateInstances",
                                "ec2:*",
                                "iam:PassRole"
                            ],
                            "Resource": "*"
                        }
                    ]
                }
            }
        ]
        instance_profile = 'EMR_EC2_DefaultInstanceProfile'

        for role, policy_arn in roles.items():
            self.ensure_role(role, policy_arn)
        for policy in policies:
            policy_arn = self.ensure_policy(policy)
            for role in roles.keys():
                self.attach_policy_to_role(role, policy_arn)
        self.ensure_instance_profile(instance_profile, roles.keys())

    def ensure_role(self, role_name, policy_arn):
        try:
            self.iam_client.get_role(RoleName=role_name)
            logging.info(f"Role {role_name} already exists.")
        except self.iam_client.exceptions.NoSuchEntityException:
            assume_role_policy_document = {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "elasticmapreduce.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }]
            }
            self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
            )
            logging.info(f"Role {role_name} created.")
            self.iam_client.attach_role_policy(RoleName=role_name,
                                               PolicyArn=policy_arn)

    def ensure_policy(self, policy):
        policy_arn = f"arn:aws:iam::{Config.AWS_ACCOUNT_ID}:policy/{policy['PolicyName']}"
        try:
            self.iam_client.get_policy(PolicyArn=policy_arn)
            logging.info(f"Policy {policy['PolicyName']} already exists.")
        except self.iam_client.exceptions.NoSuchEntityException:
            self.iam_client.create_policy(
                PolicyName=policy['PolicyName'],
                PolicyDocument=json.dumps(policy['PolicyDocument'])
            )
            logging.info(f"Policy {policy['PolicyName']} created.")
        return policy_arn

    def attach_policy_to_role(self, role, policy_arn):
        self.iam_client.attach_role_policy(RoleName=role, PolicyArn=policy_arn)

    def ensure_instance_profile(self, instance_profile_name, role_names):
        try:
            self.iam_client.get_instance_profile(
                InstanceProfileName=instance_profile_name)
            logging.info(
                f"Instance profile {instance_profile_name} already exists.")
        except self.iam_client.exceptions.NoSuchEntityException:
            self.iam_client.create_instance_profile(
                InstanceProfileName=instance_profile_name)
            logging.info(f"Instance profile {instance_profile_name} created.")
        for role_name in role_names:
            try:
                self.iam_client.add_role_to_instance_profile(
                    InstanceProfileName=instance_profile_name,
                    RoleName=role_name)
                logging.info(
                    f"Role {role_name} added to instance profile {instance_profile_name}.")
            except (self.iam_client.exceptions.LimitExceededException,
                    self.iam_client.exceptions.EntityAlreadyExistsException) as e:
                logging.info(
                    f"Role {role_name} is already attached to instance profile {instance_profile_name}: {str(e)}")


if __name__ == "__main__":
    iam_setup = IAMSetup()
    iam_setup.create_or_update_iam_roles()
