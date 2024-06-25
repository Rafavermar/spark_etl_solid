import boto3
import logging
from src.config.config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class IAMTeardown:
    def __init__(self):
        self.iam_client = boto3.client('iam')

    def delete_iam_roles_and_policies(self):
        roles = ['EMR_DefaultRole', 'EMR_EC2_DefaultRole']
        policies = ['EMR_S3_Access_Policy', 'EMR_EC2_Permissions']
        instance_profile = 'EMR_EC2_DefaultInstanceProfile'

        # Detach policies from roles
        for role in roles:
            try:
                attached_policies = self.iam_client.list_attached_role_policies(RoleName=role)
                for policy in attached_policies['AttachedPolicies']:
                    self.iam_client.detach_role_policy(RoleName=role, PolicyArn=policy['PolicyArn'])
                logging.info(f"Detached policies from role {role}")
            except self.iam_client.exceptions.NoSuchEntityException:
                logging.info(f"Role {role} does not exist, skipping detach")

        # Remove roles from instance profile
        for role in roles:
            try:
                self.iam_client.remove_role_from_instance_profile(InstanceProfileName=instance_profile, RoleName=role)
                logging.info(f"Removed role {role} from instance profile {instance_profile}")
            except self.iam_client.exceptions.NoSuchEntityException:
                logging.info(f"Instance profile {instance_profile} or role does not exist, skipping remove role")

        # Delete instance profile
        try:
            self.iam_client.delete_instance_profile(InstanceProfileName=instance_profile)
            logging.info(f"Deleted instance profile {instance_profile}")
        except self.iam_client.exceptions.NoSuchEntityException:
            logging.info(f"Instance profile {instance_profile} does not exist, skipping delete")
        except self.iam_client.exceptions.DeleteConflictException:
            logging.error(f"Cannot delete instance profile {instance_profile}, roles still attached")

        # Delete roles
        for role in roles:
            try:
                self.iam_client.delete_role(RoleName=role)
                logging.info(f"Deleted role {role}")
            except self.iam_client.exceptions.NoSuchEntityException:
                logging.info(f"Role {role} does not exist, skipping delete")

        # Delete policies
        for policy in policies:
            try:
                policy_arn = f"arn:aws:iam::{Config.AWS_ACCOUNT_ID}:policy/{policy}"
                self.iam_client.delete_policy(PolicyArn=policy_arn)
                logging.info(f"Deleted policy {policy}")
            except self.iam_client.exceptions.NoSuchEntityException:
                logging.info(f"Policy {policy} does not exist, skipping delete")


if __name__ == "__main__":
    iam_teardown = IAMTeardown()
    iam_teardown.delete_iam_roles_and_policies()
