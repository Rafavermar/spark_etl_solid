import boto3
import time
import logging
from botocore.exceptions import ClientError
from src.config.config import Config
from src.emr_setup.upload_script import S3Uploader
from src.emr_setup.iam_setup import IAMSetup
from src.emr_setup.vpc_setup import VPCSetup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class EMRClusterManager:
    def __init__(self):
        self.client = boto3.client('emr', region_name='us-east-1')
        self.ec2_client = boto3.client('ec2', region_name='us-east-1')

    def get_existing_cluster_id(self, emr_cluster_name):
        clusters = self.client.list_clusters(
            ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
        )
        for cluster in clusters['Clusters']:
            if cluster['Name'] == emr_cluster_name:
                return cluster['Id']
        return None

    def create_key_pair(self, key_name):
        try:
            self.ec2_client.describe_key_pairs(KeyNames=[key_name])
            logging.info(f"Key pair {key_name} already exists.")
        except ClientError:
            response = self.ec2_client.create_key_pair(KeyName=key_name)
            with open(f"{key_name}.pem", "w") as file:
                file.write(response["KeyMaterial"])
            logging.info(f"Key pair {key_name} created and saved as {key_name}.pem")

    def create_cluster(self, emr_cluster_name, subnet_id):
        response = self.client.run_job_flow(
            Name=emr_cluster_name,
            ServiceRole='EMR_EC2_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultInstanceProfile',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Core nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 2,
                    },
                ],
                'Ec2KeyName': Config.EC2_KEY_NAME,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': subnet_id
            },
            Applications=[
                {'Name': 'Hadoop'},
                {'Name': 'Spark'},
            ],
            Configurations=[
                {
                    'Classification': 'spark',
                    'Properties': {
                        'maximizeResourceAllocation': 'true'
                    }
                }
            ],
            VisibleToAllUsers=True,
            LogUri=f's3://{Config.LOG_BUCKET}/',
            ReleaseLabel='emr-6.3.0'
        )
        emr_cluster_id = response['JobFlowId']
        logging.info(f"Cluster created with ID: {emr_cluster_id}")
        return emr_cluster_id

    def wait_for_cluster_ready(self, emr_cluster_id):
        while True:
            response = self.client.describe_cluster(ClusterId=emr_cluster_id)
            status = response['Cluster']['Status']['State']
            logging.info(f"Cluster status: {status}")
            if status == 'WAITING':
                break
            elif status == 'TERMINATED_WITH_ERRORS' or status == 'TERMINATED':
                raise Exception(f"Cluster {emr_cluster_id} failed to start.")
            time.sleep(30)


def setup_and_run_emr_job():
    iam_setup = IAMSetup()
    iam_setup.create_or_update_iam_roles()
    time.sleep(30)  # Agregar una espera para asegurarse de que los roles y las políticas se propaguen

    vpc_setup = VPCSetup()
    vpc_id, subnet_id = vpc_setup.create_vpc()

    emr_manager = EMRClusterManager()
    emr_cluster_name = 'MySparkCluster'
    emr_manager.create_key_pair(Config.EC2_KEY_NAME)

    script_path = Config.PYSPARK_JOB_PATH
    script_s3_key = 'pyspark_jobs/pyspark_job.py'

    uploader = S3Uploader()
    uploader.upload_script(script_path, Config.AWS_S3_BUCKET, script_s3_key)

    emr_cluster_id = emr_manager.get_existing_cluster_id(emr_cluster_name)
    if not emr_cluster_id:
        emr_cluster_id = emr_manager.create_cluster(emr_cluster_name, subnet_id)
        emr_manager.wait_for_cluster_ready(emr_cluster_id)
    else:
        logging.info(f"Using existing cluster {emr_cluster_id}")

    script_s3_path = f"s3://{Config.AWS_S3_BUCKET}/{script_s3_key}"

    from src.emr_setup.emr_job import EMRJobManager  # Importar aquí para evitar importaciones circulares
    emr_job_manager = EMRJobManager()
    job_step_id = emr_job_manager.add_pyspark_step(emr_cluster_id, script_s3_path)
    emr_job_manager.wait_for_step_completion(emr_cluster_id, job_step_id)


if __name__ == "__main__":
    setup_and_run_emr_job()
