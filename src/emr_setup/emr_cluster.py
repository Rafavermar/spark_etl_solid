import boto3
import time
import logging
from src.config.config import Config
from src.emr_setup.upload_script import S3Uploader
from src.emr_setup.emr_job import EMRJobManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class EMRClusterManager:
    def __init__(self):
        self.client = boto3.client('emr', region_name='us-east-1')

    def get_existing_cluster_id(self, cluster_name):
        clusters = self.client.list_clusters(
            ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
        )
        for cluster in clusters['Clusters']:
            if cluster['Name'] == cluster_name:
                return cluster['Id']
        return None

    def create_cluster(self, cluster_name):
        response = self.client.run_job_flow(
            Name=cluster_name,
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
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
                'Ec2KeyName': 'your-key-pair-name',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
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
            LogUri='s3://your-log-bucket/',
            ReleaseLabel='emr-6.3.0'
        )
        cluster_id = response['JobFlowId']
        logging.info(f"Cluster created with ID: {cluster_id}")
        return cluster_id

    def wait_for_cluster_ready(self, cluster_id):
        while True:
            response = self.client.describe_cluster(ClusterId=cluster_id)
            status = response['Cluster']['Status']['State']
            logging.info(f"Cluster status: {status}")
            if status == 'WAITING':
                break
            elif status == 'TERMINATED_WITH_ERRORS' or status == 'TERMINATED':
                raise Exception(f"Cluster {cluster_id} failed to start.")
            time.sleep(30)


if __name__ == "__main__":
    emr_manager = EMRClusterManager()
    cluster_name = 'MySparkCluster'
    script_path = Config.PYSPARK_JOB_PATH
    script_s3_key = 'pyspark_jobs/pyspark_job.py'

    uploader = S3Uploader()
    uploader.upload_script(script_path, Config.AWS_S3_BUCKET, script_s3_key)

    cluster_id = emr_manager.get_existing_cluster_id(cluster_name)
    if not cluster_id:
        cluster_id = emr_manager.create_cluster(cluster_name)
        emr_manager.wait_for_cluster_ready(cluster_id)
    else:
        logging.info(f"Using existing cluster {cluster_id}")

    script_s3_path = f"s3://{Config.AWS_S3_BUCKET}/{script_s3_key}"
    emr_job_manager = EMRJobManager()
    step_id = emr_job_manager.add_pyspark_step(cluster_id, script_s3_path)
    emr_job_manager.wait_for_step_completion(cluster_id, step_id)
