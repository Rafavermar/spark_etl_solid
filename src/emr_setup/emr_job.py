import boto3
import logging
import time
from src.config.config import Config
from src.emr_setup.emr_cluster import EMRClusterManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class EMRJobManager:
    def __init__(self):
        self.client = boto3.client('emr', region_name='us-east-1')

    def add_pyspark_step(self, cluster_id, script_path):
        step_config = {
            'Name': 'Spark Step',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    script_path
                ]
            }
        }

        response = self.client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[step_config]
        )

        step_id_response = response['StepIds'][0]
        logging.info(f"Step created with ID: {step_id_response}")
        return step_id_response

    def wait_for_step_completion(self, cluster_id, step_id):
        while True:
            response = self.client.describe_step(ClusterId=cluster_id, StepId=step_id)
            status = response['Step']['Status']['State']
            logging.info(f"Step status: {status}")
            if status == 'COMPLETED':
                break
            elif status == 'FAILED' or status == 'CANCELLED':
                raise Exception(f"Step {step_id} failed.")
            time.sleep(30)


if __name__ == "__main__":
    emr_cluster_manager = EMRClusterManager()
    cluster_name = 'MySparkCluster'

    cluster_id_response = emr_cluster_manager.get_existing_cluster_id(cluster_name)
    if not cluster_id_response:
        logging.error(f"No active cluster found with name {cluster_name}")
    else:
        logging.info(f"Using existing cluster {cluster_id_response}")

        script_s3_path = f"s3://{Config.AWS_S3_BUCKET}/pyspark_jobs/pyspark_job.py"

        emr_job_manager = EMRJobManager()
        step_id_response = emr_job_manager.add_pyspark_step(cluster_id_response, script_s3_path)
        emr_job_manager.wait_for_step_completion(cluster_id_response, step_id_response)
