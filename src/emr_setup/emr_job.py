import boto3
import logging
import time

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
                    '--executor-memory', '4G',
                    '--num-executors', '10',
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
            reason = response['Step']['Status'].get('FailureDetails', {}).get('Message', 'No additional error info provided.')
            logging.info(f"Step status: {status}")
            if status == 'COMPLETED':
                break
            elif status in ['FAILED', 'CANCELLED']:
                logging.error(f"Step {step_id} failed with error: {reason}")
                raise Exception(f"Step {step_id} failed. Reason: {reason}")
            time.sleep(30)

