import boto3
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class VPCSetup:
    def __init__(self):
        self.ec2_client = boto3.client('ec2', region_name='us-east-1')

    def create_vpc(self):
        # Check if VPC already exists
        vpcs = self.ec2_client.describe_vpcs()
        for vpc in vpcs['Vpcs']:
            if 'Tags' in vpc and any(tag['Key'] == 'Name' and tag['Value'] == 'MyVPC' for tag in vpc['Tags']):
                logging.info(f"VPC 'MyVPC' already exists with ID: {vpc['VpcId']}")
                return vpc['VpcId'], self.get_subnet_id(vpc['VpcId'])

        try:
            response = self.ec2_client.create_vpc(CidrBlock='10.0.0.0/16')
            vpc_id = response['Vpc']['VpcId']
            logging.info(f"VPC created with ID: {vpc_id}")

            self.ec2_client.create_tags(Resources=[vpc_id], Tags=[{'Key': 'Name', 'Value': 'MyVPC'}])
            self.ec2_client.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={'Value': True})
            self.ec2_client.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={'Value': True})

            response = self.ec2_client.create_internet_gateway()
            igw_id = response['InternetGateway']['InternetGatewayId']
            logging.info(f"Internet Gateway created with ID: {igw_id}")

            self.ec2_client.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)

            response = self.ec2_client.create_subnet(CidrBlock='10.0.1.0/24', VpcId=vpc_id)
            subnet_id = response['Subnet']['SubnetId']
            logging.info(f"Subnet created with ID: {subnet_id}")

            route_table = self.ec2_client.create_route_table(VpcId=vpc_id)
            route_table_id = route_table['RouteTable']['RouteTableId']
            self.ec2_client.create_route(
                RouteTableId=route_table_id,
                DestinationCidrBlock='0.0.0.0/0',
                GatewayId=igw_id
            )
            self.ec2_client.associate_route_table(SubnetId=subnet_id, RouteTableId=route_table_id)
            logging.info(f"Route Table associated with Subnet ID: {subnet_id}")

            return vpc_id, subnet_id
        except ClientError as e:
            logging.error(e)
            raise

    def get_subnet_id(self, vpc_id):
        subnets = self.ec2_client.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}])
        if subnets['Subnets']:
            return subnets['Subnets'][0]['SubnetId']
        return None


if __name__ == "__main__":
    vpc_setup = VPCSetup()
    vpc_id, subnet_id = vpc_setup.create_vpc()
