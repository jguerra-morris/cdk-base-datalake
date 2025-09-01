from aws_cdk import (
    Stack,
    aws_ec2 as ec2
)
from constructs import Construct
from utils import create_name

class NetworkingStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        vpc = ec2.Vpc(
            self,
            create_name(self, "vpc", "redshift"),
            ip_addresses=ec2.IpAddresses.cidr("10.130.0.0/16"),
            max_azs=3,
            enable_dns_support=True,
            enable_dns_hostnames=True,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public_subnet",
                    cidr_mask=24,
                    subnet_type=ec2.SubnetType.PUBLIC,
                ),
                ec2.SubnetConfiguration(
                    name="private_subnet",
                    cidr_mask=24,
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                ),
            ],
        )