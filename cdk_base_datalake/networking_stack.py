from aws_cdk import (
    Stack,
    CfnOutput,
    aws_ec2 as ec2
)
from constructs import Construct
from utils import create_name
from aws_cdk import aws_iam as iam

class NetworkingStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        self.vpc = ec2.Vpc(
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
        

        # Create a transit gateway attatchment
        tgw_attachment = ec2.CfnTransitGatewayAttachment(
            self,
            create_name(self, "tgw", "attachment"),
            transit_gateway_id="tgw-0cf1f3933cccd7532",
            vpc_id=self.vpc.vpc_id,
            subnet_ids=self.vpc.select_subnets(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ).subnet_ids,
            tags=[{"key": "Name", "value": create_name(self, "tgw", "attachment")}],
        )


        # Select private subnets
        private_subnets = self.vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        ).subnets


        # Add a route to each private subnet's route table
        for i, subnet in enumerate(private_subnets):
            routes = ec2.CfnRoute(
                self,
                f"PrivateSubnetTgwRoute{i}",
                route_table_id=subnet.route_table.route_table_id,
                destination_cidr_block="10.100.3.158/32",
                transit_gateway_id="tgw-0cf1f3933cccd7532",
            )

            routes.node.add_dependency(tgw_attachment)


        # Security Group
        security_group = ec2.SecurityGroup(
            self,
            create_name(self, "sg", "instance-test"),
            vpc=self.vpc,
            security_group_name=create_name(self, "sg", "instance-test"),
            description="Gives access to Instance",
        )
        
        security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4("10.100.3.158/32"),
            connection=ec2.Port.all_traffic(),  #5439
            description="Connection to cluster from anywhere"
        )


        instance_test = False
        if instance_test:
            # Add a role for an EC2 instance that can be connected through SSM
            role = iam.Role(
                self,
                create_name(self, "role", "instance-test"),
                assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
                role_name=create_name(self, "role", "instance-test"),
            )
            role.add_managed_policy(
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                )
            )

            # Create an ec2 instance with role and in private with egress subnets
            instaceTest = ec2.Instance(
                self,
                create_name(self, "instance", "test"),
                vpc=self.vpc,
                role=role,
                security_group=security_group,
                instance_type=ec2.InstanceType.of(
                    ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MEDIUM
                ),
                machine_image=ec2.MachineImage.latest_amazon_linux2(),
                vpc_subnets=ec2.SubnetSelection(
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
                ),
            )

            # Create a Cloudformation output for the instanceId
            CfnOutput(
                self,
                create_name(self, "output", "ssm-connection"),
                value="aws ssm start-session --target "+instaceTest.instance_id+"  --profile marina --region us-east-1",
            )

