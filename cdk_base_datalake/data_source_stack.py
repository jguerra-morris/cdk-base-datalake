from aws_cdk import (
    Duration,
    CfnOutput,
    RemovalPolicy,
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_glue as glue,
    aws_rds as rds,

)
from constructs import Construct


from utils import create_name

class DataSourceStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, vpc: ec2.Vpc, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        secret_read_policy = iam.PolicyStatement(
            sid="AllowSecretRead",
            effect=iam.Effect.ALLOW,
            actions=[
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            resources=[
                "arn:aws:secretsmanager:"+self.region+":"+self.account+":secret:*"
            ]
        )
        vpc_networking_policy = iam.PolicyStatement(
            sid="AllowVpcNetworking",
            effect=iam.Effect.ALLOW,
            actions=[
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeVpcs",
                "ec2:DescribeSubnets",
                "ec2:DescribeSecurityGroups"
            ],
            resources=["*"]
        )
        

        glue_connection_role = iam.Role(
            self,
            create_name(self, "role", "glue-rds-connection"),
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name=create_name(self, "role", "glue-rds-connection"),
        )
        glue_connection_role.add_to_policy(secret_read_policy)
        glue_connection_role.add_to_policy(vpc_networking_policy)


        # Security Group
        security_group = ec2.SecurityGroup(
            self,
            create_name(self, "sg", "rds"),
            vpc=vpc,
            security_group_name=create_name(self, "sg", "rds"),
            description="Gives access to RDS",
        )

        security_group.add_ingress_rule(
            peer=security_group,
            connection=ec2.Port.all_traffic(),
            description="Self-referencing rule."
        )

        security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(3306),
            description="Connection to RDS from anywhere"
        )


        rds_enabled = True
        if rds_enabled:  
            

            rds_cluster = rds.DatabaseCluster(self, "rds-cluster",
                engine=rds.DatabaseClusterEngine.aurora_mysql(
                    version=rds.AuroraMysqlEngineVersion.VER_3_04_1
                ),
                credentials=rds.Credentials.from_generated_secret("admin"),
                vpc=vpc,
                security_groups=[security_group],
                vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
                writer=rds.ClusterInstance.provisioned(create_name(self, "rds-instance", "1"),
                    instance_type=ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MEDIUM)
                ),
                readers=[
                    rds.ClusterInstance.provisioned(create_name(self, "rds-instance", "2"),
                        instance_type=ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MEDIUM)
                    )
                ],
                removal_policy=RemovalPolicy.DESTROY,
                default_database_name="rdsdb",
            )

            CfnOutput(
                self,
                create_name(self, "output", "rds-endpoint"),
                value=rds_cluster.cluster_endpoint.hostname
            )
    
