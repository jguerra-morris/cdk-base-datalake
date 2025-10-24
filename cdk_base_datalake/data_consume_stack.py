from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_redshift as redshift,
    aws_s3 as s3,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_secretsmanager as secretsmanager,
    aws_glue as glue)
from constructs import Construct


from utils import create_name

class DataConsumeStack(Stack):

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
            create_name(self, "role", "glue-redshift-connection"),
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name=create_name(self, "role", "glue-redshift-connection"),
        )
        glue_connection_role.add_to_policy(secret_read_policy)
        glue_connection_role.add_to_policy(vpc_networking_policy)
        
        # IAM Role for Cluster
        cluster_iam_role = iam.Role(
            self,
            create_name(self, "role", "redshift"),
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            role_name=create_name(self, "role", "redshift"),
            description="Redshift Cluster IAM Role",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3ReadOnlyAccess"
                )
            ],
        )

        # Redshift Subnet Group
        cluster_subnet_group = redshift.CfnClusterSubnetGroup(
            self,
            create_name(self, "sn-group", "redshift"),
            subnet_ids=vpc.select_subnets(
                subnet_type=ec2.SubnetType.PUBLIC
            ).subnet_ids,
            description="Redshift Demo Cluster Subnet Group",
        )

        # Security Group
        security_group = ec2.SecurityGroup(
            self,
            create_name(self, "sg", "redshift"),
            vpc=vpc,
            security_group_name=create_name(self, "sg", "redshift"),
            description="Gives access to Redshift",
        )

        security_group.add_ingress_rule(
            peer=security_group,
            connection=ec2.Port.all_traffic(),
            description="Self-referencing rule."
        )

        security_group.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(5439),
            description="Connection to cluster from anywhere"
        )



        # ---------------------- CORE GLUE METADATA STORE ----------------------
        # Create Glue Database
        glue_database = glue.CfnDatabase(
            self,
            create_name(self, "glue", "database"),
            catalog_id=self.account,
            database_input={
                "name": create_name(self, "glue", f"database-{self.account}"),
            },
        )

        redshift_enabled = True
        if redshift_enabled:  
            # Redshift Cluster
            cluster = redshift.CfnCluster(
                self,
                create_name(self, "redshift", "cluster"),
                db_name="db-name",
                cluster_identifier=create_name(self, "redshift", "cluster"),
                master_username="awsuser",
                cluster_type="multi-node",
                manage_master_password=True,
                iam_roles=[cluster_iam_role.role_arn],
                node_type="ra3.large",
                number_of_nodes=2,
                cluster_subnet_group_name=cluster_subnet_group.ref,
                vpc_security_group_ids=[security_group.security_group_id],
                publicly_accessible=False
            )        
    
