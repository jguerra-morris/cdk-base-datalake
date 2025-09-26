from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_redshift as redshift,
    aws_s3 as s3,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_secretsmanager as secretsmanager,
    aws_glue as glue,
    aws_lakeformation as lakeformation,

)
from constructs import Construct


from utils import create_name

class DataConsumeStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, vpc: ec2.Vpc, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

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
