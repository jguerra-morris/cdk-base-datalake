from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_iam as iam,
)
from constructs import Construct


from utils import create_name

class DataStorageLayerStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an aws s3 bucket
        self.scripts_bucket = s3.Bucket(
            self,
            create_name(self, "bucket", "datalake-scripts"),
            bucket_name=create_name(self, "bucket", f"datalake-scripts-{self.account}"),
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Create an aws s3 bucket
        self.raw_bucket = s3.Bucket(
            self,
            create_name(self, "bucket", "datalake-raw"),
            bucket_name=create_name(self, "bucket", f"datalake-raw-{self.account}"),
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )


        self.master_bucket = s3.Bucket(
            self,
            create_name(self, "bucket", "datalake-master"),
            bucket_name=create_name(self, "bucket", f"datalake-master-{self.account}"),
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )


        self.analytics_bucket = s3.Bucket(
            self,
            create_name(self, "bucket", "datalake-analytics"),
            bucket_name=create_name(self, "bucket", f"datalake-analytics-{self.account}"),
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Deploy glue scripts to s3 bucket
        s3deploy.BucketDeployment(
            self,
            create_name(self, "deploy", "glue-scripts"),
            sources=[s3deploy.Source.asset("./glue")],
            destination_bucket=self.scripts_bucket,
            destination_key_prefix="glue",
        )



