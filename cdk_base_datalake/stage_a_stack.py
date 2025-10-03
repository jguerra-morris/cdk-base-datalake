from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_s3 as s3,
    aws_iam as iam,
)
from constructs import Construct


from utils import create_name

class StageAStack(Stack):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            scripts_bucket: s3.Bucket,
            raw_bucket: s3.Bucket,
            master_bucket: s3.Bucket,
            **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        

        s3_read_write_policy = iam.PolicyStatement(
                actions=["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:ListBucket"],
                resources=[
                    raw_bucket.arn_for_objects("*"),
                    scripts_bucket.arn_for_objects("*"),
                    master_bucket.arn_for_objects("*"),
                    master_bucket.bucket_arn,
                    raw_bucket.bucket_arn,
                    scripts_bucket.bucket_arn
                ],
            )
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
        step_function_policy = iam.PolicyStatement(
            sid="AllowSFAllow",
            effect=iam.Effect.ALLOW,
            actions=[
                "states:StartExecution"
            ],
            resources=["*"]
        )
        

        glue_role = iam.Role(
            self,
            create_name(self, "role", "stage-a-glue"),
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name=create_name(self, "role", "stage-a-glue"),
        )
        glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )
        glue_role.add_to_policy(secret_read_policy)
        glue_role.add_to_policy(s3_read_write_policy)
        glue_role.add_to_policy(step_function_policy)


        # Create an aws glue job for python
        glue_job = glue.CfnJob(
            self,
            create_name(self, "job", "stage-a"),
            name=create_name(self, "job", "stage-a"),
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{scripts_bucket.bucket_name}/glue/stage_a.py",
            ),
            default_arguments={
                "--SOURCE_BUCKET": raw_bucket.bucket_name,
                "--TARGET_BUCKET": master_bucket.bucket_name,
            },
            glue_version="5.0",
            max_capacity=1.0,
            timeout=10,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=3
            )
        )
