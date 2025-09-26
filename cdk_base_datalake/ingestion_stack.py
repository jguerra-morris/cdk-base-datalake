from aws_cdk import (
    Stack,
    Duration,
    aws_glue as glue,
    aws_s3 as s3,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,

)
from constructs import Construct


from utils import create_name

class IngestionStack(Stack):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            scripts_bucket: s3.Bucket,
            ingestion_bucket: s3.Bucket,
            raw_bucket: s3.Bucket,
            **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        

        s3_read_write_policy = iam.PolicyStatement(
                actions=["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:ListBucket"],
                resources=[
                    raw_bucket.arn_for_objects("*"),
                    scripts_bucket.arn_for_objects("*"),
                    ingestion_bucket.arn_for_objects("*"),
                    raw_bucket.bucket_arn,
                    ingestion_bucket.bucket_arn,
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
            create_name(self, "role", "ingestion-glue"),
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name=create_name(self, "role", "ingestion-glue"),
        )
        glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )
        glue_role.add_to_policy(secret_read_policy)
        glue_role.add_to_policy(s3_read_write_policy)
        glue_role.add_to_policy(step_function_policy)


        ############### SCHEDULED EXECUTION  #####################

        # Create an aws glue job for python
        glue_job = glue.CfnJob(
            self,
            create_name(self, "job", "ingestion"),
            name=create_name(self, "job", "ingestion"),
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{scripts_bucket.bucket_name}/glue/ingestion_scheduled.py",
            ),
            default_arguments={
                "--TARGET_BUCKET": raw_bucket.bucket_name,
                "--CONNECTION_NAME": "Mysql connection",
                "--DB_TABLE": "rdsdb.delitos_junin"
            },
            glue_version="5.0",
            max_capacity=1.0,
            timeout=10,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=3
            ),
            connections=glue.CfnJob.ConnectionsListProperty(
                    connections=[
                        "Mysql connection"
                        #create_name(self, "connection", "sap-hana")
                    ]
                )
        )

        scheduled_rule = glue.CfnTrigger(
            self,
            create_name(self, "trigger", "ingestion"),
            name=create_name(self, "trigger", "ingestion"),
            type="SCHEDULED",
            schedule="cron(0 0/1 * * ? *)",
            start_on_creation=True,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=glue_job.name,
                )
            ],
        ) 





        ############ EVENT DRIVEN EXECUTION  ################
        
        glue_job_triggered = glue.CfnJob(
            self,
            create_name(self, "job", "ingestion_triggered"),
            name=create_name(self, "job", "ingestion_triggered"),
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{scripts_bucket.bucket_name}/glue/ingestion_triggered.py",
            ),
            default_arguments={
                "--TARGET_BUCKET": raw_bucket.bucket_name,
                "--SOURCE_BUCKET": ingestion_bucket.bucket_name,
                "--DB_TABLE": "rdsdb.delitos_junin"
            },
            glue_version="5.0",
            max_capacity=1.0,
            timeout=10,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=3
            )
        )


        # 3. Lambda to start Glue Job
        lambda_fn = _lambda.Function(self, create_name(self, "lambda", "start-glue-ingestion"),
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.handler",
            timeout=Duration.seconds(120),
            function_name=create_name(self, "lambda", "start-glue-ingestion"),
            code=_lambda.Code.from_inline(
                f"""
import boto3
def handler(event, context):
    client = boto3.client('glue')
    response = client.start_job_run(JobName='{glue_job_triggered.name}')
    print("Started Glue Job:", response)
    return response
"""
            )
        )
        
        # Permissions for Lambda to start Glue Job
        lambda_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["glue:StartJobRun"],
            resources=["*"] 
        ))

        # 4. EventBridge Rule - S3 PutObject
        rule = events.Rule(self, create_name(self, "rule", "s3-putobject"),
            rule_name=create_name(self, "rule", "s3-putobject"),
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                resources=[ingestion_bucket.bucket_arn],
            )
        )

        # 5. Add Lambda as Target
        rule.add_target(targets.LambdaFunction(lambda_fn))













