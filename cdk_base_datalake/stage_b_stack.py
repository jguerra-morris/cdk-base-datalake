from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_sns as sns,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
    RemovalPolicy,
    Stack,
    aws_glue as glue,
    aws_s3 as s3,
    aws_iam as iam,
)
from constructs import Construct


from utils import create_name

class StageBStack(Stack):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            scripts_bucket: s3.Bucket,
            master_bucket: s3.Bucket,
            analytics_bucket: s3.Bucket,
            **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        

        s3_read_write_policy = iam.PolicyStatement(
                actions=["s3:PutObject", "s3:GetObject"],
                resources=[
                    master_bucket.arn_for_objects("*"),
                    scripts_bucket.arn_for_objects("*"),
                    analytics_bucket.arn_for_objects("*"),
                    analytics_bucket.bucket_arn,
                    master_bucket.bucket_arn,
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
        

        glue_role = iam.Role(
            self,
            create_name(self, "role", "stage-b-glue"),
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name=create_name(self, "role", "stage-b-glue"),
        )
        glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )
        glue_role.add_to_policy(secret_read_policy)
        glue_role.add_to_policy(s3_read_write_policy)


        # Create an aws glue job for python
        glue_job = glue.CfnJob(
            self,
            create_name(self, "job", "stage-b"),
            name=create_name(self, "job", "stage-b"),
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{scripts_bucket.bucket_name}/glue/stage_b.py",
            ),
            default_arguments={
                "--SOURCE_BUCKET": master_bucket.bucket_name,
                "--TARGET_BUCKET": analytics_bucket.bucket_name,
                "--CONNECTION_NAME": create_name(self, "connection", "redshift"),
                "--REDSHIFT_TABLE": "public.banmedica_test",
            },
            glue_version="4.0",
            max_capacity=1.0,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            connections=glue.CfnJob.ConnectionsListProperty(
                    connections=[
                        create_name(self, "connection", "redshift")
                    ]
                )
        )

        glue_task = tasks.GlueStartJobRun(
            self,
            create_name(self, "task", "stage-b"),
            glue_job_name=glue_job.name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--KEY.$": "$.key"
            }),
            output_path="$",
            input_path="$",
            result_path="$",
        )

        topic = sns.Topic(
            self,
            create_name(self, "topic", "stage-b"),
            display_name="Topico de notificacion de funcionalidad del Stage B.",
            topic_name=create_name(self, "topic", "stage-b"),
        )

        job_failed_task = tasks.SnsPublish(
            self,
            create_name(self, "task", "job-failed"),
            topic=topic,
            message=sfn.TaskInput.from_object({
                "Error": "Job failed",
                "Cause": sfn.JsonPath.string_at("$.error"),
            }),
            result_path="$.error",
            subject="Failed Pipeline",
        )

        step_function_definition = glue_task.add_catch(
            job_failed_task,
            result_path="$.error",
        )


        sf_machine = sfn.StateMachine(
            self,
            create_name(self, "state-machine", "data-stage-b"),
            state_machine_name=create_name(self, "state-machine", "data-stage-b"),
            definition_body=sfn.DefinitionBody.from_chainable(step_function_definition),
            timeout=Duration.minutes(10),
        )
