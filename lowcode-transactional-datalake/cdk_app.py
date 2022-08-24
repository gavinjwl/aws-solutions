import os

from aws_cdk import (App, Aws, CfnOutput, CfnParameter, Duration, Stack, aws_glue,
                     aws_iam, aws_lambda, aws_lambda_event_sources, aws_logs,
                     aws_s3)
from aws_cdk import aws_s3_deployment as s3deploy
from aws_cdk import aws_s3_notifications, aws_sqs
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks
from constructs import Construct


class SampleStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # default parameters could be customized
        database_name = 'lowcode_transactional_database'
        table_name = 'lowcode_transactional_table'

        # default code base path
        codebase_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'lowcode_transactional_datalake'
        )

        # bucket for source code, data ingestion, temp data, glue database/table
        bucket = aws_s3.Bucket(
            self, 'bucket',
        )

        # S3 object notification
        queue = aws_sqs.Queue(self, id='queue')
        bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,
            aws_s3_notifications.SqsDestination(queue),
            aws_s3.NotificationKeyFilter(prefix='raw/'),
        )
        trigger_function = aws_lambda.Function(
            self, 'trigger_stepfunctions-function',
            code=aws_lambda.Code.from_asset(
                os.path.join(codebase_path, 'lambda_trigger_stepfunctions')
            ),
            handler='lambda_function.lambda_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            memory_size=128,
            timeout=Duration.seconds(30),
            log_retention=aws_logs.RetentionDays.TWO_WEEKS,
            reserved_concurrent_executions=1,
            dead_letter_queue_enabled=True,
            events=[
                aws_lambda_event_sources.SqsEventSource(queue),
            ],
        )

        # stepfunctions - step 1
        clean_s3_function = aws_lambda.Function(
            self, 'clean_s3-function',
            code=aws_lambda.Code.from_asset(
                os.path.join(codebase_path, 'lambda_clean_s3')
            ),
            handler='lambda_function.lambda_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            memory_size=128,
            timeout=Duration.seconds(30),
            log_retention=aws_logs.RetentionDays.TWO_WEEKS,
            dead_letter_queue_enabled=True,
            environment={
                'BUCKET': bucket.bucket_name,
                'PREFIX': 'temp/',
            },
        )
        bucket.grant_read(clean_s3_function)
        bucket.grant_delete(clean_s3_function)

        # glue data catalog
        aws_glue.CfnDatabase(
            self, 'database',
            catalog_id=Aws.ACCOUNT_ID,
            database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
                name=database_name,
                location_uri=f's3://{bucket.bucket_name}/{database_name}/',
            )
        )
        aws_glue.CfnTable(
            self, 'table',
            catalog_id=Aws.ACCOUNT_ID,
            database_name=database_name,
            table_input=aws_glue.CfnTable.TableInputProperty(
                name=table_name,
            )
        )

        # IAM role for Glue job
        glue_role = aws_iam.Role(
            self, 'glue_role',
            assumed_by=aws_iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name('AmazonEC2ContainerRegistryReadOnly'),
            ]
        )
        bucket.grant_read_write(glue_role)
        bucket.grant_delete(glue_role)

        # Stepfunctions - step 2
        s3deploy.BucketDeployment(
            self, "csv_to_parquet-code",
            sources=[s3deploy.Source.asset(
                os.path.join(codebase_path, 'glue_csv_to_parquet')
            )],
            destination_bucket=bucket,
            destination_key_prefix="scripts"
        )
        csv_to_parquet = aws_glue.CfnJob(
            self, 'csv_to_parquet-glue-job',
            name='csv_to_parquet-glue-job',
            log_uri=f's3://{bucket.bucket_name}/sparkHistoryLogs/',
            role=glue_role.role_name,
            execution_property=aws_glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            command=aws_glue.CfnJob.JobCommandProperty(
                name='glueetl',
                script_location=f's3://{bucket.bucket_name}/scripts/csv_to_parquet.py',
                python_version='3'
            ),
            default_arguments={
                '--SOURCE_BUCKET': bucket.bucket_name,
                '--SOURCE_KEY': 'raw/',
                '--TARGET_BUCKET': bucket.bucket_name,
                '--TARGET_PREFIX': 'temp/',
                #
                '--class': 'GlueApp',
                '--job-language': 'python',
                '--TempDir': f's3://{bucket.bucket_name}/temporary/',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-continuous-log-filter': 'true',
                '--enable-glue-datacatalog': 'true',
                '--enable-job-insights': 'true',
                '--enable-metrics': 'true',
                '--enable-spark-ui': 'true',
                '--enable-auto-scaling': 'true',
                '--enable-s3-parquet-optimized-committer': 'true',
                '--job-bookmark-option': 'job-bookmark-disable',
                '--spark-event-logs-path': f's3://{bucket.bucket_name}/sparkHistoryLogs/',
            },
            max_retries=0,
            timeout=60,
            glue_version='3.0',
            number_of_workers=2,
            worker_type='G.1X'
        )

        # Stepfunctions - step 3
        s3deploy.BucketDeployment(
            self, "parquet_to_hudi_glue-code",
            sources=[s3deploy.Source.asset(
                os.path.join(
                    codebase_path, 'glue_parquet_to_hudi')
            )],
            destination_bucket=bucket,
            destination_key_prefix="scripts"
        )
        parquet_to_hudi = aws_glue.CfnJob(
            self, 'parquet_to_hudi_glue-glue-job',
            name='parquet_to_hudi_glue-glue-job',
            log_uri=f's3://{bucket.bucket_name}/sparkHistoryLogs/',
            role=glue_role.role_name,
            execution_property=aws_glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            command=aws_glue.CfnJob.JobCommandProperty(
                name='glueetl',
                script_location=f's3://{bucket.bucket_name}/scripts/parquet_to_hudi_glue.py',
                python_version='3'
            ),
            default_arguments={
                '--SOURCE_BUCKET': bucket.bucket_name,
                '--SOURCE_KEY': 'temp/',
                '--TARGET_BUCKET': bucket.bucket_name,
                '--TARGET_DATABASE': database_name,
                '--TARGET_TABLE': table_name,
                #
                '--class': 'GlueApp',
                '--job-language': 'python',
                '--TempDir': f's3://{bucket.bucket_name}/temporary/',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-continuous-log-filter': 'true',
                '--enable-glue-datacatalog': 'true',
                '--enable-job-insights': 'true',
                '--enable-metrics': 'true',
                '--enable-spark-ui': 'true',
                '--enable-auto-scaling': 'true',
                '--enable-s3-parquet-optimized-committer': 'true',
                '--extra-jars': '/tmp/*',
                '--job-bookmark-option': 'job-bookmark-disable',
                '--spark-event-logs-path': f's3://{bucket.bucket_name}/sparkHistoryLogs/',
                '--conf': 'spark.serializer=org.apache.spark.serializer.KryoSerializer',
            },
            max_retries=0,
            timeout=60,
            glue_version='3.0',
            number_of_workers=2,
            worker_type='G.1X'
        )

        # Construct Stepfunctions
        clean_s3_task = sfn_tasks.LambdaInvoke(
            self, 'clean_s3-task',
            lambda_function=clean_s3_function,
        )
        csv_to_parquet_task = sfn_tasks.GlueStartJobRun(
            self, 'csv_to_parquet-task',
            glue_job_name=csv_to_parquet.name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )
        parquet_to_hudi_task = sfn_tasks.GlueStartJobRun(
            self, 'parquet_to_hudi-task',
            glue_job_name=parquet_to_hudi.name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )
        definition = (
            clean_s3_task
            .next(csv_to_parquet_task)
            .next(parquet_to_hudi_task)
        )
        workflow = sfn.StateMachine(
            self, 'workflow-sfn',
            definition=definition,
            timeout=Duration.hours(2),
        )
        workflow.grant_start_execution(trigger_function)
        trigger_function.add_environment(
            'STATE_MACHINE_ARN', workflow.state_machine_arn
        )


app = App()
SampleStack(
    app, 'lowcode-transactional-datalake'
)
app.synth()
