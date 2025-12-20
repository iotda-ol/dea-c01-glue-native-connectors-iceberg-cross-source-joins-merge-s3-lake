"""
Script to create AWS Glue jobs programmatically
"""

import boto3
import argparse
import yaml
from pathlib import Path

def create_glue_job(job_name, script_location, role_arn, **kwargs):
    """
    Create an AWS Glue job.
    
    Args:
        job_name: Name of the Glue job
        script_location: S3 location of the job script
        role_arn: IAM role ARN for the job
        **kwargs: Additional job parameters
    """
    glue_client = boto3.client('glue')
    
    job_config = {
        'Name': job_name,
        'Role': role_arn,
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': script_location,
            'PythonVersion': '3'
        },
        'DefaultArguments': {
            '--enable-metrics': 'true',
            '--enable-continuous-cloudwatch-log': 'true',
            '--enable-spark-ui': 'true',
            '--enable-job-insights': 'true',
            '--job-language': 'python',
            '--TempDir': kwargs.get('temp_dir', 's3://glue-temp/'),
            '--enable-glue-datacatalog': 'true'
        },
        'MaxRetries': kwargs.get('max_retries', 1),
        'Timeout': kwargs.get('timeout', 120),
        'GlueVersion': kwargs.get('glue_version', '4.0'),
        'WorkerType': kwargs.get('worker_type', 'G.1X'),
        'NumberOfWorkers': kwargs.get('number_of_workers', 10)
    }
    
    # Add connections if specified
    if 'connections' in kwargs:
        job_config['Connections'] = {'Connections': kwargs['connections']}
    
    # Add security configuration if specified
    if 'security_configuration' in kwargs:
        job_config['SecurityConfiguration'] = kwargs['security_configuration']
    
    try:
        response = glue_client.create_job(**job_config)
        print(f"Created Glue job: {job_name}")
        return response
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Job {job_name} already exists. Updating...")
        # Remove 'Name' from config for update
        job_config.pop('Name')
        response = glue_client.update_job(JobName=job_name, JobUpdate=job_config)
        print(f"Updated Glue job: {job_name}")
        return response
    except Exception as e:
        print(f"Error creating job {job_name}: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(description='Create AWS Glue jobs')
    parser.add_argument('--source', choices=['redshift', 'teradata', 'bigquery', 'all'],
                       default='all', help='Source system for extraction job')
    parser.add_argument('--job-type', choices=['extract', 'join', 'merge', 'all'],
                       default='all', help='Type of job to create')
    parser.add_argument('--config', default='config/pipeline_config.yaml',
                       help='Path to pipeline configuration file')
    
    args = parser.parse_args()
    
    # Load configuration
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    # Get common parameters from config
    job_settings = config.get('job_settings', {})
    
    # Example: Create main ETL job
    if args.job_type in ['all', 'merge']:
        create_glue_job(
            job_name='cross-source-etl-job',
            script_location='s3://your-scripts-bucket/main_etl_job.py',
            role_arn='arn:aws:iam::ACCOUNT:role/GlueJobExecutionRole',
            temp_dir='s3://your-temp-bucket/',
            glue_version=job_settings.get('glue_version', '4.0'),
            worker_type=job_settings.get('worker_type', 'G.1X'),
            number_of_workers=job_settings.get('number_of_workers', 10),
            timeout=job_settings.get('timeout_minutes', 120)
        )
    
    print("Job creation completed!")


if __name__ == '__main__':
    main()
