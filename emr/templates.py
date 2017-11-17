from jinja2 import Template

emr_create_cluster_template = Template('''aws emr create-cluster{% if not airflow %} --profile {{ profile }}{% endif %}
    \t--name {{ cluster_name }}
    \t--release-label {{ emr_version }}
    \t--configurations file://configurations.json
    \t--{{ term_choice }}
    \t--service-role {{ emr_service_role }}
    \t--tags env={{ env }} owner=bigdata project={{ project }} Name={{ cluster_name }}
    \t--applications Name=Hadoop Name=Spark Name=Hue 
    \t--bootstrap-actions Path=s3://{{ env }}-sonic-artifacts/scripts/download_service.sh,Name=S3DownloadService,Args=[{% if artifact_path %}{{ job_runtime }},{{ artifact_path }}{% endif %}]
    \t--ec2-attributes InstanceProfile={{ emr_instance_profile }},KeyName={{ emr_ssh_key_name }},SubnetIds=[\'{{ master_private_subnet_app_secondary_id }}\'],ServiceAccessSecurityGroup={{ master_emrservice_security_group_id }},EmrManagedMasterSecurityGroup={{ master_emr_security_group_id }},EmrManagedSlaveSecurityGroup={{ master_emr_security_group_id }} 
    \t--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType={{ master_type }} InstanceGroupType=CORE,InstanceType={{ core_type }},InstanceCount={{ core_count }} 
    \t{% if artifact_path %}--steps{% if has_s3_checkpoints %} Type=CUSTOM_JAR,Jar=command-runner.jar,Name=S3DistCp,ActionOnFailure=CONTINUE,Args=[s3-dist-cp,--s3Endpoint=s3.amazonaws.com,--src=s3://{{ checkpoint_bucket }}/{{ job_name }}/,--dest=/checkpoints/,--srcPattern=.*]{% endif %} Type=Spark,Name={{ job_name }},ActionOnFailure=CONTINUE,Args={{ step_args }}{% endif %}
''')

# for pyspark we run spark spark-submit with basically just --py-files, for JVM we have more configurations
spark_template = Template('''--deploy-mode cluster --master yarn 
{% if job_configs %}{{ job_configs }} {% endif %}
--conf 'spark.app.name={{ job_name }}'  
--conf 'spark.yarn.max.executor.failures={{ num_executors * 8 }}'
{% if job_runtime.lower() == "python" %}--conf 'spark.yarn.appMasterEnv.ENVIRONMENT={{ env }}' --py-files {{ artifact_path }}application.zip {{ artifact_path }}main.py 
{% else %} 
--class {{ main_class }}
--conf 'spark.driver.extraClassPath=/home/hadoop/{{ artifact_parts[1] }}:/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*' 
--conf 'spark.executor.extraClassPath=/home/hadoop/{{ artifact_parts[1] }}:/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*' 
--conf 'spark.driver.extraJavaOptions=-DenvironmentKey={{ env }}' 
--conf 'spark.executor.extraJavaOptions=-DenvironmentKey={{ env }}' 
{{ artifact_path }} 
{% endif %}
{{ job_args }}
''')

add_spark_step_template = \
    Template('aws emr add-steps{% if not airflow %} --profile {{ profile }}{% endif %} --cluster-id {{ cluster_id }} '
             '--steps Type=Spark,Name={{ job_name }},ActionOnFailure=CONTINUE,Args={{ step_args }}')

add_checkpoint_cp_step_template = \
    Template('aws emr add-steps{% if not airflow %} --profile {{ profile }}{% endif %} --cluster-id {{ cluster_id }} '
             '--steps Type=CUSTOM_JAR,Jar=command-runner.jar,Name=S3DistCp,ActionOnFailure=CONTINUE,'
             'Args=[s3-dist-cp,--s3Endpoint=s3.amazonaws.com,--src=/checkpoints/,'
             '--dest=s3://{{ checkpoint_bucket }}/{{ job_name }}/,--srcPattern=.*]')
