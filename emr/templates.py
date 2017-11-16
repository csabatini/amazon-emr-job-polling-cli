from jinja2 import Template

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
    Template('aws emr add-steps{% if not airflow %} --profile {{ profile }}{% endif %} --cluster-id {{ cluster_id }}'
             '--steps Type=Spark,Name={{ job_name }},ActionOnFailure=CONTINUE,Args={{ step_args }}')

add_checkpoint_cp_step_template = \
    Template('aws emr add-steps{% if not airflow %} --profile {{ profile }}{% endif %} --cluster-id {{ cluster_id }}'
             '--steps Type=CUSTOM_JAR,Jar=command-runner.jar,Name=S3DistCp,ActionOnFailure=CONTINUE,'
             'Args=[s3-dist-cp,--s3Endpoint=s3.amazonaws.com,--src=/checkpoints/,'
             '--dest=s3://{{ checkpoint_bucket }}/{{ job_name }}/,--srcPattern=".*"]')
