from jinja2 import Template

# for pyspark we run spark spark-submit with basically just --py-files, for JVM we have more configurations
spark_template = Template('''--deploy-mode cluster --master yarn 
{% if job_runtime.lower() == "python" %}--conf 'spark.yarn.appMasterEnv.ENVIRONMENT={{ env }}' --py-files {{ artifact_path }}application.zip {{ artifact_path }}main.py 
{% else %}
{% if h2o_backend %}--num-executors {{ core_count + 1 }}{% endif %}
--conf 'spark.shuffle.service.enabled=true' 
--conf 'spark.dynamicAllocation.enabled={% if h2o_backend %}false{% else %}true{% endif %}' 
--conf 'spark.app.name={{ job_name }}'  
--class {{ main_class }}
--conf 'spark.driver.extraClassPath=/home/hadoop/{{ artifact_parts[0] }}:/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*' 
--conf 'spark.executor.extraClassPath=/home/hadoop/*:/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*' 
--conf 'spark.driver.extraJavaOptions=-DenvironmentKey={{ env }}' 
--conf 'spark.executor.extraJavaOptions=-DenvironmentKey={{ env }}' 
{{ artifact_path }} 
{% endif %}
{{ job_args }}
''')
