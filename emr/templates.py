from jinja2 import Template

# for pyspark we run spark spark-submit with basically just --py-files, for JVM we have more configurations
spark_template = Template('''--deploy-mode cluster --master yarn 
{% if job_configs %}{{ job_configs }} {% endif %}
--conf 'spark.app.name={{ job_name }}'  
--conf 'spark.shuffle.service.enabled={% if job_configs %}false{% else %}true{% endif %}' 
--conf 'spark.dynamicAllocation.enabled={% if job_configs %}false{% else %}true{% endif %}'
{% if job_runtime.lower() == "python" %}--conf 'spark.yarn.appMasterEnv.ENVIRONMENT={{ env }}' --py-files {{ artifact_path }}application.zip {{ artifact_path }}main.py 
{% else %} 
--class {{ main_class }}
--conf 'spark.driver.extraClassPath=/home/hadoop/{{ artifact_parts[1] }}:/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*' 
--conf 'spark.executor.extraClassPath=/home/hadoop/*:/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*' 
--conf 'spark.driver.extraJavaOptions=-DenvironmentKey={{ env }}' 
--conf 'spark.executor.extraJavaOptions=-DenvironmentKey={{ env }}' 
{{ artifact_path }} 
{% endif %}
{{ job_args }}
''')
