from jinja2 import Template

spark_template = Template('''--deploy-mode cluster --master yarn
{% if job_configs %}{{ job_configs }} {% endif %}
--conf 'spark.app.name={{ job_name }}'
{% if job_runtime.lower() == "python" %}--conf 'spark.yarn.appMasterEnv.ENVIRONMENT={{ env }}' --py-files {{ artifact_path }}application.zip {{ artifact_path }}main.py
{% else %}
--class {{ main_class }}
--conf 'spark.driver.extraJavaOptions=-DenvironmentKey={{ env }}'
--conf 'spark.executor.extraJavaOptions=-DenvironmentKey={{ env }}'
{{ artifact_path }}
{% endif %}
{{ job_args }}
''')

add_spark_step_template = \
    Template('aws emr add-steps{% if profile %} --profile {{ profile }}{% endif %} --cluster-id {{ cluster_id }} '
             '--steps Type=Spark,Name={{ job_name }},ActionOnFailure=CONTINUE,Args={{ step_args }}')
