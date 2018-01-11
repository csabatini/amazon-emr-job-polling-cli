Amazon EMR Job Polling CLI
==========================
.. image:: https://travis-ci.org/csabatini/amazon-emr-job-polling-cli.svg?branch=master
    :target: https://travis-ci.org/csabatini/amazon-emr-job-polling-cli
.. image:: https://codecov.io/gh/csabatini/amazon-emr-job-polling-cli/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/csabatini/amazon-emr-job-polling-cli
Command line interface for submitting and polling the state of Elastic MapReduce jobs.

Why?
----
By default, the Amazon EMR APIs are asynchronous. They return a Resource ID (ClusterId, StepId), and require additional calls to get the current state.

There is `wait <https://docs.aws.amazon.com/cli/latest/reference/emr/wait/index.html>`_ functionality in the AWS CLI, however, it only supports the commands ``wait cluster-running`` and ``wait cluster-terminated``.

This project abstracts the EMR Steps API. You can submit Spark or MapReduce steps to a cluster, and poll until they succeed or fail. It can be useful if you have an ETL workflow that involves dependencies between EMR and non-EMR tasks.


Usage
-----
1. Run tests in a virtual environment with ``tox``
2. Install depedencies with ``pip install -r requirements.txt``
3. Run ``python -m emr.job_client --help`` for help with the available options
4. Optional: modify the ``logging.yml`` configuration, or set the LOG_CFG environment variable to a custom file.

Note: an error will be raised if the named EMR cluster isn't found using your AWS profile or instance role.

Example
-------
Command: ::
    
    python -m emr.job_client --env qa \
        --profile qa \
        --cluster-name Sandbox \
        --job-name WordCount \
        --job-runtime Java \
        --job-args "hdfs:///text-input/" \
        --artifact-path s3://us-east-1.elasticmapreduce/samples/wordcount.jar \
        --main-class org.apache.spark.examples.WordCount \
        --no-auto-terminate \
        --poll-cluster

Output: ::

    INFO     environment=qa, cluster=Sandbox, job=WordCount, action=check-runtime, runtime=Java
    INFO     environment=qa, cluster=Sandbox, job=WordCount, action=get-clusters, count=1, clusterList=[{"id": "j-6AEOL53QG34E", "name": "Sandbox", "state": "WAITING"}]
    INFO     aws emr add-steps --profile qa --cluster-id j-2MTD0ERMUNR2A --steps Type=Spark,Name=WordCount,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,--conf,'spark.app.name=WordCount',--class,org.apache.spark.examples.WordCount,--conf,'spark.driver.extraJavaOptions=-DenvironmentKey=qa',--conf,'spark.executor.extraJavaOptions=-DenvironmentKey=qa',s3://us-east-1.elasticmapreduce/samples/wordcount.jar,hdfs:///text-input/]
    {
        "StepIds": [
            "s-1YGO9JYO1D6HV"
        ]
    }
    INFO     environment=qa, cluster=Sandbox, job=WordCount, action=add-job-step
    INFO     environment=qa, cluster=Sandbox, job=WordCount, action=get-steps, clusterId=j-6AEOL53QG34E, numSteps=1
    INFO     environment=qa, cluster=Sandbox, job=WordCount, action=poll-cluster, stepId=s-1GJOV3B7L7228, state=PENDING, createdTime=2017-12-28T18-20-08, minutesElapsed=1.0
    INFO     environment=qa, cluster=Sandbox, job=WordCount, action=poll-cluster, stepId=s-1GJOV3B7L7228, state=RUNNING, createdTime=2017-12-28T18-20-08, minutesElapsed=2.0
    INFO     environment=qa, cluster=Sandbox, job=WordCount, action=poll-cluster, stepId=s-1GJOV3B7L7228, state=RUNNING, createdTime=2017-12-28T18-20-08, minutesElapsed=3.0
    INFO     environment=qa, cluster=Sandbox, job=WordCount, action=poll-cluster, stepId=s-1GJOV3B7L7228, state=RUNNING, createdTime=2017-12-28T18-20-08, minutesElapsed=4.0
    INFO     environment=qa, cluster=Sandbox, job=WordCount, action=poll-cluster, stepId=s-1GJOV3B7L7228, state=COMPLETED, createdTime=2017-12-28T18-20-08, minutesElapsed=5.0

