from googleapiclient.discovery import build


def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "practice-pde"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bq-load",  # Provide a unique name for the job
    "parameters": {
        "inputFilePattern": "gs://ranking-data-etl/batsmen_rankings.csv",
        "JSONPath": "gs://ranking-data-etl/bq.json",
        "outputTable": "practice-pde:Rank.Ranking",
        "bigQueryLoadingTemporaryDirectory": "gs://bkt-metadata-gcs/temp/",
        "javascriptTextTransformGcsPath": "gs://ranking-data-etl/udf.js",
        "javascriptTextTransformFunctionName": "transform"
    }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)

