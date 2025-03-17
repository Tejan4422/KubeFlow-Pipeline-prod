import functions_framework
from google.cloud import aiplatform

@functions_framework.cloud_event
def trigger_pipeline(cloud_event):
    """Triggered when a file is uploaded to Cloud Storage, then triggers a Vertex AI pipeline."""
    
    try:
        # Extract event data (bucket name & file name)
        data = cloud_event.data
        bucket_name = data["bucket"]
        file_name = data["name"]

        #  Check if the uploaded file is an .xlsx file
        if not file_name.lower().endswith(".xlsx"):
            print(f"Skipping non-XLSX file: {file_name}")
            return {"status": "Skipped", "reason": "Not an XLSX file", "file": file_name}


        # Log the file upload event
        print(f"New file uploaded: {file_name} in {bucket_name}")

        # Initialize Vertex AI SDK
        aiplatform.init(project="veefin-ai-426106", location="us-central1")

        # Define pipeline parameters
        pipeline_params = {
            "source_bucket": bucket_name,
            "results_bucket": "rfp_pipeline_results",  # Output bucket
            "file_name": file_name,
            "firestore_project_id": "veefin-ai-426106",
            "bigquery_table": "veefin-ai-426106.rfp_data.rfp_queries_responses_timestamps"
        }

        # Trigger Vertex AI Pipeline
        job = aiplatform.PipelineJob(
            display_name="rag-processing-pipeline",
            template_path="gs://rfp_pipeline_results/pipeline_files/optimized_rag_pipeline.json",
            parameter_values=pipeline_params,
            enable_caching=False
        )

        job.run(sync=False)  #  Run asynchronously to prevent Cloud Function timeout
        print(f"✅ Triggered Vertex AI pipeline for {file_name}")

        return {"status": "Pipeline Triggered", "file": file_name}

    except Exception as e:
        print(f"❌ Error triggering pipeline: {str(e)}")
        return {"status": "Error", "error": str(e)}