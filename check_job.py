from src.transform import check_gemini_batch_job

job_id = "batches/fw24e23xttssxb7d9r2h91l94juvu82i43np"

status = check_gemini_batch_job(job_id)
print(f"Status of job {job_id}: {status}")