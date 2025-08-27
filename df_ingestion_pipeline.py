import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
import datetime


class ParseJobPosting(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode("utf-8"))
        # Add ingestion timestamp
        record["ingestion_time"] = datetime.datetime.utcnow().isoformat()
        return [record]


def run():
    options = PipelineOptions(
        streaming=True,
        save_main_session=True
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(topic="projects/your-project-id/topics/job-postings")
            | "Window into 60s" >> beam.WindowInto(beam.window.FixedWindows(60))
            | "Parse JSON" >> beam.ParDo(ParseJobPosting())
            | "Write to BQ" >> beam.io.WriteToBigQuery(
                table="your-project-id:labour_market.raw_job_posting",
                schema={
                    "fields": [
                        {"name": "job_title", "type": "STRING"},
                        {"name": "posted_date", "type": "TIMESTAMP"},
                        {"name": "salary_min", "type": "FLOAT"},
                        {"name": "salary_max", "type": "FLOAT"},
                        {"name": "company_name", "type": "STRING"},
                        {"name": "location", "type": "STRING"},
                        {"name": "description", "type": "STRING"},
                        {"name": "ingestion_time", "type": "TIMESTAMP"}
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )


if __name__ == "__main__":
    run()
