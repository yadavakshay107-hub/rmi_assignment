import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from rmi.ml.model import predict_salary_of_job
import pandas as pd


class PredictSalary(beam.DoFn):
    def process(self, element):
        job_title = element["job_title"]
        month = element["month"]
        df = pd.DataFrame([element])
        prediction = predict_salary_of_job(df)
        yield {
            "job_title": job_title,
            "month": month,
            "predicted_salary": float(prediction)
        }


def run():
    options = PipelineOptions(
        save_main_session=True,
        streaming=False
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from BQ" >> beam.io.ReadFromBigQuery(
                query="SELECT job_title, EXTRACT(MONTH FROM posted_date) as month, salary_min, salary_max, min_salary FROM labour_market.preprocessed_job_posting",
                use_standard_sql=True
            )
            | "Predict Salary" >> beam.ParDo(PredictSalary())
            | "Write to BQ" >> beam.io.WriteToBigQuery(
                table="labour_market.predicted_salary_per_month",
                schema="job_title:STRING, month:INTEGER, predicted_salary:FLOAT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )


if __name__ == "__main__":
    run()
