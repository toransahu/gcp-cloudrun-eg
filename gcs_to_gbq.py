import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery

logger = logging.getLogger(__name__)

GCP_PROJECT = "ansible-eg"
GCP_REGION = "us-west1"
GBQ_DATASET = "ansible_eg"


class Pipeline:
    def __init__(self, args=[]) -> None:
        self.pipeline_options = PipelineOptions(
            args,
            streaming=False,
        )

    def run(self) -> None:
        """Executes the beam pipeline DAG."""

        with beam.Pipeline(options=self.pipeline_options) as pipeline:
            self.definition(pipeline)

    def definition(self, pipeline: beam.Pipeline) -> None:

        (  # pyright: reportUnusedExpression=false
            pipeline
            | "FileURIsTablesMap"
            >> beam.Create(
                [
                    (
                        ["gs://my_data_files/sample.parquet"],
                        "sample",
                    )
                ]
            )
            | "LoadFileURIsToGBQ" >> beam.ParDo(LoadGCSFileURIsToBigQueryFn())
        )


class LoadGCSFileURIsToBigQueryFn(beam.DoFn):
    """Load GCS File URIs to Google BigQuery."""

    def setup(self):
        """Setup the Function."""

        self.gcp_project = GCP_PROJECT
        self.gcp_region = GCP_REGION
        self.dataset = GBQ_DATASET
        self.client = bigquery.Client(project=self.gcp_project, location=self.gcp_region)

    def teardown(self):
        self.client.close()
        return super().teardown()

    @property
    def dataset_fq(self):
        """Fully qualified BigQuery Dataset."""

        return f"{self.gcp_project}.{self.dataset}"

    def process(self, element):
        """Process the element.

        :param element: Element to process (list of tuple of file URIs & name of the
            BigQuery table)
        :return: Yields the same input element
        """

        file_uris = element[0]
        table_name = element[1]
        if not file_uris:
            logger.info(f"No File URIs to process, skip Load for {table_name}")
            yield element
            return
        if not table_name:
            logger.info(f"No table to process, skip Load for {table_name}")
            yield element
            return

        table_fq = f"{self.dataset_fq}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            # append to the table
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            # create table if does not exists
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            source_format=bigquery.SourceFormat.PARQUET,
        )
        try:
            load_job = self.client.load_table_from_uri(file_uris, table_fq, job_config=job_config)
            result = load_job.result()
            # result.add_done_callback(lambda e: e)
            took = result.ended - result.started
            logger.info(f"Loaded {result.output_rows} row(s) in {took.seconds} sec(s)")
        except Exception:
            logger.error(
                "LoadFileURIsToBigQueryFn failed.",
                exc_info=True,
            )
        yield element


if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.run()
