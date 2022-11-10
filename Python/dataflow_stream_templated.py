# -*- coding: utf-8 -*-

"""An Apache Beam streaming pipeline example.
It reads JSON encoded messages from Pub/Sub, transforms the message data and
writes the results to BigQuery.
"""

import argparse
import json
import logging
#import time
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
#from apache_beam.options.pipeline_options import SetupOptions
#import apache_beam.transforms.window as window

# Defines the BigQuery schema for the output table.
SCHEMA = ','.join([
    'business_id:STRING',
    'highlights:STRING',
    'delivery_or_takeout:STRING',
    'Grubhub_enabled:STRING',
    'Call_To_Action_enabled:STRING',
    'Request_a_Quote_Enabled:STRING',
    'Covid_Banner:STRING',
    'Temporary_Closed_Until:STRING',
    'Virtual_Services_Offered:STRING',
])

ERROR_SCHEMA = ','.join([
    'error:STRING',
])


class ParseMessage(beam.DoFn):
    OUTPUT_ERROR_TAG = 'error'    
    def process(self, line):
        """
        Extracts fields from json message
        :param line: pubsub message
        :return: have two outputs:
            - main: parsed data
            - error: error message
        """
        try:
            parsed_row = json.loads(line) # parse json message to corresponding bgiquery table schema
            print("Running")
            logging.info("Running")
            yield {
                 'business_id': parsed_row['business_id'],
                 'highlights': parsed_row['highlights'],
                 'delivery_or_takeout': parsed_row['delivery or takeout'],
                 'Grubhub_enabled': parsed_row['Grubhub enabled'],
                 'Call_To_Action_enabled': parsed_row['Call To Action enabled'],
                 'Request_a_Quote_Enabled': parsed_row['Request a Quote Enabled'],
                 'Covid_Banner': parsed_row['Covid Banner'],
                 'Temporary_Closed_Until': parsed_row['Temporary Closed Until'],
                 'Virtual_Services_Offered': parsed_row['Virtual Services Offered']
                 }
        except Exception as error:
            print("error")
            logging.info("error")
            error_row = { 'error': str(error) }
            yield beam.pvalue.TaggedOutput(self.OUTPUT_ERROR_TAG, error_row)

class DataflowOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
        '--input_subscription',default="projects/fabric-1333/subscriptions/gcp-yelp-topic-sub",
        help='Input PubSub subscription of the form "/subscriptions/<PROJECT>/<SUBSCRIPTION>".')
        parser.add_argument(
            '--output_table', type=str,default='test.yelp_covid',
            help='Output BigQuery table for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.')
        parser.add_argument(
            '--output_error_table', type=str,default='test.error',
            help='Output BigQuery table for errors specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.')

def run(argv=None):
    """Build and run the pipeline."""
    #options = PipelineOptions(args, save_main_session=True, streaming=True)
    parser = argparse.ArgumentParser(argv)    
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
    dataflow_options = options.view_as(DataflowOptions)
    
    with beam.Pipeline(options=options) as pipeline:
        # Read the messages from PubSub and process them.
        rows, error_rows = (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
                subscription=str(dataflow_options.input_subscription)).with_output_types(bytes)
            | 'UTF-8 bytes to string' >> beam.Map(lambda msg: msg.decode('utf-8'))
            | 'Parse JSON messages' >> beam.ParDo(ParseMessage()).with_outputs(ParseMessage.OUTPUT_ERROR_TAG,
                                                                                main='rows')
             )
        # Output the results into BigQuery table.
        _ = (rows | 'Write to BigQuery'
             >> beam.io.WriteToBigQuery(table=dataflow_options.output_table,
                                        schema=SCHEMA,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
                                        )
             )

        _ = (error_rows | 'Write errors to BigQuery'
             >> beam.io.WriteToBigQuery(table=dataflow_options.output_error_table,
                                        schema=ERROR_SCHEMA,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
                                        )
             )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    #known_args, pipeline_args = parser.parse_known_args()
    run()

