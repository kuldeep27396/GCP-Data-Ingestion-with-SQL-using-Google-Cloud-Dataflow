# -*- coding: utf-8 -*-


"""An Apache Beam batch pipeline example.
It reads JSON encoded messages from GCS file, transforms the message data and
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
REVIEW_SCHEMA = ','.join([
    'review_id:STRING',
    'user_id:STRING',
    'business_id:STRING',
    'stars:INT64',
    'date:STRING',
    'text:STRING',
    'useful:INT64',
    'funny:INT64',
    'cool:INT64',
])

ERROR_SCHEMA = ','.join([
    'error:STRING',
])

review_schema = {
    'fields': [{
        'name': 'review_id', 'type': 'STRING', 'mode': 'REQUIRED'
    }, {
        'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'
    },
    {
        'name': 'business_id', 'type': 'STRING', 'mode': 'REQUIRED'
    }, {
        'name': 'stars', 'type': 'FLOAT64', 'mode': 'NULLABLE'
    },
     {
        'name': 'date', 'type': 'STRING', 'mode': 'NULLABLE'
    }, {
        'name': 'text', 'type': 'STRING', 'mode': 'NULLABLE'
    },
    {
        'name': 'useful', 'type': 'INT64', 'mode': 'NULLABLE'
    }, {
        'name': 'funny', 'type': 'INT64', 'mode': 'NULLABLE'
    },
     {
        'name': 'cool', 'type': 'INT64', 'mode': 'NULLABLE'
    }]
}

business_schema1 = {
    'fields': [{
        'name': 'business_id', 'type': 'STRING', 'mode': 'REQUIRED'
    }]}

business_schema = {
    'fields': [{
        'name': 'business_id', 'type': 'STRING', 'mode': 'REQUIRED'
    }, {
        'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'
    },
    {
        'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'
    }, {
        'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'
    },
     {
        'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'
    }, {
        'name': 'postal_code', 'type': 'STRING', 'mode': 'NULLABLE'
    },
    {
        'name': 'latitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'
    }, {
        'name': 'longitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'
    },
     {
        'name': 'stars', 'type': 'FLOAT64', 'mode': 'NULLABLE'
    },
    {
        'name': 'review_count', 'type': 'INT64', 'mode': 'NULLABLE'
    },
     {
        'name': 'is_open', 'type': 'INT64', 'mode': 'NULLABLE'
    },
    {
        'name': 'categories', 'type': 'STRING', 'mode': 'NULLABLE'
    }
     ,{
        'name': 'hours', 'type': 'RECORD', 'mode': 'NULLABLE',"fields": [
            {
                "name": "Monday",
                "type": "STRING"
            },
            {
                "name": "Tuesday",
                "type": "STRING"
            },
            {
                "name": "Friday",
                "type": "STRING"
           },
            {
                "name": "Wednesday",
                "type": "STRING"
            },
            {
               "name": "Thursday",
                "type": "STRING"
           },
           {
               "name": "Sunday",
                "type": "STRING"
            },
            {
                "name": "Saturday",
                "type": "STRING"
            }
        ]
    }
    ]
}


class ParseMessage(beam.DoFn):
    OUTPUT_BUSINESS_TAG = 'business' 
    OUTPUT_ERROR_TAG = 'error'
    def process(self, element,table):
        """
        Extracts fields from json message
        :param element: file metadata message returned from reading files at input path.
        :return: have two outputs:
            - reviews: parsed review data
            - business: parsed business data
        """
        try:
            line = json.loads(element)
            print(line)
            logging.info(line)
            if 'review' in table:
                review = {
                        'review_id': line['review_id'],
                        'user_id': line['user_id'],
                        'business_id': line['business_id'],
                        'stars': line['stars'],
                        'date': line['date'],
                        'text': line['text'],
                        'useful': line['useful'],
                        'funny': line['funny'],
                        'cool': line['cool']
                        }
                print(review)
                logging.info(review)
                
                yield review
            else:
                business =  {
                        'business_id': line['business_id'],
                        'name': line['name'] ,
                        'address': line['address'] ,
                        'city': line['city'] ,
                        'state': line['state'] ,
                        'postal_code': line['postal_code'] ,
                        'latitude': line['latitude'],
                        'longitude': line['longitude'],
                        'stars': line['stars'],
                        'review_count': line['review_count'],
                        'is_open': line['is_open'],
                        'categories': line['categories'],
                        'hours': line['hours']
                        }
                print(business)
                logging.info(business)
                #yield beam.pvalue.TaggedOutput(self.OUTPUT_BUSINESS_TAG, business)
                yield business
        except Exception as error:
            print("error")
            logging.info(str(error))
            error_row = { 'error': str(error) }
            yield beam.pvalue.TaggedOutput(self.OUTPUT_ERROR_TAG, error_row)

class DataflowOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
        '--input_path', required=True,
        help='Input GCS path from where files will be read.')
        parser.add_argument(
            '--table', required=True,
            help='Output BigQuery table for file specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.')
        parser.add_argument(
            '--error_table', required=True,
            help='Output BigQuery table for error as: PROJECT:DATASET.TABLE or DATASET.TABLE.')
    '''@classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
        '--input_path', type=str,default='gs://gcp-file-source/data1/*',
        help='Input GCS path from where files will be read.')
        parser.add_value_provider_argument(
            '--table', type=str,default='test.business',
            help='Output BigQuery table for file specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.')
        parser.add_value_provider_argument(
            '--error_table', type=str,default='test.error',
            help='Output BigQuery table for error as: PROJECT:DATASET.TABLE or DATASET.TABLE.')
        '''
def run(argv=None):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser(argv)    
    known_args, pipeline_args = parser.parse_known_args()
    options = PipelineOptions(pipeline_args, save_main_session=True)
    dataflow_options = options.view_as(DataflowOptions)
    with beam.Pipeline(options=options) as pipeline: 
        rows, error = (
            pipeline
            | beam.io.ReadFromText(dataflow_options.input_path)
            | 'Parse JSON messages' >> beam.ParDo(ParseMessage(),dataflow_options.table).with_outputs(ParseMessage.OUTPUT_ERROR_TAG,
                                                                                main='rows')
             )
           

        # Output the results into BigQuery table.
        _ = (rows | 'Write rows to BigQuery'
             >> beam.io.WriteToBigQuery(table=dataflow_options.table,
                                        schema=lambda table: (business_schema
                                                              if 'business' in table
                                                              else review_schema),
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        #insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
                                        )
             )

        _ = (error | 'Write error to BigQuery'
             >> beam.io.WriteToBigQuery(table=dataflow_options.error_table,
                                        schema=ERROR_SCHEMA,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        #insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
                                        )
             )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)   
    run()



