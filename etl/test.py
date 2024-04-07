
class Notification:
    def __init__(self, email=None, slack=None):
        self.email = email
        self.slack = slack

    def send_email(subject, body):
        # Implement your email sending logic here
        print(f"Email sent: Subject: {subject}, Body: {body}")

    def generate_html_table(self):
        html = "<table border='1'><tr><th>Error Count</th></tr>"
        html += f"<tr><td>{self.error_accumulator.value}</td></tr>"
        html += "</table>"
        return html


-------


import yaml

class AppConfiguration():

    def __init__(self, file_path):
        self.file_path = file_path

    def load_yaml(self):
        with open(self.file_path, 'r') as file:
            return yaml.safe_load(file)

    def display(self):
        print("Property:", self.prop)



------



import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark import AccumulatorParam
from conf.appconfiguration import AppConfiguration
from conf.notification import Notification


class Test(AppConfiguration, Notification):

    def __init__(self, file_path):
        AppConfiguration.__init__(self, file_path)
        yaml_data = self.load_yaml()
        Notification.__init__(self, email=yaml_data['notification']['email'], slack=yaml_data['notification']['slack'])
        self.test = yaml_data.get('test', None)


    def display_date(self):
        print("Date is:", self.date)

    def execute_pipeline(self):
        spark = SparkSession.builder \
            .appName("DataPipeline") \
            .getOrCreate()

        try:
            self.error_accumulator = spark.sparkContext.accumulator(0, ErrorAccumulator())

            # Execute queries to get datasets
            course_df = spark.sql(self.test['source']['course'])
            student_df = spark.sql(self.test['source']['student'])

            # Perform join operation
            joined_df = course_df.join(student_df, "id")

            # Count errors
            self.error_accumulator.add(1) if joined_df.isEmpty() else self.error_accumulator.add(0)

            # If the join is successful, proceed with further processing
            # For example:
            # joined_df.show()
        except Exception as e:
            # Send email notification in case of join failure
            self.send_email("Join failed", f"Join failed at {datetime.now()} with error: {str(e)}")
        finally:
            spark.stop()

# Example usage:
if __name__ == "__main__":
    file_path = "/resources/test.yaml"
    pipeline = Test(file_path)

    print("Email:", pipeline.email)
    print("Slack:", pipeline.slack)
    pipeline.execute_pipeline()
    html_table = pipeline.generate_html_table()
    print(html_table)

