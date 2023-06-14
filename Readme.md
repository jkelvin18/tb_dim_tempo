# AWS Lambda Function: Time Dimension Generator

This AWS Lambda function generates a time dimension for a given year and month and saves it to an S3 location in a parquet format.

## Description

The lambda function uses the date reference given in the `event` input to generate time-based data for a month, which includes details for each hour of every day in that month. If no date is given, it defaults to generating data for the next month.

The function generates details such as hour, day, month, year, trimester and semester, for every hour in the given month and stores them in a pandas DataFrame. This DataFrame is then saved to an AWS S3 location in parquet format.

This AWS Lambda function is designed to run in a serverless environment and it is triggered based on the AWS Lambda service event source mappings.

## Requirements

- `boto3`
- `pandas`
- `awswrangler`
- `dateutil`
- `calendar`

## Function Details

The function is composed of several helper functions:

- `parse_event_date(event: dict) -> datetime`: Parses a date from the event data.

- `generate_month_data(year: int, month: int) -> list`: Generates data for the entire month.

- `save_to_s3(df: DataFrame, database_name: str, table_name: str, s3_location: str, boto3_session: Session) -> None`: Save a dataframe to an S3 location.

The main lambda function handler `lambda_handler(event: dict, context: object) -> None`, orchestrates the overall process, including parsing the event date, generating the month data, converting it to a DataFrame, and saving it to S3.

## Testing

To test this function locally, you need to set up AWS credentials in your environment and install the required Python packages mentioned above. You can then trigger the function with a dictionary as an event to simulate an AWS Lambda event trigger.

## Deployment

This function is intended to be deployed on AWS Lambda and can be added to your AWS environment using the AWS CLI or AWS Management Console. It also requires access to AWS S3 and AWS Glue services.

## Error Handling

The function has error handling in place for saving the data to the S3 bucket. If there's an error, it will be logged and an exception will be raised.