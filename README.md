# ETL Pipeline using Airflow, AWS EMR and Pyspark

This is an simple airflow DAG which process the data from s3 by creating EMR clusters and terminates after processing.
You can find the dataset from [Kaggle](https://www.kaggle.com/datasets/sangamsharmait/ecommerce-orders-data-analysis)

### AWS setup on Airflow Dashboard
In Airflow dashboard, you need setup connection with AWS. Go to Admin > connections > Add connection
Now pass 

| Key                     | Value |
| -------------           |:-------------:         |
| Connection ID           | aws_default            |
| Connection Type         | Amazon Web Services    |
| AWS Access Key ID       | <your_aws_access_key>  |
| AWS Secret Access Key   | <your_aws_secret_key>  |

In Extra Fields pass this json 
{
  "region_name": "us-west-2"
}
