# aws-glue-etl-project
This repo contains python script or glue etl job
Please refer this blog for step by step guide - https://medium.com/@navin5556/mastering-aws-glue-etl-a-step-by-step-guide-to-loading-data-from-s3-to-redshift-4152be7e2922


## GitHub repo structure
```
aws-glue-etl-project/
|
|-- inputfile/
|   |-- product_data-2021.csv
|   |-- product_data.csv
|   └── product_data_2022.csv
|
|-- python_script/
    |-- MyGlueInsertRedshift.py
    |-- MyGlueJobReadFromS3.py
    |-- MyGlueReadFromS3-initial.py
    |-- MyGlueRedshiftInsert-initial.py
    └── lambda-code-to-trigger-etl.py
```



## s3 bucket structure
```
myglue-etl-project-sit/
|
|-- Input/
|   └── product/
|       |-- year=2021
|       └── year=2022
|-- Output/
|-- Scripts/
└── Temp/
```
