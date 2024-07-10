# aws-glue-etl-project
this repo contains python script or glue etl job


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
