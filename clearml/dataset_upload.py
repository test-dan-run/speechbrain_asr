from clearml import Dataset

dataset = Dataset.create(dataset_name='cv-corpus-7.0-2021-07-21-en-raw', dataset_project='datasets/commonvoice')
dataset.add_files('cv-corpus-7.0-2021-07-21/en')
dataset.upload(output_url='s3://public-data/')
dataset.finalize()
dataset.publish()