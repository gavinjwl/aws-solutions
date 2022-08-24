# Low-Code Transactional Data Lake on AWS

## Prerequisite

### Subscribe Apache Hudi Connector for AWS Glue from AWS marketplace

- Navigate to AWS Marketplace from AWS Glue Studio Console

    ![Navigate to AWS Marketplace from AWS Glue Studio Console](.github/subscribe-apache-hudi-connector-for-aws-glue-from-aws-marketplace/1.png)

- Search Apache Hudi Connector for AWS Glue

    ![Search Apache Hudi Connector for AWS Glue](.github/subscribe-apache-hudi-connector-for-aws-glue-from-aws-marketplace/2.png)

- Start to subscribe Apache Hudi Connector for AWS Glue

    ![Start to subscribe Apache Hudi Connector for AWS Glue](.github/subscribe-apache-hudi-connector-for-aws-glue-from-aws-marketplace/3.png)

- Accept EULA and continue

    ![Accept EULA and continue](.github/subscribe-apache-hudi-connector-for-aws-glue-from-aws-marketplace/4.png)

- Configure Glue and Hudi version

    ![Configure Glue and Hudi version](.github/subscribe-apache-hudi-connector-for-aws-glue-from-aws-marketplace/5.png)

- Open instruction

    ![Open instruction](.github/subscribe-apache-hudi-connector-for-aws-glue-from-aws-marketplace/6.png)

- Activate the Glue connector from AWS Glue Studio

    ![Activate the Glue connector from AWS Glue Studio](.github/subscribe-apache-hudi-connector-for-aws-glue-from-aws-marketplace/7.png)

- Setup the connector name to `hudi-connector` and click __Create Connection and activate connector__

    ![Setup the connector name to `hudi-connector` and click __Create Connection and activate connector__](.github/subscribe-apache-hudi-connector-for-aws-glue-from-aws-marketplace/8.png)

- Check final result

    ![Check final result](.github/subscribe-apache-hudi-connector-for-aws-glue-from-aws-marketplace/9.png)

## Deployment

Before starting, make sure your Python verion >= 3.8 and installed [Poetry](https://python-poetry.org/) ([How to install](https://python-poetry.org/docs/#installation))

```bash
git clone https://github.com/gavinjwl/aws-solutions.git

cd lowcode-transactional-datalake

poetry install

source .venv/bin/activate

cdk deploy
```

## Known issues

- Lakeformation integration

    After CDK deployed, you need to grant database/table read/write permissions to Glue role which CDK created
