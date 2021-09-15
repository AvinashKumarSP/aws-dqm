import boto3
from pprint import pprint

from botocore.exceptions import ClientError


def create_rules_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.create_table(
        TableName='dq_rules',
        KeySchema=[
            {
                'AttributeName': 'source',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'entity',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'source',
                'AttributeType': 'S'  # Partition key
            },
            {
                'AttributeName': 'entity',
                'AttributeType': 'S'  # Sort key
            }

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table


def put_rule(source, entity, cde, kde, rules_run_on, single_column_profiler_rules, single_column_validation_rules, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('dq_rules')
    response = table.put_item(
        Item={
            'source': source,
            'entity': entity,
            'cde': cde,
            'kde': kde,
            'rules_run_on': rules_run_on,
            'single_column_profiler_rules': single_column_profiler_rules,
            'single_column_validation_rules': single_column_validation_rules
        }
    )
    return response


def put_rule_updated(tbl_name, item_dict, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('dq_rules')
    response = table.put_item(
        Item=item_dict
    )
    return response


def get_rule(source, entity, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('dq_rules')

    try:
        response = table.get_item(Key={'source': source, 'entity': entity})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']


def update_rule(source, entity, single_column_profiler_rules, single_column_validation_rules, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('dq_rules')

    response = table.update_item(
        Key={
            'source': source,
            'entity': entity
        },
        UpdateExpression="set rules_run_on=:r, single_column_profiler_rules=:p, single_column_validation_rules=:a",
        ExpressionAttributeValues={
            ':r': ["cde", "kde"],
            ':a': single_column_validation_rules,
            ':p': single_column_profiler_rules
        },
        ReturnValues="UPDATED_NEW"
    )
    return response

item1={

    "source": "nhs",
    "entity": "patient",
    "cde": [],
    "kde": [],
    "rules_run_on": [],
    "single_column_profiler_rules": "",
    "single_column_validation_rules" : {
        "patient_id": [
            {
                "rule_specification": "patient_id has null",
                "rule_name": "patient_id_isnull",
                "rule": "patient_id is not null",
                "is_active": 1
            }
        ],
        "country": [
            {
                "rule_specification": "Country has ['United States','United Kingdom']",
                "rule_name": "country_has",
                "rule": "country in ('United States','United Kingdom')",
                "is_active": 1
            },
            {
                "rule_specification": "Country has ['United States','United Kingdom']",
                "rule_name": "country_has",
                "rule": "country is not null",
                "is_active": 0
            }
        ]
    }
}

if __name__ == '__main__':
    # rules_table = create_rules_table()
    # print("Table status:", rules_table.table_status)
    rule = {
        "patient_id": [
            {
                "rule_specification": "patient_id has null",
                "rule_name": "patient_id_isnull",
                "rule": "patient_id is not null",
                "is_active": 1
            }
        ],
        "country": [
            {
                "rule_specification": "Country has ['United States','United Kingdom']",
                "rule_name": "country_has",
                "rule": "country in ('United States','United Kingdom')",
                "is_active": 1
            },
            {
                "rule_specification": "Country has ['United States','United Kingdom']",
                "rule_name": "country_has",
                "rule": "country is not null",
                "is_active": 0
            }
        ]
    }
    '''
    rule_resp = put_rule("nhs", "patient",
                         ["patient_id", "age"], ["country", "state"],
                         ["cde", "kde"], rule)
    pprint(rule_resp, sort_dicts=False)
    update_rule("nhs", "patient", rule)'''
    rule_res = get_rule("nhs", "patient")
    if rule_res:
        print("Get movie succeeded:")
        pprint(rule_res, sort_dicts=False)
