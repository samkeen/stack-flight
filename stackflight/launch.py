import json
import logging
from datetime import datetime
from multiprocessing import Process, Queue, current_process
from typing import Dict

import boto3
from botocore.exceptions import ClientError

cf = boto3.client('cloudformation')  # pylint: disable=C0103
log = logging.getLogger(__name__)  # pylint: disable=C0103


def cfn_capabilities(capability_iam: str, capability_named_iam: str, capability_auto_expand: str) -> list:
    capabilities = []
    if capability_iam.upper() == 'Y':
        capabilities.append('CAPABILITIES_IAM')
    if capability_named_iam.upper() == 'Y':
        capabilities.append('CAPABILITIES_NAMED_IAM')
    if capability_auto_expand.upper() == 'Y':
        capabilities.append('CAPABILITIES_AUTO_EXPAND')
    return capabilities


def cfn_api_params(stack_name: str, template_data: str, parameter_data: str, capabilities: list) -> Dict[str, str]:
    return {
        'StackName': stack_name,
        'TemplateBody': template_data,
        'Parameters': parameter_data,
        'Capabilities': capabilities
    }


def create_stack_worker(stack_name, params, stack_create_results: Queue):
    log.debug(f'Starting process {current_process().name}')
    try:
        if _stack_exists(stack_name):
            print('Updating {}'.format(stack_name))
            stack_result = cf.update_stack(**params)
            waiter = cf.get_waiter('stack_update_complete')
        else:
            print('Creating {}'.format(stack_name))
            stack_result = cf.create_stack(**params)
            waiter = cf.get_waiter('stack_create_complete')
        print("...waiting for stack to be ready...")
        waiter.wait(StackName=stack_name)
    except ClientError as ex:
        error_message = ex.response['Error']['Message']
        if error_message == 'No updates are to be performed.':
            print("No changes")
        else:
            raise
    else:
        stack_create_results.put(json.dumps(
            cf.describe_stacks(StackName=stack_result['StackId']),
            indent=2,
            default=json_serial
        ))


def delete_stack_worker(stack_name, stack_delete_results: Queue):
    try:
        print(f'Deleting {stack_name}')
        cf.delete_stack(StackName=stack_name)
    except ClientError as ex:
        error_message = ex.response['Error']['Message']
        print(error_message)
    else:
        stack_delete_results.put(f'Process {current_process().name} issued delete stack for {stack_name}')


def _stack_name(stack_name_prefix):
    return f'{stack_name_prefix}-{datetime.now().strftime("%a-%H%M%S%f")}'


def _parse_template(template):
    with open(template) as template_fileobj:
        template_data = template_fileobj.read()
    cf.validate_template(TemplateBody=template_data)
    return template_data


def _parse_parameters(parameters):
    with open(parameters) as parameter_fileobj:
        parameter_data = json.load(parameter_fileobj)
    return parameter_data


def _stack_exists(stack_name):
    stacks = cf.list_stacks()['StackSummaries']
    for stack in stacks:
        if stack['StackStatus'] == 'DELETE_COMPLETE':
            continue
        if stack_name == stack['StackName']:
            return True
    return False


def _bail_out(message):
    log.fatal(message)
    exit(1)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError("Type not serializable")
