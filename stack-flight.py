import json
import logging
from datetime import datetime
from multiprocessing import Process, Queue, current_process
from time import sleep
from typing import Dict

import boto3
import click
from botocore.exceptions import ClientError

cf = boto3.client('cloudformation')  # pylint: disable=C0103
log = logging.getLogger('deploy.cf.create_or_update')  # pylint: disable=C0103
MAX_STACK_COUNT = 10


@click.command()
@click.option('--stack_count', '-c',
              prompt='Number of stacks to launch',
              default=1,
              type=click.IntRange(1, MAX_STACK_COUNT),
              help='Number of stacks to launch')
@click.option('--stack_name_prefix', '-n',
              prompt='stack name prefix',
              default='stack-flight',
              help='The prefix used for stack names. A unique string is appended to this prefix')
@click.option('--stack_file', '-t',
              prompt='path to stack file',
              default='./tests/fixtures/test.cfn.yaml',
              help='path to stack file')
@click.option('--stack_params_file', '-p',
              prompt='path to stack params file',
              default='./tests/fixtures/params.test.json',
              help='path to stack params file')
@click.option('--capability_iam',
              prompt='Add CAPABILITY_IAM?',
              type=click.Choice(['Y', 'N'], case_sensitive=False),
              default='N',
              help='if yes we declare CAPABILITY_IAM in the stack create call')
@click.option('--capability_named_iam',
              prompt='Add CAPABILITY_NAMED_IAM?',
              type=click.Choice(['Y', 'N'], case_sensitive=False),
              default='N',
              help='if yes we declare CAPABILITY_NAMED_IAM in the stack create call')
@click.option('--capability_auto_expand',
              prompt='Add CAPABILITY_AUTO_EXPAND?',
              type=click.Choice(['Y', 'N'], case_sensitive=False),
              default='N',
              help='if yes we declare CAPABILITY_AUTO_EXPAND in the stack create call')
def main(stack_name_prefix, stack_count, stack_file, stack_params_file, capability_iam, capability_named_iam,
         capability_auto_expand):
    capabilities = cfn_capabilities(capability_iam, capability_named_iam, capability_auto_expand)
    template_data = _parse_template(stack_file)
    parameter_data = _parse_parameters(stack_params_file)
    create_procs = []
    stack_create_results = Queue()
    stack_names = [_stack_name(stack_name_prefix) for i in range(stack_count)]
    for stack_name in stack_names:
        params = cfn_api_params(stack_name, template_data, parameter_data, capabilities)
        proc = Process(name=stack_name, target=create_stack_worker, args=(stack_name, params, stack_create_results))
        create_procs.append(proc)
        proc.start()
        sleep(1)
    for proc in create_procs:
        proc.join()

    log.info('all stacks created, now let\'s delete')

    delete_procs = []
    stack_delete_results = Queue()
    for stack_name in stack_names:
        proc = Process(name=stack_name, target=delete_stack_worker, args=(stack_name, stack_delete_results))
        delete_procs.append(proc)
        proc.start()
        sleep(1)
    for proc in delete_procs:
        proc.join()

    while not stack_create_results.empty():
        print(stack_create_results.get())
    while not stack_delete_results.empty():
        print(stack_delete_results.get())


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


if __name__ == '__main__':
    main()
