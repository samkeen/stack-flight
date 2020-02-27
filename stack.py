import json
import logging
import multiprocessing
import sys
from datetime import datetime
from time import sleep

import boto3
from botocore.exceptions import ClientError

cf = boto3.client('cloudformation')  # pylint: disable=C0103
log = logging.getLogger('deploy.cf.create_or_update')  # pylint: disable=C0103



def main(stack_name_prefix, number_of_stacks, template, parameters):
    'Update or create stack'
    template_data = _parse_template(template)
    parameter_data = _parse_parameters(parameters)
    number_of_stacks = _parameter_num_stacks(number_of_stacks)

    jobs = []
    for i in range(number_of_stacks):
        sleep(2)
        p = multiprocessing.Process(target=_worker, args=(stack_name_prefix, template_data, parameter_data,))
        jobs.append(p)
        p.start()

    # _worker(stack_name_prefix, template_data, parameter_data)

def _worker(stack_name_prefix, template_data, parameter_data):
    try:
        stack_name = _stack_name(stack_name_prefix)
        params = {
            'StackName': stack_name,
            'TemplateBody': template_data,
            'Parameters': parameter_data,
            'Capabilities': ['CAPABILITY_IAM']
        }
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
        print(json.dumps(
            cf.describe_stacks(StackName=stack_result['StackId']),
            indent=2,
            default=json_serial
        ))

def _stack_name(stack_name_prefix):
    return f'{stack_name_prefix}-{datetime.now().strftime("%Y%m%d-%f")}'

def _parameter_num_stacks(number_of_stacks: str) -> int:
    parsed_int = int(number_of_stacks)
    if parsed_int < 1:
        _bail_out(f'unable to parse integer greater than one from commandline arg: "{number_of_stacks}"')
    if parsed_int > 10:
        _bail_out(f'Sorry max of 10 stacks for now, your tried: "{parsed_int}"')
    return parsed_int

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

def _usage():
    return 'Usage: python stack.py  stack-name-prefix  number-of-stacks  template-file  params-file'

def _bail_out(message):
    log.fatal(message)
    print(_usage())
    exit(1)

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError("Type not serializable")


if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) != 4:
        _bail_out(f'Expecting 4 commandline arguments, got: [{len(args)}]')
    main(*sys.argv[1:])
