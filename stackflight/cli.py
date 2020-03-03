import logging
from multiprocessing import Process, Queue, current_process
from time import sleep

import click

from stackflight.launch import cfn_capabilities, _parse_template, _parse_parameters, _stack_name, cfn_api_params, \
    create_stack_worker, delete_stack_worker

log = logging.getLogger(__name__)  # pylint: disable=C0103
MAX_STACK_COUNT = 10


class Config:
    """
    singleton used by @pass_config
    """

    def __init__(self):
        self.verbose = False


# this builds @pass_config for us. Any function with @pass_config will
#   have config as first argument, singleton used to communicate between
#   functions.
pass_config = click.make_pass_decorator(Config, ensure=True)


@click.group()
@click.option('--verbose', is_flag=True)
@pass_config
def cli(config, verbose):
    config.verbose = verbose


@cli.command()
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
@pass_config
def launch(config, stack_name_prefix, stack_count, stack_file, stack_params_file, capability_iam, capability_named_iam,
           capability_auto_expand):
    """
    Utility to launch multiple stacks in parallel
    """
    if config.verbose:
        click.echo('verbose mode')
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


if __name__ == '__main__':
    cli()
