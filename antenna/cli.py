import os
import json
import os.path
import click
import antenna
import antenna.Controller as Controller
from antenna.DataMapper import DataMapper
import time
import shutil

v = "0.1.3"
@click.group()
@click.version_option(version=v, message='%(prog)s %(version)s')
@click.option('--debug/--no-debug', default=False,
              help='Write debug logs to standard error.')
@click.pass_context
def cli(ctx, debug=False):
    if not ctx.obj:
        ctx.obj = {}
    ctx.obj['config_file'] = 'antenna.json'
    ctx.obj['project_dir'] = os.getcwd()
    ctx.obj['debug'] = debug

@cli.command()
@click.pass_context
def init(ctx):
    srcdir = os.path.join("/", *(__file__.split("/")[:-2] + ["tests/test_data"]))

    shutil.copy2(os.path.join(srcdir, "antenna.json"),
                 os.path.join(os.getcwd(), "antenna.json"))
    shutil.copy2(os.path.join(srcdir, "transformers.py"),
                 os.path.join(os.getcwd(), "transformers.py"))

    print("Initialized project in directory %s" % os.getcwd())

@cli.command()
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.pass_context
def local(ctx, aws_profile):
    if ctx.obj['config_file'] not in os.listdir(ctx.obj['project_dir']):
        click.echo('No antenna_config.json file found in directory')
        raise click.Abort()

    config = {}
    with open(os.path.join(ctx.obj['project_dir'], ctx.obj['config_file']), 'r') as config_file:
        config = json.load(config_file)

    try:
        controller = Controller.Controller(config, os.getcwd(), aws_profile = aws_profile)
        #controller.create_resources()
    except Exception as e:
        click.echo('Error with config: %s' % e)
        raise click.Abort()

    items = []
    for source in controller.config['sources']:
        items += controller.run_source_job(source)

    transformers = []
    for transformer_conf in controller.config['transformers']:
        transformers.append(controller.instantiate_transformer(transformer_conf, os.getcwd()))

    produced = True
    while produced:
        produced = False
        produced_items = []
        for item in items:
            if item is not None:
                print(item.payload)
            for transformer in transformers:
                if item is not None and item.item_type in transformer.input_item_types:
                    print("Running transformer on item")
                    new_item = controller.run_transformer_job(transformer.params, item,
                                                              os.getcwd(), use_queues=False)
                    produced_items.append(new_item)
                    break
        if len(produced_items) > 0:
            produced = True
        items = produced_items

@cli.command(name='deploy-monitoring')
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.pass_context
def deploy_monitoring(ctx, aws_profile):
    if ctx.obj['config_file'] not in os.listdir(ctx.obj['project_dir']):
        click.echo('No antenna_config.json file found in directory')
        raise click.Abort()

    config = {}
    with open(os.path.join(ctx.obj['project_dir'], ctx.obj['config_file']), 'r') as config_file:
        config = json.load(config_file)

    try:
        controller = Controller.Controller(config, os.getcwd(), aws_profile = aws_profile)
        #controller.create_resources()
    except Exception as e:
        click.echo('Error with config: %s' % e)
        raise click.Abort()

    controller.create_resources()
    print("Sleeping to allow time for IAM role propagation")
    time.sleep(3)
    controller.deploy_monitoring(os.path.join(ctx.obj['project_dir']))
    print("Monitoring system deployed")

@cli.command(name='run-source')
@click.argument('search_key')
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.pass_context
def run_source(ctx, search_key, aws_profile):
    if ctx.obj['config_file'] not in os.listdir(ctx.obj['project_dir']):
        click.echo('No antenna_config.json file found in directory')
        raise click.Abort()

    config = {}
    with open(os.path.join(ctx.obj['project_dir'], ctx.obj['config_file']), 'r') as config_file:
        config = json.load(config_file)

    try:
        controller = Controller.Controller(config, os.getcwd(), aws_profile = aws_profile)
        #controller.create_resources()
    except Exception as e:
        click.echo('Error with config: %s' % e)
        raise click.Abort()

    items = []
    for source in controller.config['sources']:
        found = False
        for k in source:
            if search_key in str(source[k]).lower():
                found = True
                break
        if found:
            print("Found source: ")
            print(json.dumps(source, indent=4))
            items += controller.run_source_job(source)

    print(json.dumps(list(map(lambda x: x.payload, items)), indent=4))

@cli.command(name='run-controller')
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.pass_context
def run_controller(ctx, aws_profile):
    if ctx.obj['config_file'] not in os.listdir(ctx.obj['project_dir']):
        click.echo('No antenna_config.json file found in directory')
        raise click.Abort()

    config = {}
    with open(os.path.join(ctx.obj['project_dir'], ctx.obj['config_file']), 'r') as config_file:
        config = json.load(config_file)

    try:
        controller = Controller.Controller(config, os.getcwd(), aws_profile=aws_profile)
        controller.create_resources()
    except Exception as e:
        click.echo('Error with config: %s' % e)
        raise click.Abort()
    controller.run()

@cli.command(name='backfill',
             help='Run a transformer across data stored in dynamodb'
)
@click.argument('dynamodb-table-name')
@click.argument('transformer-type')
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.option('--required-null-field', default=None,
              help='Only backfill over rows where this field is null')
@click.option('--limit', default=None,
              help='Maximum number of rows to backfill')
@click.pass_context
def backfill(ctx, aws_profile, dynamodb_table_name, transformer_type, required_null_field, limit):
    if ctx.obj['config_file'] not in os.listdir(ctx.obj['project_dir']):
        click.echo('No antenna_config.json file found in directory')
        raise click.Abort()

    config = {}
    with open(os.path.join(ctx.obj['project_dir'], ctx.obj['config_file']), 'r') as config_file:
        config = json.load(config_file)

    try:
        controller = Controller.Controller(config, os.getcwd(), aws_profile=aws_profile)
        #controller.create_resources()
    except Exception as e:
        click.echo('Error with config: %s' % e)
        raise click.Abort()

    mapper = DataMapper(controller)

    print("Running backfill operation.")
    if limit is not None:
        limit = int(limit)

    stats = mapper.local_backfill(dynamodb_table_name,
                                  "backfill_item_" + dynamodb_table_name,
                                  transformer_type,
                                  required_null_field=required_null_field,
                                  limit=limit
    )
    print("Backfill operation complete.")
    print(json.dumps(stats, indent=4))

@cli.command(name='run-transformer')
@click.argument('transformer-type')
@click.argument('item-type')
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.pass_context
def run_transformer(ctx, aws_profile, transformer_type, item_type):
    if ctx.obj['config_file'] not in os.listdir(ctx.obj['project_dir']):
        click.echo('No antenna_config.json file found in directory')
        raise click.Abort()

    config = {}
    with open(os.path.join(ctx.obj['project_dir'], ctx.obj['config_file']), 'r') as config_file:
        config = json.load(config_file)

    try:
        config['local_jobs'] = True
        controller = Controller.Controller(config, os.getcwd(), aws_profile=aws_profile)
        #controller.create_resources()
    except Exception as e:
        click.echo('Error with config: %s' % e)
        raise click.Abort()

    # Note that this selection mechanism will be incorrect
    # if multiple transformers of the same type are present
    transformer_config = {}
    for conf in config['transformers']:
        if conf['type'] == transformer_type:
            transformer_config = conf
    controller.create_transformer_job(transformer_config, item_type, os.getcwd())

@cli.command()
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.option('--recreate-permissions', default=False,
              help='Recreate the permission resources')
@click.pass_context
def deploy(ctx, aws_profile, recreate_permissions):
    if ctx.obj['config_file'] not in os.listdir(ctx.obj['project_dir']):
        click.echo('No antenna_config.json file found in directory')
        raise click.Abort()

    config = {}
    with open(os.path.join(ctx.obj['project_dir'], ctx.obj['config_file']), 'r') as config_file:
        config = json.load(config_file)

    if True:
        controller = Controller.Controller(config, os.getcwd(), aws_profile=aws_profile)
    try:
        pass
    except Exception as e:
        click.echo('Error with config: %s' % e)
        raise click.Abort()

    # Create cluster if needed
    controller.create_resources()

    time.sleep(3)

    # Update Lambdas if needed
    controller.create_lambda_functions()

    # Schedule master lambda
    controller.schedule_controller_lambda()

def main():
    cli(obj={})
