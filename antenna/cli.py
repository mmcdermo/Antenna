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

@cli.command(name='run-sources')
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.pass_context
def run_sources(ctx, aws_profile):
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
    controller.run_sources()
    print(json.dumps(list(map(lambda x: x.payload, items)), indent=4))

@cli.command(name='run-source-and-aggregate-transformer')
@click.argument('search_key')
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.pass_context
def run_source_and_aggregate_transformer(ctx, search_key, aws_profile):
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
    augmented = controller.augment_config_with_dynamodb_data(controller.config)
    for source in augmented['sources']:
        found = False
        for k in source:
            if search_key in str(source[k]).lower():
                found = True
                break
        if found:
            print("Found source: ")
            print(json.dumps(source, indent=4))
            items += controller.run_source_and_aggregate_transformer(source)

        print(len(list(map(lambda x: x.payload, items))))

@cli.command(name='backfill-source')
@click.argument('site_url')
@click.option('--local/--remote', default=True)
@click.option('--batch-size', default=10)
@click.option('--source-type', default="NewspaperLibSource")
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.pass_context
def backfill_source(ctx, site_url, local, batch_size, source_type, aws_profile):
    if ctx.obj['config_file'] not in os.listdir(ctx.obj['project_dir']):
        click.echo('No antenna_config.json file found in directory')
        raise click.Abort()

    config = {}
    with open(os.path.join(ctx.obj['project_dir'], ctx.obj['config_file']), 'r') as config_file:
        config = json.load(config_file)
    controller = Controller.Controller(config, os.getcwd(), aws_profile = aws_profile)
    items = []
    source = {
        "type": source_type,
        "url": site_url,
        "rss_feed_url": site_url,
        "item_type": "ArticleReference"
    }
    items += controller.run_source_and_aggregate_transformer(
        source, local=local, batch_size=batch_size)
    print(len(list(map(lambda x: x.payload, items))))

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

@cli.command(name='run-aggregate-controller')
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.pass_context
def run_aggregate_controller_local(ctx, aws_profile):
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
    controller.run_aggregate_controller_job(local=True)

@cli.command(name='backfill',
             help='Run a transformer across data stored in dynamodb'
)
@click.argument('dynamodb-table-name')
@click.argument('transformer-type')
@click.option('--index-name', default=None)
@click.option('--partition-key', default=None)
@click.option('--partition-key-value', default=None)
@click.option('--min-key', default=None)
@click.option('--min-value', default=None)

@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.option('--required-null-field', default=None,
              help='Only backfill over rows where this field is null')
@click.option('--limit', default=None,
              help='Maximum number of rows to backfill')
@click.pass_context
def backfill(ctx, aws_profile, dynamodb_table_name, transformer_type, index_name, partition_key, partition_key_value, min_key, min_value, required_null_field, limit):
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
                                  index_name=index_name,
                                  required_null_field=required_null_field,
                                  partition_key=partition_key,
                                  partition_key_value=partition_key_value,
                                  min_key=min_key,
                                  min_value=min_value,
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
    #controller.schedule_controller_lambda()

    controller.schedule_source_controller_lambda()
    if config["aggregate_transformer_only"]:
        print("Scheduling aggregate transformer lambda")
        controller.schedule_aggregate_controller_lambda()
    else:
        print("""
          Scheduling standard transformer lambda controller.
          For performance reasons, consider setting
          "aggregate_transformer_only": true in your config
        """)
        controller.schedule_transformer_controller_lambda()


def main():
    cli(obj={})
