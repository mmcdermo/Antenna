import os
import json
import os.path
import click
import antenna

@click.group()
@click.version_option(version=SignalAntenna.__version__, message='%(prog)s %(version)s')
@click.option('--debug/--no-debug', default=False,
              help='Write debug logs to standard error.')
@click.pass_context
def cli(ctx, debug=False):
    if not ctx.obj:
        ctx.obj = {}
    ctx.obj['config_file'] = 'antenna.json'
    ctx.obj['project_dir'] = os.getcwd()
    ctx.obj['debug'] = debug

@cli.group('controller')
@click.pass_context
def controller(ctx):
    if not ctx.obj:
        ctx.obj = {}

@controller.command()
@click.option('--aws-profile', default=None,
              help='AWS Profile to use for cluster commands')
@click.pass_context
def launch(ctx, aws_profile):
    if ctx.obj['config_file'] not in os.listdir(ctx.obj['project_dir']):
        click.echo('No antenna_config.json file found in directory')
        raise click.Abort()

    config = json.loads(os.path.join(ctx.obj['project_dir'], ctx.obj['config_file']))
    try:
        controller = SignalAntenna.Controller(config)
    except Exception as e:
        click.echo('Error with config: %s' % e)
        raise click.Abort()
    controller.launch()

def main():
    cli(obj={})
