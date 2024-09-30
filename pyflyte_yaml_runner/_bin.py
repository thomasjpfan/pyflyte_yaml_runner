import rich_click as click
from flytekit.remote import FlyteRemote
from flytekit.configuration import Config

from ._register import register_script
from ._workflow import workflow_from_yaml


@click.command
@click.argument("yaml-file", type=click.File("r"))
def main(yaml_file):
    wf, files_to_include = workflow_from_yaml(yaml_file.read())

    remote = FlyteRemote(
        config=Config.for_sandbox(),
        default_domain="development",
        default_project="flytesnacks",
    )

    remote_wf = register_script(remote, wf, include_files=files_to_include)
    execute = remote.execute(remote_wf, inputs={})
    print(remote.generate_console_url(execute))
