import rich_click as click
from flytekit import Workflow
from flytekit.remote import FlyteRemote
from flytekit.configuration import Config

from ._flyte_yaml_runner_resolver import resolver
from ._run import register_script


@click.command
def main():
    task = resolver.get_task()

    imperative_wf = Workflow(name="my_workflow")
    node = imperative_wf.add_entity(task)
    imperative_wf.add_workflow_output("wf_output", node.outputs["o0"], python_type=str)

    remote = FlyteRemote(
        config=Config.for_sandbox(),
        default_domain="development",
        default_project="flytesnacks",
    )

    remote_wf = register_script(remote, imperative_wf)
    remote.execute(remote_wf, inputs={})
