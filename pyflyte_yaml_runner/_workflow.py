import re
import rich_click as click
from itertools import chain
from pathlib import Path
from typing import List, Tuple
from ._pyflyte_yaml_runner_resolver import WorkflowConfig, resolver, TaskIOConfig

from flytekit import Workflow


def workflow_from_yaml(yaml_file) -> Tuple[WorkflowConfig, List[str]]:
    wf_config: WorkflowConfig = WorkflowConfig.from_yaml(yaml_file)

    imperative_wf = Workflow(name=wf_config.name)

    files_to_include: List[Path] = list(
        Path(file)
        for file in chain.from_iterable(task.files for task in wf_config.tasks)
    )

    for file in files_to_include:
        if not file.exists():
            raise click.ClickException(f"{file} does not exist!")

    name_to_outputs = {
        task.name: {o.name: o.type for o in task.outputs} for task in wf_config.tasks
    }

    # Infer inputs based on needs
    for task in wf_config.tasks:
        if not task.needs:
            continue

        needs = set(task.needs)
        parameters: List[str] = re.findall(r"\${{\s*(.*?)\s*}}", task.run)

        task_inputs = []
        for parameter in parameters:
            if parameter.startswith("outputs."):
                continue

            parameter_split = parameter.split(".")

            needed_task = parameter_split[0]
            if needed_task not in needs:
                raise click.ClickException(
                    f"Unable to find {needed_task} from {task.name}.needs"
                )

            assert parameter_split[1] == "outputs"
            parameter_name = parameter_split[2]

            task_input = TaskIOConfig(
                name=parameter_name,
                prefix=".".join(parameter_split[:2]),
                type=name_to_outputs[needed_task][parameter_name],
            )

            task_inputs.append(task_input)
        task.inputs = task_inputs

    task_entities = {}
    for task_config in wf_config.tasks:
        task = resolver.get_task(task_config)

        # has inputs
        inputs_kwargs = {}
        for task_input in task_config.inputs:
            needed_task_name = task_input.prefix.split(".")[0]
            needed_task_entity = task_entities[needed_task_name]
            inputs_kwargs[task_input.name] = needed_task_entity.outputs[task_input.name]

        task_entities[task_config.name] = imperative_wf.add_entity(
            task, **inputs_kwargs
        )

    return imperative_wf, files_to_include
