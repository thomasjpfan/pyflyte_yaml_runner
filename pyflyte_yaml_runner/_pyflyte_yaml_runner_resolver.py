import re
from flytekit import current_context
from textwrap import dedent
from dataclasses import dataclass
from subprocess import PIPE, Popen
from flytekit.types.directory import FlyteDirectory
from typing import List, Any
from flytekit.core.python_function_task import PythonInstanceTask
from flytekit.core.base_task import TaskResolverMixin
from flytekit.core.interface import Interface
from collections import OrderedDict
from flytekit.exceptions.user import FlyteUserException
import os
from flytekit import Resources
from dataclasses import field
from mashumaro.mixins.yaml import DataClassYAMLMixin
from mashumaro.mixins.json import DataClassJSONMixin
from enum import Enum


class ResourceConfig(Resources, DataClassYAMLMixin, DataClassJSONMixin):
    pass


class TaskIOTypeConfig(Enum):
    DIRECTORY = "directory"
    STRING = "string"

    def to_python_type(self):
        if self == TaskIOTypeConfig.DIRECTORY:
            return FlyteDirectory
        elif self == TaskIOTypeConfig.STRING:
            return str


@dataclass
class WorkflowInputConfig(DataClassYAMLMixin, DataClassJSONMixin):
    name: str
    type: TaskIOTypeConfig = TaskIOTypeConfig.STRING


@dataclass
class TaskIOConfig(DataClassYAMLMixin, DataClassJSONMixin):
    name: str
    type: TaskIOTypeConfig
    prefix: str = ""


@dataclass
class TaskConfig(DataClassYAMLMixin, DataClassJSONMixin):
    name: str
    run: str
    container_image: str
    resources: ResourceConfig
    files: List[str] = field(default_factory=list)
    needs: List[str] = field(default_factory=list)
    inputs: List[TaskIOConfig] = field(default_factory=list)
    outputs: List[TaskIOConfig] = field(default_factory=list)

    def __post_init__(self):
        filtered_run = self.run.replace(os.linesep, " ").strip()
        self.run = filtered_run


@dataclass
class WorkflowConfig(DataClassYAMLMixin, DataClassJSONMixin):
    name: str
    tasks: List[TaskConfig]
    workflow_inputs: List[WorkflowInputConfig] = field(default_factory=list)


@dataclass
class ProcessResult:
    returncode: int
    output: str
    error: str


class YamlTask(PythonInstanceTask[TaskConfig]):
    def __init__(
        self,
        name: str,
        command: str,
        task_config: TaskConfig,
        **kwargs,
    ):
        self.command = command

        task_inputs = OrderedDict()
        for task_input in task_config.inputs:
            task_inputs[task_input.name] = task_input.type.to_python_type()

        outputs = OrderedDict()
        outputs["command"] = str

        for output in task_config.outputs:
            outputs[output.name] = output.type.to_python_type()

        super().__init__(
            name,
            task_config,
            task_type="yaml-task",
            interface=Interface(inputs=task_inputs, outputs=outputs),
            **kwargs,
        )

    def execute(self, **kwargs) -> Any:
        ctx = current_context()
        working_dir = ctx.working_directory

        variables = {}
        for task_input in self.task_config.inputs:
            if task_input.type == TaskIOTypeConfig.DIRECTORY:
                local_dir = kwargs[task_input.name].download()
                variables[f"{task_input.prefix}.{task_input.name}"] = local_dir
            elif task_input.type == TaskIOTypeConfig.STRING:
                variables[f"{task_input.prefix}.{task_input.name}"] = kwargs[
                    task_input.name
                ]

        outputs = []
        for output in self.task_config.outputs:
            if output.type == TaskIOTypeConfig.DIRECTORY:
                new_folder = os.path.join(working_dir, output.name)
                os.mkdir(new_folder)
                variables[f"outputs.{output.name}"] = new_folder

                outputs.append(new_folder)

        command = self._replace_command_variables(self.command, variables)

        try:
            result = _run_command(command)
        except Exception as e:
            msg = dedent(f"Failed to execute, command='{command}'")
            raise FlyteUserException(msg) from e

        if result.returncode != 0:
            error = dedent(f"""\
                Failed to execute script,
                command='{command}'
                return-code={result.returncode}
                std-out:
                {result.output}
                std-error:
                {result.error}\
            """)
            raise FlyteUserException(error)

        if outputs:
            return [self.command] + outputs
        return self.command

    def _replace_command_variables(self, command: str, variables: dict) -> str:
        def replace_placeholder(match):
            variable_name = match.group(1)
            return variables.get(variable_name, match.group(0))

        return re.sub(r"\${{\s*(.*?)\s*}}", replace_placeholder, command)


class YamlTaskResolver(TaskResolverMixin):
    @property
    def location(self) -> str:
        return "_pyflyte_yaml_runner_resolver.resolver"

    @property
    def name(self) -> str:
        return "_pyflyte_yaml_runner_resolver"

    def get_task(self, task_config: TaskConfig) -> PythonInstanceTask:
        # Update interface based on task_config.outputs
        return YamlTask(
            name=task_config.name,
            task_resolver=self,
            command=task_config.run,
            resources=task_config.resources,
            container_image=task_config.container_image,
            task_config=task_config,
        )

    def load_task(self, loader_args: List[str]) -> PythonInstanceTask:
        _, task_name, command, task_json = loader_args

        task_config = TaskConfig.from_json(task_json)

        return YamlTask(
            name=task_name,
            task_resolver=self,
            command=command,
            task_config=task_config,
        )

    def loader_args(self, settings, task):
        return [
            "yaml-task",
            task.name,
            task.command,
            task.task_config.to_json(),
        ]


resolver = YamlTaskResolver()


def _run_command(command: List[str]) -> ProcessResult:
    process = Popen(
        command,
        stdout=PIPE,
        stderr=PIPE,
        text=True,
        shell=True,
    )

    process_stdout, process_stderr = process.communicate()
    out = ""
    for line in process_stdout.splitlines():
        print(line)
        out += line

    code = process.wait()
    return ProcessResult(code, out, process_stderr)
