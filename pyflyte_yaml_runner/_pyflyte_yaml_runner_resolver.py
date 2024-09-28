from typing import List, TypeVar, Any
from flytekit import PythonFunctionTask
from flytekit.core.python_function_task import PythonInstanceTask
from flytekit.core.base_task import TaskResolverMixin
from flytekit.core.interface import Interface
from flytekit import kwtypes

T = TypeVar("T")


class YamlTask(PythonInstanceTask[T]):
    def __init__(self, name: str, task_config: T = None, **kwargs):
        super().__init__(
            name,
            task_config,
            task_type="yaml-task",
            interface=Interface(outputs=kwtypes(o0=str)),
            **kwargs,
        )

    def execute(self, **kwargs) -> Any:
        return "hello world"


class YamlTaskResolver(TaskResolverMixin):
    @property
    def location(self) -> str:
        return "_flyte_yaml_runner_resolver.resolver"

    @property
    def name(self) -> str:
        return "_flyte_yaml_runner_resolver"

    def get_task(self) -> PythonFunctionTask:
        return YamlTask(name="yaml-task", task_resolver=self)

    def load_task(self, loader_args: List[str]) -> PythonFunctionTask:
        return YamlTask(name="yaml-task", task_resolver=self)

    def loader_args(self, settings, task):
        return ["yaml-task"]


resolver = YamlTaskResolver()
