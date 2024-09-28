import tarfile
import os
import tempfile
from pathlib import Path
from flytekit.remote import FlyteRemote
from flytekit import ImageSpec
from typing import Union, Optional, Dict, List
from flytekit.configuration import (
    ImageConfig,
    SerializationSettings,
    FastSerializationSettings,
)
from flytekit.core.python_auto_container import PythonAutoContainerTask
from flytekit.core.workflow import WorkflowBase
from flytekit.tools.translator import Options
from flytekit.remote.entities import FlyteWorkflow, FlyteTask
from flytekit.core.base_task import PythonTask
from flytekit.remote.remote import _get_git_repo_url

from . import _flyte_yaml_runner_resolver


def register_script(
    remote: FlyteRemote,
    entity: Union[WorkflowBase, PythonTask],
    image_config: Optional[ImageConfig] = None,
    version: Optional[str] = None,
    destination_dir: str = ".",
    default_launch_plan: bool = True,
    options: Optional[Options] = None,
    source_path: Optional[str] = None,
    include_files: Optional[List[Path]] = None,
    envs: Optional[Dict[str, str]] = None,
) -> Union[FlyteWorkflow, FlyteTask]:
    if image_config is None:
        image_config = ImageConfig.auto_default_image()

    if include_files is None:
        include_files = []

    resolver_file = _flyte_yaml_runner_resolver.__file__
    with tempfile.TemporaryDirectory() as tmp_dir:
        archive_fname = Path(os.path.join(tmp_dir, "script_mode.tar.gz"))
        with tarfile.open(archive_fname, "w:gz") as tar:
            tar.add(resolver_file, arcname=os.path.basename(resolver_file))

        # TODO: Add everything we need into script_mode.tar.gz
        md5_bytes, upload_native_url = remote.upload_file(
            archive_fname, remote.default_project, remote.default_domain
        )

    serialization_settings = SerializationSettings(
        project=remote.default_project,
        domain=remote.default_domain,
        image_config=image_config,
        git_repo=_get_git_repo_url(source_path),
        env=envs,
        fast_serialization_settings=FastSerializationSettings(
            enabled=True,
            destination_dir=destination_dir,
            distribution_location=upload_native_url,
        ),
        source_root=source_path,
    )
    if version is None:

        def _get_image_names(
            entity: Union[PythonAutoContainerTask, WorkflowBase],
        ) -> List[str]:
            if isinstance(entity, PythonAutoContainerTask) and isinstance(
                entity.container_image, ImageSpec
            ):
                return [entity.container_image.image_name()]
            if isinstance(entity, WorkflowBase):
                image_names = []
                for n in entity.nodes:
                    image_names.extend(_get_image_names(n.flyte_entity))
                return image_names
            return []

        default_inputs = None
        if isinstance(entity, WorkflowBase):
            default_inputs = entity.python_interface.default_inputs_as_kwargs

        # The md5 version that we send to S3/GCS has to match the file contents exactly,
        # but we don't have to use it when registering with the Flyte backend.
        # For that add the hash of the compilation settings to hash of file
        version = remote._version_from_hash(
            md5_bytes, serialization_settings, default_inputs, *_get_image_names(entity)
        )

    if isinstance(entity, PythonTask):
        return remote.register_task(entity, serialization_settings, version)
    fwf = remote.register_workflow(
        entity, serialization_settings, version, default_launch_plan, options
    )
    fwf._python_interface = entity.python_interface
    return fwf
