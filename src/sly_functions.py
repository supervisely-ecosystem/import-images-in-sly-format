import functools
import os
import shutil
from typing import Callable

import supervisely as sly
from supervisely.io.fs import get_file_name_with_ext, silent_remove


def update_progress(count, api: sly.Api, progress: sly.Progress) -> None:
    count = min(count, progress.total - progress.current)
    progress.iters_done(count)
    if progress.need_report():
        progress.report_progress()


def get_progress_cb(
    api: sly.Api,
    message: str,
    total: int,
    is_size: bool = False,
    func: Callable = update_progress,
) -> functools.partial:
    progress = sly.Progress(message, total, is_size=is_size)
    progress_cb = functools.partial(func, api=api, progress=progress)
    progress_cb(0)
    return progress_cb


def download_data_from_team_files(api: sly.Api, save_path: str, context: sly.app.Import.Context) -> str:
    """Download data from remote directory in Team Files."""
    project_path = None
    IS_ON_AGENT = api.file.is_on_agent(context.path)
    if context.is_directory is True:
        if IS_ON_AGENT:
            agent_id, cur_files_path = api.file.parse_agent_id_and_path(context.path)
        else:
            cur_files_path = context.path
        remote_path = context.path
        project_path = os.path.join(
            save_path, os.path.basename(os.path.normpath(cur_files_path))
        )
        sizeb = api.file.get_directory_size(context.team_id, remote_path)
        progress_cb = get_progress_cb(
            api=api,
            message=f"Downloading {remote_path.lstrip('/').rstrip('/')}",
            total=sizeb,
            is_size=True,
        )
        api.file.download_directory(
            team_id=context.team_id,
            remote_path=remote_path,
            local_save_path=project_path,
            progress_cb=progress_cb,
        )

    elif context.is_directory is False:
        if IS_ON_AGENT:
            agent_id, cur_files_path = api.file.parse_agent_id_and_path(context.path)
        else:
            cur_files_path = context.path
        remote_path = context.path
        save_archive_path = os.path.join(save_path, get_file_name_with_ext(cur_files_path))
        sizeb = api.file.get_info_by_path(context.team_id, remote_path).sizeb
        progress_cb = get_progress_cb(
            api=api,
            message=f"Downloading {remote_path.lstrip('/')}",
            total=sizeb,
            is_size=True,
        )
        api.file.download(
            team_id=context.team_id,
            remote_path=remote_path,
            local_save_path=save_archive_path,
            progress_cb=progress_cb,
        )
        shutil.unpack_archive(save_archive_path, save_path)
        silent_remove(save_archive_path)
        if len(os.listdir(save_path)) > 1:
            sly.logger.error(
                "There must be only 1 project directory in the archive"
            )
            raise Exception("There must be only 1 project directory in the archive")

        project_name = os.listdir(save_path)[0]
        project_path = os.path.join(save_path, project_name)
    return project_path
