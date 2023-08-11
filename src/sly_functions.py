import functools
import os
import shutil
from typing import Callable

import supervisely as sly
from supervisely.io.fs import get_file_name_with_ext, silent_remove, get_file_name

import sly_globals as g


def update_progress(count, api: sly.Api, task_id: int, progress: sly.Progress) -> None:
    count = min(count, progress.total - progress.current)
    progress.iters_done(count)
    if progress.need_report():
        progress.report_progress()


def get_progress_cb(
    api: sly.Api,
    task_id: int,
    message: str,
    total: int,
    is_size: bool = False,
    func: Callable = update_progress,
) -> functools.partial:
    progress = sly.Progress(message, total, is_size=is_size)
    progress_cb = functools.partial(func, api=api, task_id=task_id, progress=progress)
    progress_cb(0)
    return progress_cb


def download_data_from_team_files(api: sly.Api, task_id: int, save_path: str) -> str:
    """Download data from remote directory in Team Files."""
    if g.INPUT_DIR:
        if sly.fs.get_file_ext(g.INPUT_DIR) in [".zip", ".tar"]:
            sly.logger.info("Folder mode is selected, but archive file is uploaded.")
            sly.logger.info("Switching to file mode.")
            g.INPUT_DIR, g.INPUT_FILE = None, g.INPUT_DIR
    elif g.INPUT_FILE:
        if sly.fs.get_file_ext(g.INPUT_FILE) not in [".zip", ".tar"]:
            parent_dir, _ = os.path.split(g.INPUT_FILE)
            if os.path.basename(parent_dir) in ["img", "ann"]:
                parent_dir = os.path.dirname(os.path.dirname(parent_dir))
            if not parent_dir.endswith("/"):
                parent_dir += "/"
            sly.logger.info("File mode is selected, but archive file is not uploaded.")
            sly.logger.info("Switching to folder mode.")
            g.INPUT_DIR, g.INPUT_FILE = parent_dir, None

    if g.INPUT_DIR is not None:
        sly.logger.debug(f"The app is working with directory {g.INPUT_DIR}.")

        if g.IS_ON_AGENT:
            agent_id, cur_files_path = api.file.parse_agent_id_and_path(g.INPUT_DIR)
        else:
            cur_files_path = g.INPUT_DIR
        remote_path = g.INPUT_DIR
        input_path = os.path.join(
            save_path, os.path.basename(os.path.normpath(cur_files_path))
        )
        sizeb = api.file.get_directory_size(g.TEAM_ID, remote_path)
        progress_cb = get_progress_cb(
            api=api,
            task_id=task_id,
            message=f"Downloading {remote_path.lstrip('/').rstrip('/')}",
            total=sizeb,
            is_size=True,
        )
        api.file.download_directory(
            team_id=g.TEAM_ID,
            remote_path=remote_path,
            local_save_path=input_path,
            progress_cb=progress_cb,
        )

    elif g.INPUT_FILE is not None:
        sly.logger.debug(f"The app is working with file {g.INPUT_FILE}.")

        if g.IS_ON_AGENT:
            agent_id, cur_files_path = api.file.parse_agent_id_and_path(g.INPUT_FILE)
        else:
            cur_files_path = g.INPUT_FILE
        remote_path = g.INPUT_FILE

        save_archive_path = os.path.join(
            save_path, get_file_name_with_ext(cur_files_path)
        )
        sizeb = api.file.get_info_by_path(g.TEAM_ID, remote_path).sizeb
        progress_cb = get_progress_cb(
            api=api,
            task_id=task_id,
            message=f"Downloading {remote_path.lstrip('/')}",
            total=sizeb,
            is_size=True,
        )
        api.file.download(
            team_id=g.TEAM_ID,
            remote_path=remote_path,
            local_save_path=save_archive_path,
            progress_cb=progress_cb,
        )

        input_path = os.path.join(save_path, get_file_name(cur_files_path))
        shutil.unpack_archive(save_archive_path, input_path)

        sly.logger.debug(f"Unpacked archive {save_archive_path} to {input_path}.")

        silent_remove(save_archive_path)

    project_path = find_project_path(input_path)
    return project_path


def find_project_path(input_path):
    sly.logger.debug(f"Trying to find project path in {input_path}.")

    input_files = sly.fs.list_dir_recursively(input_path)
    for input_file in input_files:
        if get_file_name_with_ext(input_file) == "meta.json":
            parent_dir = os.path.dirname(input_file)
            project_path = os.path.join(input_path, parent_dir)

            sly.logger.debug(f"Found project path: {project_path}.")
            return project_path

    sly.logger.warning(
        f"Project path not found in any subdirectory of input path: {input_path}."
    )
