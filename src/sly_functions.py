import functools
import os
import shutil
import requests
from typing import Callable, List

import supervisely as sly
from supervisely.io.fs import get_file_name_with_ext, silent_remove, get_file_name, file_exists, download, mkdir

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

def download_file_from_link(
    link, file_name, archive_path, progress_message, app_logger
):
    response = requests.head(link, allow_redirects=True)
    sizeb = int(response.headers.get("content-length", 0))
    progress_cb = get_progress_cb(
        g.api, g.TASK_ID, progress_message, sizeb, is_size=True
    )
    if not file_exists(archive_path):
        download(link, archive_path, cache=g.my_app.cache, progress=progress_cb)
        app_logger.info(f"{file_name} has been successfully downloaded")

def download_data(api: sly.Api, task_id: int, save_path: str) -> List[str]:
    """
    Download data and returns list of valid images project paths.

    :param api: Supervisely API object.
    :type api: sly.Api
    :param task_id: Supervisely task ID.
    :type task_id: int
    :param save_path: Path to save data.
    :type save_path: str
    :return: List of valid images project paths.
    :rtype: List[str]
    """

    if g.INPUT_DIR is not None:
        # If the app received a path to the directory in TeamFiles from environment variables.
        sly.logger.debug(f"The app is working with directory {g.INPUT_DIR}.")

        if g.IS_ON_AGENT:
            agent_id, cur_files_path = api.file.parse_agent_id_and_path(g.INPUT_DIR)
        else:
            cur_files_path = g.INPUT_DIR
        remote_path = g.INPUT_DIR
        input_path = os.path.join(save_path, os.path.basename(os.path.normpath(cur_files_path)))
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
        # If the app received a path to the file in TeamFiles from environment variables.
        sly.logger.debug(f"The app is working with file {g.INPUT_FILE}.")

        if g.IS_ON_AGENT:
            agent_id, cur_files_path = api.file.parse_agent_id_and_path(g.INPUT_FILE)
        else:
            cur_files_path = g.INPUT_FILE
        remote_path = g.INPUT_FILE

        save_archive_path = os.path.join(save_path, get_file_name_with_ext(cur_files_path))
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
    
    elif g.EXTERNAL_LINK is not None:
        remote_path = g.EXTERNAL_LINK
        file_name = "my_project.tar"
        proj_path = os.path.join(save_path, get_file_name(file_name))
        if not os.path.exists(proj_path):
            mkdir(proj_path, True)
        save_archive_path = os.path.join(proj_path, file_name)
        download_file_from_link(
            link=remote_path, 
            file_name=file_name,
            archive_path=save_archive_path,
            progress_message=f"Downloading archive from link",
            app_logger=g.my_app.logger
        )
        input_path = os.path.join(save_path, get_file_name(proj_path))
        shutil.unpack_archive(save_archive_path, input_path)
        sly.logger.debug(f"Unpacked archive {save_archive_path} to {input_path}.")
        silent_remove(save_archive_path)

    project_dirs = [
        project_dir for project_dir in sly.project.project.find_project_dirs(input_path)
    ]

    bad_proj_types = []
    for r, d, fs in os.walk(input_path):
        if "meta.json" in fs:
            meta_json = sly.json.load_json_file(os.path.join(r, "meta.json"))
            meta = sly.ProjectMeta.from_json(meta_json)
            if meta.project_type != sly.ProjectType.IMAGES:
                bad_proj_types.append(meta.project_type)

    if len(project_dirs) == 0:
        msg = f"No valid projects found in the given directory {input_path}."
        if len(bad_proj_types) > 0:
            msg += f"\nProjects with another types are found: {bad_proj_types}."
        raise FileNotFoundError(msg)
    elif len(bad_proj_types) > 0:
        sly.logger.warn(
            f"Found {len(bad_proj_types)} projects with another types: {bad_proj_types}."
            f"Make sure that you are uploading only images projects."
        )
    return project_dirs
