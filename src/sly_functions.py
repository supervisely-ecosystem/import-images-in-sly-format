import functools
import os
import shutil
from os.path import basename, dirname, normpath
import requests
from typing import Callable, List
from collections import defaultdict

import supervisely as sly
from supervisely.io.fs import (
    get_file_name_with_ext,
    silent_remove,
    get_file_ext,
    get_file_name,
    file_exists,
    download,
    mkdir,
)

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


def download_file_from_link(link, file_name, archive_path, progress_message, app_logger):
    response = requests.head(link, allow_redirects=True)
    sizeb = int(response.headers.get("content-length", 0))
    progress_cb = get_progress_cb(g.api, g.TASK_ID, progress_message, sizeb, is_size=True)
    if not file_exists(archive_path):
        download(link, archive_path, cache=g.my_app.cache, progress=progress_cb)
        app_logger.info(f"{file_name} has been successfully downloaded")


def search_projects(dir_path):
    files = os.listdir(dir_path)
    meta_exists = "meta.json" in files
    datasets = [f for f in files if sly.fs.dir_exists(os.path.join(dir_path, f))]
    datasets_exists = len(datasets) > 0
    return meta_exists and datasets_exists


def search_images_dir(dir_path):
    listdir = os.listdir(dir_path)
    is_img_dir = any(get_file_ext(f) in sly.image.SUPPORTED_IMG_EXTS for f in listdir)
    return is_img_dir


def is_archive(path):
    return get_file_ext(path) in [".zip", ".tar"] or path.endswith(".tar.gz")


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

    if g.INPUT_DIR:
        listdir = api.file.listdir(g.TEAM_ID, g.INPUT_DIR)
        archives_cnt = len([is_archive(file) for file in listdir if is_archive(file) is True])
        if archives_cnt > 1:
            raise Exception("Multiple archives are not supported.")
        if len(listdir) == 1 and archives_cnt == 1:
            sly.logger.info(
                "Folder mode is selected, but archive file is uploaded. Switching to file mode."
            )
            g.INPUT_DIR, g.INPUT_FILE = None, listdir[0]
        else:
            if all(
                basename(normpath(x)) in ["img", "ann", "meta"]
                for x in listdir
                if api.file.dir_exists(g.TEAM_ID, x)
            ):
                g.INPUT_DIR = dirname(normpath(g.INPUT_DIR))
                listdir = api.file.listdir(g.TEAM_ID, g.INPUT_DIR)
            if basename(normpath(g.INPUT_DIR)) in ["img", "ann", "meta"]:
                g.INPUT_DIR = dirname(normpath(g.INPUT_DIR))
                listdir = api.file.listdir(g.TEAM_ID, g.INPUT_DIR)
            if "meta.json" in [
                basename(normpath(x))
                for x in api.file.listdir(g.TEAM_ID, dirname(normpath(g.INPUT_DIR)))
            ]:
                g.INPUT_DIR = dirname(normpath(g.INPUT_DIR))
            if not g.INPUT_DIR.endswith("/"):
                g.INPUT_DIR += "/"

    if g.INPUT_FILE:
        file_ext = get_file_ext(g.INPUT_FILE)
        if not is_archive(g.INPUT_FILE):
            sly.logger.info("File mode is selected, but uploaded file is not archive.")
            if basename(normpath(g.INPUT_FILE)) == "meta.json":
                g.INPUT_DIR, g.INPUT_FILE = dirname(g.INPUT_FILE), None
            elif sly.image.is_valid_ext(file_ext) or file_ext == ".json":
                parent_dir = dirname(normpath(g.INPUT_FILE))
                listdir = api.file.listdir(g.TEAM_ID, parent_dir)
                if all(
                    basename(normpath(x)) in ["img", "ann", "meta"]
                    for x in listdir
                    if api.file.dir_exists(g.TEAM_ID, x)
                ):
                    parent_dir = dirname(normpath(parent_dir))
                    listdir = api.file.listdir(g.TEAM_ID, parent_dir)
                if basename(normpath(parent_dir)) in ["img", "ann", "meta"]:
                    parent_dir = dirname(normpath(parent_dir))
                    listdir = api.file.listdir(g.TEAM_ID, parent_dir)
                if "meta.json" in [
                    basename(normpath(x)) for x in api.file.listdir(g.TEAM_ID, dirname(parent_dir))
                ]:
                    sly.logger.info(f"Found meta.json in {dirname(parent_dir)}.")
                    parent_dir = dirname(normpath(parent_dir))
                if not parent_dir.endswith("/"):
                    parent_dir += "/"
                g.INPUT_DIR, g.INPUT_FILE = parent_dir, None

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
        sly.fs.remove_junk_from_dir(input_path)

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
        if not is_archive(save_archive_path):
            raise Exception(f"Downloaded file is not archive. Path: {save_archive_path}")
        sly.fs.unpack_archive(save_archive_path, input_path)
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
            app_logger=g.my_app.logger,
        )
        input_path = os.path.join(save_path, get_file_name(proj_path))
        if not is_archive(save_archive_path):
            raise Exception(f"Downloaded file is not archive. Path: {save_archive_path}")
        sly.fs.unpack_archive(save_archive_path, input_path)
        sly.logger.debug(f"Unpacked archive {save_archive_path} to {input_path}.")
        silent_remove(save_archive_path)

    project_dirs = [project_dir for project_dir in sly.fs.dirs_filter(input_path, search_projects)]

    only_images = []
    if len(project_dirs) == 0:
        only_images = [img_dir for img_dir in sly.fs.dirs_filter(input_path, search_images_dir)]

    bad_projs = defaultdict(int)
    project_type_to_cls = {
        "videos": sly.VideoProject,
        "volumes": sly.VolumeProject,
        "point_clouds": sly.PointcloudProject,
        "point_cloud_episodes": sly.PointcloudEpisodeProject,
    }
    bad_projs = defaultdict(int)
    # search for projects with another types
    for r, d, fs in os.walk(input_path):
        if "meta.json" in fs:
            meta_json = sly.json.load_json_file(os.path.join(r, "meta.json"))
            try:
                meta = sly.ProjectMeta.from_json(meta_json)
            except:
                continue
            if meta.project_type == str(sly.ProjectType.IMAGES):
                continue
            try:
                pr_cls = project_type_to_cls[meta.project_type]
                sly.read_single_project(r, pr_cls)
            except:
                continue
            bad_projs[str(meta.project_type)] += 1
            bad_projs["total"] += 1

    bad_proj_cnt = bad_projs["total"]
    bad_proj_msg = "Projects with another types are found: "
    for pr_type, cnt in bad_projs.items():
        if pr_type != "total":
            bad_proj_msg += f"{cnt} {pr_type}; "

    if bad_proj_cnt > 0:
        sly.logger.warn(f"{bad_proj_msg}. Make sure that you are uploading only images projects.")
    return project_dirs, only_images


def get_effective_ann_name(img_name, ann_names):
    new_format_name = img_name + g.ANN_EXT
    if new_format_name in ann_names:
        return new_format_name
    else:
        old_format_name = os.path.splitext(img_name)[0] + g.ANN_EXT
        return old_format_name if (old_format_name in ann_names) else None


def create_empty_ann(imgs_dir, img_name, ann_dir):
    ann = sly.Annotation.from_img_path(os.path.join(imgs_dir, img_name))
    ann_name = img_name + g.ANN_EXT
    sly.json.dump_json_file(ann.to_json(), os.path.join(ann_dir, ann_name))
    return ann_name


def upload_only_images(api: sly.Api, task_id, img_dirs: list):
    project_name = "Images project"
    project = api.project.create(g.WORKSPACE_ID, project_name, change_name_if_conflict=True)
    images_cnt = 0
    for img_dir in img_dirs:
        if not sly.fs.dir_exists(img_dir):
            continue
        image_paths = sly.fs.list_files(
            img_dir,
            valid_extensions=sly.image.SUPPORTED_IMG_EXTS,
            ignore_valid_extensions_case=True,
        )
        if len(image_paths) == 0:
            continue
        dataset_name = os.path.basename(os.path.normpath(img_dir))
        dataset = api.dataset.create(project.id, dataset_name, change_name_if_conflict=True)
        image_names = [
            os.path.basename(path) for path in image_paths if sly.image.has_valid_ext(path)
        ]
        images = api.image.upload_paths(dataset.id, image_names, image_paths)
        images_cnt += len(images)
    if images_cnt > 1:
        sly.logger.info(f"{images_cnt} images were uploaded to project '{project_name}'.")
    elif images_cnt == 1:
        sly.logger.info(f"{images_cnt} image was uploaded to project '{project_name}'.")
    return project.name
