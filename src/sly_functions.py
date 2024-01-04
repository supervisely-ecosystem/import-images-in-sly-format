import functools
import json
import os
import time
from collections import defaultdict
from os.path import basename, dirname, normpath
from typing import Callable, List

import requests
from tqdm import tqdm

import sly_globals as g
import supervisely as sly
from supervisely.annotation.annotation import AnnotationJsonFields
from supervisely.annotation.label import LabelJsonFields
from supervisely.io.fs import (
    download,
    file_exists,
    get_file_ext,
    get_file_name,
    get_file_name_with_ext,
    mkdir,
    silent_remove,
)


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
    if file_exists(archive_path) and sizeb != os.path.getsize(archive_path):
        silent_remove(archive_path)
        raise Exception(
            f"Failed to download dataset archive. "
            "It may be due to high load on the server. "
            f"Please, try again later."
        )
    app_logger.info(f"{file_name} has been successfully downloaded")


def download_file_from_dropbox(shared_link: str, destination_path, progress_message, app_logger):
    retry_attemp = 0
    timeout = 10

    total_size = None

    while True:
        try:
            with open(destination_path, "ab") as file:
                response = requests.get(
                    shared_link,
                    stream=True,
                    headers={"Range": f"bytes={file.tell()}-"},
                    timeout=timeout,
                )
                if total_size is None:
                    total_size = int(response.headers.get("content-length", 0))
                    progress_bar = tqdm(
                        desc=progress_message,
                        total=total_size,
                        is_size=True,
                    )
                app_logger.info("Connection established")
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        retry_attemp = 0
                        file.write(chunk)
                        progress_bar.update(len(chunk))
        except requests.exceptions.RequestException as e:
            retry_attemp += 1
            if timeout < 90:
                timeout += 10
            if retry_attemp == 9:
                raise e
            app_logger.warning(
                f"Downloading request error, please wait ... Retrying ({retry_attemp}/8)"
            )
            if retry_attemp <= 4:
                time.sleep(5)
            elif 4 < retry_attemp < 9:
                time.sleep(10)
        except Exception as e:
            retry_attemp += 1
            if retry_attemp == 3:
                raise e
            app_logger.warning(f"Error: {str(e)}. Retrying ({retry_attemp}/2")

        else:
            filename = get_file_name(destination_path)
            app_logger.info(f"{filename} downloaded successfully")
            break


def search_projects(dir_path):
    files = os.listdir(dir_path)
    meta_exists = "meta.json" in files
    if meta_exists:
        try:
            meta_path = os.path.join(dir_path, "meta.json")
            try:
                with open(meta_path, encoding="utf-8") as fin:
                    meta_json = json.load(fin)
            except json.decoder.JSONDecodeError as e:
                sly.logger.error(
                    f"Can not decode meta.json file with path {meta_path}: {e.msg} at "
                    f"line number: {e.lineno}, column: {e.colno}, position: {e.pos}. ",
                    exc_info=False,
                )
                return False
            meta_json = sly.json.load_json_file(meta_path)
            meta = sly.ProjectMeta.from_json(meta_json)
        except Exception as e:
            sly.logger.error(
                f"Incorrect meta.json file in {dir_path}. \nError: {repr(e)}",
                exc_info=False,
            )
            return False
    datasets = [f for f in files if sly.fs.dir_exists(os.path.join(dir_path, f))]
    datasets_exists = len(datasets) > 0
    return meta_exists and datasets_exists


def search_images_dir(dir_path):
    listdir = os.listdir(dir_path)
    images_found = any([sly.image.has_valid_ext(os.path.join(dir_path, f)) for f in listdir])
    return images_found


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

    if not g.IS_ON_AGENT:
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
            sly.logger.warn(
                f"Unsupported file extension ({save_archive_path}). \n"
                "Please, upload the data as directory or archive (.tar, .tar.gz or .zip)."
            )
            raise Exception(f"Downloaded file has unsupported extension. Read the app overview.")
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
        if remote_path.startswith("https://www.dropbox.com/"):
            download_file_from_dropbox(
                remote_path,
                save_archive_path,
                f"Downloading archive from link",
                g.my_app.logger,
            )
        else:
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
        try:
            sly.fs.unpack_archive(save_archive_path, input_path)
            # TODO Detecting multi-part archives in the main archive and unpacking them
        except Exception as e:
            raise Exception(f"Failed to read dataset archive file. Please try again. Error: {e}")
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
            try:
                meta_json = sly.json.load_json_file(os.path.join(r, "meta.json"))
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


def upload_only_images(api: sly.Api, img_dirs: list, recursively: bool = False):
    project_name = "Images project"
    project = api.project.create(g.WORKSPACE_ID, project_name, change_name_if_conflict=True)
    images_cnt = 0
    for img_dir in img_dirs:
        if not sly.fs.dir_exists(img_dir):
            continue
        if recursively:
            image_paths = sly.fs.list_files_recursively(
                img_dir,
                valid_extensions=sly.image.SUPPORTED_IMG_EXTS,
            )
        else:
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
        sly.fs.remove_dir(img_dir)
    if images_cnt > 1:
        sly.logger.info(f"{images_cnt} images were uploaded to project '{project.name}'.")
    elif images_cnt == 1:
        sly.logger.info(f"{images_cnt} image was uploaded to project '{project.name}'.")
    else:
        api.project.remove(project.id)
        return None
    project = api.project.get_info_by_id(project.id)
    return project


def check_items(imgs_dir, ann_dir, meta, keep_classes, remove_classes):
    items_cnt = 0
    failed_ann_names = defaultdict(list)
    img_names = [name for name in os.listdir(imgs_dir) if sly.image.has_valid_ext(name)]
    raw_ann_names = [name for name in os.listdir(ann_dir) if get_file_ext(name) == g.ANN_EXT]
    res_ann_names = []
    for img_name in img_names:
        try:
            ann_name = get_effective_ann_name(img_name, raw_ann_names)
            try:
                need_to_filter = False
                if ann_name is None:
                    raise Exception("Annotation file not found")
                ann_path = os.path.join(ann_dir, ann_name)
                with open(ann_path) as ann_file:
                    data = json.load(ann_file)
                    for field in g.REQUIRED_FIELDS:
                        if field not in data:
                            raise Exception(f"No '{field}' field in annotation file")
                    for label_json in data.get(AnnotationJsonFields.LABELS):
                        if label_json.get(LabelJsonFields.OBJ_CLASS_NAME) in remove_classes:
                            need_to_filter = True
                        sly.Label.from_json(label_json, meta)
                if need_to_filter:
                    ann = sly.Annotation.load_json_file(ann_path, meta)
                    ann = ann.filter_labels_by_classes(keep_classes)
                    sly.json.dump_json_file(ann.to_json(), ann_path)
            except Exception as e:
                ann_name = create_empty_ann(imgs_dir, img_name, ann_dir)
                failed_ann_names[e.args[0]].append(ann_name)
            items_cnt += 1
            res_ann_names.append(ann_name)
        except Exception as e:
            sly.logger.warn(f"Failed to process annotation for '{img_name}': {repr(e)}. Skipping.")

    if len(failed_ann_names) > 0:
        for error, ann_names in failed_ann_names.items():
            sly.logger.warn(
                f"[{error}] error occurred for {len(ann_names)} items: {ann_names}. "
                "Created empty annotation files for them."
            )
    unwanted_ann_names = list(set(raw_ann_names) - set(res_ann_names))
    if len(unwanted_ann_names) > 0:
        sly.logger.warn(
            f"Found {len(unwanted_ann_names)} annotation files without corresponding images: "
            f"{unwanted_ann_names}. Skipping."
        )
        for name in unwanted_ann_names:
            sly.fs.silent_remove(os.path.join(ann_dir, name))

    return items_cnt
