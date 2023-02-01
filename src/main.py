import os
import shutil
import functools
import supervisely as sly
from supervisely.io.fs import mkdir
from supervisely.io.fs import silent_remove
from supervisely.io.fs import mkdir
from typing import Callable

from dotenv import load_dotenv

# for convenient debug, has no effect in production
if sly.is_development():
    load_dotenv("local.env")
    load_dotenv(os.path.expanduser("~/supervisely.env"))


api = sly.Api.from_env()

STORAGE_DIR: str = sly.app.get_data_dir()
mkdir(STORAGE_DIR, True)


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


class MyImport(sly.app.Import):

    def process(self, context: sly.app.Import.Context):

        project_dir = context.path
        if context.is_directory is False:
            shutil.unpack_archive(project_dir, STORAGE_DIR)
            silent_remove(project_dir)
            project_name = os.listdir(STORAGE_DIR)[0]
            if len(os.listdir(STORAGE_DIR)) > 1:
                raise Exception("There must be only 1 project directory in the archive")
            project_dir = os.path.join(STORAGE_DIR, project_name)
        else:
            project_name = os.path.basename(project_dir)

        files = []
        for r, d, fs in os.walk(project_dir):
            files.extend(os.path.join(r, file) for file in fs)
        total_files = len(files) - 2
        progress_project_cb = get_progress_cb(
            api, f"Uploading project: {project_name}", total_files
        )
        
        sly.upload_project(
            dir=project_dir,
            api=api,
            workspace_id=context.workspace_id,
            project_name=project_name,
            progress_cb=progress_project_cb,
        )

app = MyImport()
app.run()
