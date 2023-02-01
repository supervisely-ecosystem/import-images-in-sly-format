import os
import shutil
import supervisely as sly
from supervisely.io.fs import mkdir
from supervisely.io.fs import get_file_name_with_ext, silent_remove

from dotenv import load_dotenv

# for convenient debug, has no effect in production
if sly.is_development():
    load_dotenv("local.env")
    load_dotenv(os.path.expanduser("~/supervisely.env"))

import sly_functions as f
import sly_globals as g




class MyImport(sly.app.Import):

    def process(self, context: sly.app.Import.Context):

        #project_dir = f.download_data_from_team_files(api=g.api, save_path=g.STORAGE_DIR, team_id=context.team_id)
        project_dir = context.path
        if context.is_directory is False:
            shutil.unpack_archive(project_dir, g.STORAGE_DIR)
            silent_remove(project_dir)
        project_name = os.path.basename(project_dir)

        files = []
        for r, d, fs in os.walk(project_dir):
            files.extend(os.path.join(r, file) for file in fs)
        total_files = len(files) - 2
        progress_project_cb = f.get_progress_cb(
            g.api, f"Uploading project: {project_name}", total_files
        )
        temp = os.path.join(g.STORAGE_DIR, project_name)
        sly.logger.info(f"7777777777777777777777       {temp}")
        sly.upload_project(
            dir=temp,
            api=g.api,
            workspace_id=context.workspace_id,
            project_name=project_name,
            progress_cb=progress_project_cb,
        )

app = MyImport()
app.run()
