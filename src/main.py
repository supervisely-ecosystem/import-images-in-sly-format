import os

import supervisely as sly
from supervisely.io.fs import mkdir

from dotenv import load_dotenv

# for convenient debug, has no effect in production
if sly.is_development():
    load_dotenv("local.env")
    load_dotenv(os.path.expanduser("~/supervisely.env"))

import sly_functions as f
import sly_globals as g




class MyImport(sly.app.Import):

    def process(self, context: sly.app.Import.Context):

        project_dir = f.download_data_from_team_files(api=g.api, save_path=g.STORAGE_DIR, team_id=context.team_id)
        project_name = os.path.basename(project_dir)

        files = []
        for r, d, fs in os.walk(project_dir):
            files.extend(os.path.join(r, file) for file in fs)
        total_files = len(files) - 2
        progress_project_cb = f.get_progress_cb(
            g.api, f"Uploading project: {project_name}", total_files
        )
        sly.upload_project(
            dir=project_dir,
            api=g.api,
            workspace_id=context.workspace_id,
            project_name=project_name,
            progress_cb=progress_project_cb,
        )

app = MyImport()
app.run()
