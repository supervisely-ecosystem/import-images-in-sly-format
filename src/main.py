import os

import supervisely as sly

import sly_functions as f
import sly_globals as g


@g.my_app.callback("import-images-project")
@sly.timeit
def import_images_project(
    api: sly.Api, task_id: int, context: dict, state: dict, app_logger
) -> None:
    project_dirs = f.download_data_from_team_files(
        api=api, task_id=task_id, save_path=g.STORAGE_DIR
    )

    sly.logger.info(
        f"Found {len(project_dirs)} valid projects in the given directory. "
        f"Paths to the projects: {project_dirs}."
    )

    for project_dir in project_dirs:
        project_name = os.path.basename(os.path.normpath(project_dir))

        sly.logger.info(f"Working with project '{project_name}' from path '{project_dir}'.")

        files = []
        for r, d, fs in os.walk(project_dir):
            files.extend(os.path.join(r, file) for file in fs)
        total_files = len(files) - 2
        progress_project_cb = f.get_progress_cb(
            api, task_id, f"Uploading project: {project_name}", total_files
        )

        sly.logger.info(f"Start uploading project '{project_name}'...")

        sly.upload_project(
            dir=project_dir,
            api=api,
            workspace_id=g.WORKSPACE_ID,
            project_name=project_name,
            progress_cb=progress_project_cb,
        )

        sly.logger.info(f"Project '{project_name}' uploaded successfully.")

    sly.logger.info("All projects uploaded successfully, the app will be stopped.")

    g.my_app.stop()


def main():
    sly.logger.info(
        "Script arguments", extra={"TEAM_ID": g.TEAM_ID, "WORKSPACE_ID": g.WORKSPACE_ID}
    )
    g.my_app.run(initial_events=[{"command": "import-images-project"}])


if __name__ == "__main__":
    sly.main_wrapper("main", main)
