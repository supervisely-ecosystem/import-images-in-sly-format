import os

import supervisely as sly
from supervisely.annotation.annotation import AnnotationJsonFields

import sly_functions as f
import sly_globals as g


def _get_effective_ann_name(img_name, ann_names):
    new_format_name = img_name + g.ANN_EXT
    if new_format_name in ann_names:
        return new_format_name
    else:
        old_format_name = os.path.splitext(img_name)[0] + g.ANN_EXT
        return old_format_name if (old_format_name in ann_names) else None


@g.my_app.callback("import-images-project")
@sly.timeit
def import_images_project(
    api: sly.Api, task_id: int, context: dict, state: dict, app_logger
) -> None:
    project_dirs = f.download_data(api=api, task_id=task_id, save_path=g.STORAGE_DIR)

    if len(project_dirs) == 0:
        raise Exception(f"No valid projects found in the given directory {g.INPUT_DIR}.")

    sly.logger.info(
        f"Found {len(project_dirs)} valid projects in the given directory. "
        f"Paths to the projects: {project_dirs}."
    )

    fails = []
    for project_dir in project_dirs:
        if g.PROJECT_NAME is None:
            project_name = os.path.basename(os.path.normpath(project_dir))
        else:
            project_name = g.PROJECT_NAME

        sly.logger.info(f"Working with project '{project_name}' from path '{project_dir}'.")
        files_cnt = 0
        for dataset_dir in os.listdir(project_dir):
            dataset_path = os.path.join(project_dir, dataset_dir)
            imgs_dir = os.path.join(dataset_path, "img")
            ann_dir = os.path.join(dataset_path, "ann")
            if not sly.fs.dir_exists(dataset_path):
                continue
            if (
                len(os.listdir(dataset_path)) == 0
                or not sly.fs.dir_exists(imgs_dir)
                or len(os.listdir(imgs_dir)) == 0
            ):
                sly.fs.remove_dir(dataset_path)
                continue
            if not sly.fs.dir_exists(ann_dir):
                sly.fs.mkdir(ann_dir)

            img_names = os.listdir(imgs_dir)
            raw_ann_names = os.listdir(ann_dir)
            res_ann_names = []
            for img_name in img_names:
                ann_name = _get_effective_ann_name(img_name, raw_ann_names)
                if ann_name is None:
                    sly.logger.warn(
                        f"Annotation file for image '{img_name}' not found in the given directory {ann_dir}. "
                        "Will create an empty annotation file for this image."
                    )
                    ann = sly.Annotation.from_img_path(os.path.join(imgs_dir, img_name))
                    ann_json = ann.to_json()

                    # NOTE: remove after fixing mapping customBigData in annotation
                    if AnnotationJsonFields.CUSTOM_DATA in ann_json.keys():
                        del ann_json[AnnotationJsonFields.CUSTOM_DATA]

                    ann_name = img_name + g.ANN_EXT
                    sly.json.dump_json_file(ann_json, os.path.join(ann_dir, ann_name))
                res_ann_names.append(ann_name)
                files_cnt += 2
            unwanted_ann_names = list(set(raw_ann_names) - set(res_ann_names))
            if len(unwanted_ann_names) > 0:
                sly.logger.warn(
                    f"Found {len(unwanted_ann_names)} annotation files without corresponding images: {unwanted_ann_names}. "
                )
                for name in unwanted_ann_names:
                    sly.fs.silent_remove(os.path.join(ann_dir, name))

        if files_cnt == 0:
            sly.logger.warn(
                f"Incorrect project structure for project '{project_name}'. Skipping..."
            )
            fails.append(project_name)
            continue

        try:
            progress_project_cb = f.get_progress_cb(
                api, task_id, f"Uploading project: {project_name}", files_cnt
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
        except Exception as e:
            fails.append(project_name)
            sly.logger.warn(f"Project '{project_name}' uploading failed: {e}.")

    success = len(project_dirs) - len(fails)
    if success > 0:
        sly.logger.info(
            f"{success} project{'s' if success > 1 else ''} were uploaded successfully."
        )
    if len(fails) > 0:
        sly.logger.warn(f"Projects {fails} were not uploaded. Check your input data.")

    g.my_app.stop()


def main():
    sly.logger.info(
        "Script arguments", extra={"TEAM_ID": g.TEAM_ID, "WORKSPACE_ID": g.WORKSPACE_ID}
    )
    g.my_app.run(initial_events=[{"command": "import-images-project"}])


if __name__ == "__main__":
    sly.main_wrapper("main", main)
