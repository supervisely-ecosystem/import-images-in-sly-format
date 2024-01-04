import json
import os
from collections import defaultdict

import sly_functions as f
import sly_globals as g
import supervisely as sly
from supervisely.annotation.annotation import AnnotationJsonFields


@g.my_app.callback("import-images-project")
@sly.timeit
def import_images_project(
    api: sly.Api, task_id: int, context: dict, state: dict, app_logger
) -> None:
    project_dirs, only_images = f.download_data(api=api, task_id=task_id, save_path=g.STORAGE_DIR)

    if len(project_dirs) == 0 and len(only_images) == 0:
        raise Exception(f"Not found any images for import. Please, check your input data.")

    if len(project_dirs) > 0:
        sly.logger.info(
            f"Found {len(project_dirs)} project directories in the given directory. "
            f"Paths to the projects: {project_dirs}."
        )

        success_projects = 0
        projects_without_ann = 0
        failed_projects = 0

        for project_dir in project_dirs:
            if g.PROJECT_NAME is None:
                project_name = os.path.basename(os.path.normpath(project_dir))
            else:
                project_name = g.PROJECT_NAME
            sly.logger.info(f"Working with directory '{project_dir}'.")

            meta_path = os.path.join(project_dir, "meta.json")
            meta_json = sly.json.load_json_file(meta_path)
            meta = sly.ProjectMeta.from_json(meta_json)

            keep_classes = []
            remove_classes = []
            for obj_cls in meta.obj_classes:
                if obj_cls.geometry_type != sly.Cuboid:
                    keep_classes.append(obj_cls.name)
                else:
                    sly.logger.warn(
                        f"Class {obj_cls.name} has unsupported geometry type {obj_cls.geometry_type.name()}. "
                        f"Class will be removed from meta and all annotations."
                    )
                    remove_classes.append(obj_cls.name)

            project_items_cnt = 0
            invalid_datasets = []
            ds_cnt = len(os.listdir(project_dir))
            for dataset_dir in os.listdir(project_dir):
                dataset_path = os.path.join(project_dir, dataset_dir)
                imgs_dir = os.path.join(dataset_path, "img")
                ann_dir = os.path.join(dataset_path, "ann")
                if not sly.fs.dir_exists(dataset_path):
                    ds_cnt -= 1
                    continue
                if len(os.listdir(dataset_path)) == 0:
                    sly.fs.remove_dir(dataset_path)
                    ds_cnt -= 1
                    continue
                if not sly.fs.dir_exists(imgs_dir):
                    invalid_datasets.append(dataset_path)
                    continue
                if len(os.listdir(imgs_dir)) == 0:
                    sly.fs.remove_dir(dataset_path)
                    ds_cnt -= 1
                    continue
                if not sly.fs.dir_exists(ann_dir):
                    sly.fs.mkdir(ann_dir)

                ds_items_cnt = f.check_items(imgs_dir, ann_dir, meta, keep_classes, remove_classes)
                if ds_items_cnt == 0:
                    invalid_datasets.append(dataset_path)
                    continue
                else:
                    project_items_cnt += ds_items_cnt

            if len(invalid_datasets) > 0:
                sly.logger.warn(
                    f"Incorrect Supervisely format datasets: {invalid_datasets}. \n"
                    f"Trying to upload only images."
                )
                project_without_ann = f.upload_only_images(api, invalid_datasets, recursively=True)
                if project_without_ann is not None:
                    project_items_cnt += project_without_ann.items_count
                    projects_without_ann += 1

            if project_items_cnt == 0:
                sly.logger.warn(f"Not found images in the directory '{project_dir}'.")
                failed_projects += 1
                continue

            if len(remove_classes) > 0:
                meta = meta.delete_obj_classes(remove_classes)
                sly.logger.info(f"Meta was updated. Removed classes: {remove_classes}.")
                sly.json.dump_json_file(meta.to_json(), meta_path)

            if ds_cnt > len(invalid_datasets):
                try:
                    # find projects again, because some datasets may be already uploaded and removed
                    for project_dir in sly.fs.dirs_filter(project_dir, f.search_projects):
                        progress_project_cb = f.get_progress_cb(
                            api,
                            task_id,
                            f"Uploading project: {project_name}",
                            project_items_cnt * 2,
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
                        success_projects += 1
                except Exception as e:
                    try:
                        project = sly.project.read_single_project(project_dir)
                        sly.logger.warn(f"Project '{project_name}' uploading failed: {str(e)}.")
                        project = f.upload_only_images(
                            api, [ds.item_dir for ds in project.datasets], recursively=True
                        )
                        if project is None:
                            raise Exception
                        projects_without_ann += 1
                    except Exception as e:
                        failed_projects += 1
                        sly.logger.warn(f"Not found images in the directory '{project_dir}'.")

        total = success_projects + projects_without_ann + failed_projects
        msg = f"SUMMARY: \n    Toral processed projects: {total}. "
        if success_projects + projects_without_ann > 0:
            msg += f"\n    Uploaded projects: {success_projects + projects_without_ann} "
        if projects_without_ann > 0:
            msg += f"({projects_without_ann} projects without annotations)."
        if failed_projects > 0:
            msg += f"\n    Failed to upload projects: {failed_projects}."
            msg += f"Incorrect Supervisely format. Please, check your input data."
        sly.logger.info(msg)

        if success_projects == 0 and projects_without_ann == 0:
            raise Exception(
                f"Failed to import data. Not found images or projects in Supervisely format."
            )

    elif len(only_images) > 0:
        sly.logger.warn(
            f"Not found valid data (projects in Supervisely format). "
            f"Trying to upload only images from directories: {only_images}."
        )
        project = f.upload_only_images(api, only_images)
        if project is None:
            raise Exception(f"Failed to import data. Not found images.")

    g.my_app.stop()


def main():
    sly.logger.info(
        "Script arguments", extra={"TEAM_ID": g.TEAM_ID, "WORKSPACE_ID": g.WORKSPACE_ID}
    )
    g.my_app.run(initial_events=[{"command": "import-images-project"}])


if __name__ == "__main__":
    sly.main_wrapper("main", main, log_for_agent=False)
