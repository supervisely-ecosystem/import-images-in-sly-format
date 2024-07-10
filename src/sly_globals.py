import os
import supervisely as sly
from dotenv import load_dotenv
from supervisely.io.fs import mkdir
from supervisely.annotation.annotation import AnnotationJsonFields
from supervisely.app.v1.app_service import AppService
from workflow import Workflow

if sly.is_development():
    load_dotenv("local.env")
    load_dotenv(os.path.expanduser("~/supervisely.env"))


api: sly.Api = sly.Api.from_env()
my_app: AppService = AppService()

workflow = Workflow(api)

TEAM_ID = int(os.environ["context.teamId"])
WORKSPACE_ID = int(os.environ["context.workspaceId"])
TASK_ID = int(os.environ["TASK_ID"])

INPUT_DIR: str = os.environ.get("modal.state.slyFolder", None)
INPUT_FILE: str = os.environ.get("modal.state.slyFile", None)
EXTERNAL_LINK: str = os.environ.get("modal.state.slyArchiveUrl", None)
PROJECT_NAME: str = os.environ.get("modal.state.slyProjectName", None)
if EXTERNAL_LINK is not None:
    if not (EXTERNAL_LINK.startswith("https://") or EXTERNAL_LINK.startswith("http://")):
        raise ValueError("The link must start with 'https://' or 'http://'")


sly.logger.debug(
    f"INPUT_DIR: {INPUT_DIR}, INPUT_FILE: {INPUT_FILE}, EXTERNAL_LINK=f{EXTERNAL_LINK}"
)

IS_ON_AGENT = False
if INPUT_DIR:
    IS_ON_AGENT = api.file.is_on_agent(INPUT_DIR)
elif INPUT_FILE:
    IS_ON_AGENT = api.file.is_on_agent(INPUT_FILE)

STORAGE_DIR: str = my_app.data_dir
mkdir(STORAGE_DIR, True)

ANN_EXT = ".json"
REQUIRED_FIELDS = [
    AnnotationJsonFields.LABELS,
    AnnotationJsonFields.IMG_SIZE,
    AnnotationJsonFields.IMG_TAGS,
]
