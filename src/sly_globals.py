import os
from supervisely.api.api import Api
from supervisely.app.v1.app_service import AppService
from supervisely.io.fs import mkdir

# only for convenient debug
from dotenv import load_dotenv
load_dotenv("local.env")
load_dotenv(os.path.expanduser("~/supervisely.env"))

api: Api = Api.from_env()
my_app: AppService = AppService()

TEAM_ID = int(os.environ["context.teamId"])
WORKSPACE_ID = int(os.environ["context.workspaceId"])
TASK_ID = int(os.environ["TASK_ID"])

INPUT_DIR: str = os.environ.get("modal.state.slyFolder", None)
INPUT_FILE: str = os.environ.get("modal.state.slyFile", None)

DS_NINJA_LINK: bool = False
if INPUT_FILE.startswith("https://") or INPUT_FILE.startswith("http://"):
    DS_NINJA_LINK = True

if INPUT_DIR:
    IS_ON_AGENT = api.file.is_on_agent(INPUT_DIR)
else:
    IS_ON_AGENT = api.file.is_on_agent(INPUT_FILE)

STORAGE_DIR: str = my_app.data_dir
mkdir(STORAGE_DIR, True)
