import os
import sys
from pathlib import Path

import supervisely as sly
# from dotenv import load_dotenv
from supervisely.app.v1.app_service import AppService
from supervisely.io.fs import mkdir

root_source_dir = str(Path(sys.argv[0]).parents[1])
print(f"App source directory: {root_source_dir}")
sys.path.append(root_source_dir)

# only for convenient debug
# debug_env_path = os.path.join(root_source_dir, "debug.env")
# secret_debug_env_path = os.path.join(root_source_dir, "secret_debug.env")
# load_dotenv(debug_env_path)
# load_dotenv(secret_debug_env_path, override=True)


api: sly.Api = sly.Api.from_env()
my_app: AppService = AppService()

TEAM_ID = int(os.environ["context.teamId"])
WORKSPACE_ID = int(os.environ["context.workspaceId"])
TASK_ID = int(os.environ["TASK_ID"])

INPUT_DIR: str = os.environ.get("modal.state.slyFolder", None)
INPUT_FILE: str = os.environ.get("modal.state.slyFile", None)

STORAGE_DIR: str = my_app.data_dir
mkdir(STORAGE_DIR, True)
