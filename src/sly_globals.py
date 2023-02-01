import os

import supervisely as sly
from supervisely.io.fs import mkdir


api = sly.Api.from_env()

INPUT_DIR: str = os.environ.get("modal.state.slyFolder", None)
INPUT_FILE: str = os.environ.get("modal.state.slyFile", None)

if INPUT_DIR:
    IS_ON_AGENT = api.file.is_on_agent(INPUT_DIR)
else:
    IS_ON_AGENT = api.file.is_on_agent(INPUT_FILE)

STORAGE_DIR: str = sly.app.get_data_dir()
mkdir(STORAGE_DIR, True)
