<div align="center" markdown>

<img src="https://user-images.githubusercontent.com/48245050/182842725-14a68b5d-2eac-4bc8-ad70-979cb05eacbd.jpg"/>

# Import Images in Supervisely format

<p align="center">
  <a href="#Overview">Overview</a> •
  <a href="#How-to-Run">How to Run</a> •
  <a href="#Demo">Demo</a>
</p>

[![](https://img.shields.io/badge/supervisely-ecosystem-brightgreen)](https://ecosystem.supervisely.com/apps/supervisely-ecosystem/import-images-in-sly-format)
[![](https://img.shields.io/badge/slack-chat-green.svg?logo=slack)](https://supervisely.com/slack)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/supervisely-ecosystem/import-images-in-sly-format)
[![views](https://app.supervisely.com/img/badges/views/supervisely-ecosystem/import-images-in-sly-format.png)](https://supervisely.com)
[![runs](https://app.supervisely.com/img/badges/runs/supervisely-ecosystem/import-images-in-sly-format.png)](https://supervisely.com)

</div>

# Overview

Import images in [Supervisely format](https://docs.supervisely.com/data-organization/00_ann_format_navi) with annotations. Supported extensions: `.jpg`, `.jpeg`, `.mpo`, `.bmp`, `.png`, `.webp` and `.tiff`.

🗄️ Starting from version `1.3.22` the application supports the import of images metadata from corresponding `meta` directory in dataset. Learn more about images metadata in [this article in our Developer Portal](https://developer.supervisely.com/getting-started/python-sdk-tutorials/images/image#get-and-update-image-metadata).

🗄️ Starting from version `1.3.12` the application supports the import of multiple projects at once. Each project should be placed in a separate directory with the correct structure (see below).

🏋️ Starting from version `v1.3.2` application supports import from a special directory on your local computer. It is made for Enterprise Edition customers who need to upload tens or even hundreds of gigabytes of data without using a drag-and-drop mechanism:

1. Run an agent on your computer where data is stored. Watch the [how-to video](https://youtu.be/aO7Zc4kTrVg).
2. Copy your data to a special folder on your computer that was created by the agent. Agent mounts this directory to your Supervisely instance and it becomes accessible in Team Files. Learn more [in the documentation](https://docs.supervisely.com/customization/agents/agent-storage). Watch the [how-to video](https://youtu.be/63Kc8Xq9H0U).
3. Go to `Team Files` -> `Supervisely Agent` and find your folder there.
4. Right-click to open the context menu and start the app. Now the app will upload data directly from your computer to the platform.

#### Input files structure

You can upload a directory or an archive. If you are uploading an archive, it must contain a single top-level directory.

The directory name defines the project name. Subdirectories define dataset names.<br>
ℹ️ You can download the archive with data example [here](https://github.com/supervisely-ecosystem/import-images-in-sly-format/files/12537201/robots_project.zip).

Project directory example:

Directory `meta` with images metadata is optional. If it is not present, images will be uploaded without any additional metadata.
If the directory is present, it may contain metadata files only for some images. In this case, only these images will have metadata, others will be uploaded without it.

```text
📦 project_name
 ┣ 📂 dataset_name_01
 ┃ ┣ 📂 ann
 ┃ ┃ ┣ 📄 IMG_0748.jpeg.json
 ┃ ┃ ┣ 📄 IMG_1836.jpeg.json
 ┃ ┃ ┣ 📄 IMG_2084.jpeg.json
 ┃ ┃ ┣ 📄 IMG_3861.jpeg.json
 ┃ ┃ ┣ 📄 IMG_4451.jpeg.json
 ┃ ┃ ┗ 📄 IMG_8144.jpeg.json
 ┃ ┣ 📂 img
 ┃ ┃ ┣ 🏞️ IMG_0748.jpeg
 ┃ ┃ ┣ 🏞️ IMG_1836.jpeg
 ┃ ┃ ┣ 🏞️ IMG_2084.jpeg
 ┃ ┃ ┣ 🏞️ IMG_3861.jpeg
 ┃ ┃ ┣ 🏞️ IMG_4451.jpeg
 ┃ ┃ ┗ 🏞️ IMG_8144.jpeg
 ┃ ┗ 📂 meta (optional)
 ┃ ┃ ┣ 📄 IMG_0748.jpeg.json
 ┃ ┃ ┣ 📄 IMG_1836.jpeg.json
 ┃ ┃ ┣ 📄 IMG_2084.jpeg.json
 ┃ ┃ ┣ 📄 IMG_3861.jpeg.json
 ┃ ┃ ┣ 📄 IMG_4451.jpeg.json
 ┃ ┃ ┗ 📄 IMG_8144.jpeg.json
 ┗ 📄 meta.json
```

As a result, we will get project `cats_vs_dogs_project` with 2 datasets named: `cats` and `dogs`.

# How to Run

**Step 1.** Add [Import images in Supervisely format](https://ecosystem.supervisely.com/apps/supervisely-ecosystem/import-images-in-sly-format) app to your team from Ecosystem

<img data-key="sly-module-link" data-module-slug="supervisely-ecosystem/import-images-in-sly-format" src="https://i.imgur.com/Y6RcQPT.png" width="350px" style='padding-bottom: 10px'/>

**Step 2.** Run the application from the context menu of the directory with images on the Team Files page

<img src="https://i.imgur.com/EStpQb5.png" width="80%" style='padding-top: 10px'>

**Step 3.** Press the Run button in the modal window

<img src="https://i.imgur.com/p4ThxkI.png" width="80%" style='padding-top: 10px'>

**Step 4.** After running the application, you will be redirected to the Tasks page. Once application processing has finished, your project will become available. Click on the project name to open it.

<img src="https://i.imgur.com/UGqGvi6.png" width="80%" style='padding-top: 10px'>

### Demo

Example of uploading images project with annotations to Supervisely:
![](https://i.imgur.com/XHBmtQ9.gif)
