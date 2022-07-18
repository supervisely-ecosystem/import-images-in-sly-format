<div align="center" markdown>

<img src="https://i.imgur.com/huPAo69.png" style="width: 100%;"/>

# Import Images in Supervisely format

<p align="center">
  <a href="#Overview">Overview</a> •
  <a href="#How-to-Run">How to Run</a> •
  <a href="#Demo">Demo</a>
</p>

[![](https://img.shields.io/badge/supervisely-ecosystem-brightgreen)](https://ecosystem.supervise.ly/apps/supervisely-ecosystem/import-images-in-sly-format)
[![](https://img.shields.io/badge/slack-chat-green.svg?logo=slack)](https://supervise.ly/slack)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/supervisely-ecosystem/import-images-in-sly-format)
[![views](https://app.supervise.ly/img/badges/views/supervisely-ecosystem/import-images-in-sly-format)](https://supervise.ly)
[![runs](https://app.supervise.ly/img/badges/runs/supervisely-ecosystem/import-images-in-sly-format)](https://supervise.ly)

</div>

# Overview

Import images in [Supervisely format](https://docs.supervise.ly/data-organization/00_ann_format_navi) with annotations. Supported extensions: `.jpg`, `.jpeg`, `.mpo`, `.bmp`, `.png`, `.webp`.

#### Input files structure

You can upload a directory or an archive. If you are uploading an archive, it must contain a single top-level directory.

Directory name defines project name. Subdirectories define dataset names.

Project directory example:

```
.
cats_vs_dogs_project
├── cats
│   ├── ann
│   │   ├── cats_1.jpg.json
│   │   ├── ...
│   │   └── cats_9.jpg.json
│   └── img
│       ├── cats_1.jpg
│       ├── ...
│       └── cats_9.jpg
├── dogs
│   ├── ann
│   │   ├── dogs_1.jpg.json
│   │   ├── ...
│   │   └── dogs_9.jpg.json
│   └── img
│       ├── dogs_1.jpg
│       ├── ...
│       └── dogs_9.jpg
└── meta.json
```

As a result we will get project `cats_vs_dogs_project` with 2 datasets named: `cats` and `dogs`.

# How to Run

**Step 1.** Add [Import images in Supervisely format](https://ecosystem.supervise.ly/apps/supervisely-ecosystem/import-images-in-sly-format) app to your team from Ecosystem

<img data-key="sly-module-link" data-module-slug="supervisely-ecosystem/import-images-in-sly-format" src="https://i.imgur.com/Y6RcQPT.png" width="350px" style='padding-bottom: 10px'/>

**Step 2.** Run the application from the context menu of the directory with images on Team Files page

<img src="https://i.imgur.com/EStpQb5.png" width="80%" style='padding-top: 10px'>  

**Step 3.** Press the Run button in the modal window

<img src="https://i.imgur.com/p4ThxkI.png" width="80%" style='padding-top: 10px'>

**Step 4.** After running the application, you will be redirected to the Tasks page. Once application processing has finished, your project will become available. Click on the project name to open it.

<img src="https://i.imgur.com/UGqGvi6.png" width="80%" style='padding-top: 10px'>

### Demo
Example of uploading images project with annotations to Supervisely:
![](https://i.imgur.com/XHBmtQ9.gif)


