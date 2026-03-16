# wdr-ddj-cloud

This project enables the deployment of Python scripts to AWS using the [Serverless Framework](https://serverless.com/).

It provides tools to quickly and easily create new scrapers, deploy them to AWS Lambda and publicly serve the results via S3 and CloudFront, eg. for use in Datawrapper.

# Contributing

## Prerequisites

You need [Git](https://git-scm.com/downloads) and [Visual Studio Code](https://code.visualstudio.com/Download) or another editor of your choice to contribute a scraper to this project. It is recommended to install the [Python extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python) and [Pylance extension](https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance) for Visual Studio Code.

You will also need a [GitHub account](https://github.com/signup) if you don't already have one and request access to this repository.

## Setup

### Installing `uv`

uv is a fast Python package manager that manages Python installations, virtual environments, and dependencies for Python projects.

If you install uv using the standalone installer below, you usually do not need to install Python separately. uv will download a compatible interpreter automatically during `uv sync` if needed.

If you install uv via `pip`, you do need an existing Python installation first. The standalone installer is recommended.

Follow the installation instructions at https://docs.astral.sh/uv/getting-started/installation/ or run one of the following commands:

On Linux/macOS:

    curl -LsSf https://astral.sh/uv/install.sh | sh

On Windows (PowerShell):

    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

You can verify that `uv` is installed by running:

    uv --version

You can now close the terminal.

### Cloning the repository

Use the Windows Explorer to navigate to the folder where you want to clone the repository. Right-click in the folder and select "Git Bash Here" to open a terminal in the folder. Then run the following command to clone the repository:

    git clone https://github.com/wdr-data/wdr-ddj-cloud.git

You will be prompted for your GitHub username and password.

You can now open the cloned repository in Visual Studio Code by running the following command in the same git bash window:

    code wdr-ddj-cloud

Once the repository is open in Visual Studio Code, you can close the git bash terminal.

### Installing dependencies

Before we can start, we need to install the dependencies for the project. Open a terminal in Visual Studio Code by selecting "Terminal" -> "New Terminal" from the menu. Then run the following command to install the dependencies:

    uv sync --dev

If this is your first time running this command, `uv` will download Python 3.11 if necessary, create a virtual environment for the project, and install all dependencies. This may take a while. After completion, it is recommended to restart Visual Studio Code.

## Creating a scraper

### Creating a new branch

If you are familiar with Git, you should create a new branch for your scraper. Otherwise, you may skip this step.

### Create a new scraper

This project provides a template for new scrapers. To create a new scraper, run the following command in the terminal:

    uv run manage new

You will be guided through the process of creating a new scraper. You will be asked for the name of the scraper and some additional information. The name of the scraper will be used as the name of the folder for the scraper.

After the scraper was created, you will find a new folder in the `ddj_cloud/scrapers` folder with the name of your scraper, and within a `.py` file of the same name.

It will also tell you how to test your scraper.

### Make changes to the scraper

Find the scraper you created in the `ddj_cloud/scrapers` folder and open the `.py` file of the same name. By default, the system will execute the `run` function of your scraper. However it is possible to write a scraper without any functions and just execute the code in the file. In this case, you can remove the `run` function and just write your code as a simple Python script.

### Testing your scraper

You can run the following command to test your scraper:

    uv run manage test <scraper_name>

where `<scraper_name>` is the Python module name of your scraper.

If a local `.env` file exists in the repository root, `manage test` will load it automatically before importing the scraper.

For commands other than `manage test`, use uv's explicit `.env` support when needed, for example:

    uv run --env-file .env manage generate

The testing script will show you if any errors occurred during the execution of your scraper and it will also show you a summary of the files written by your scraper.

### Deploying your scraper

Once you are happy with your scraper, you need to commit your changes and push them to GitHub.

Basic steps are as follows.

To commit your changes, select the "Source Control" tab in Visual Studio Code and enter a commit message. Then click the checkmark icon to commit your changes.

![Committing](docs/images/vscode_commit.png?raw=true "Committing")

When asked if you want to stage all changes, select "Yes".

To push your changes, select the 🔄 icon in the menu bar at the bottom left of Visual Studio Code.

![Synchronizing changes](docs/images/vscode_sync.png?raw=true "Synchronizing changes")

Refer to [this guide](https://code.visualstudio.com/docs/sourcecontrol/overview) for more information on how to work with Git in Visual Studio Code.

If you created a new branch, you should create a pull request to merge your changes into the `main` branch. Otherwise, your changes will automatically be deployed to the staging deployment on AWS.
