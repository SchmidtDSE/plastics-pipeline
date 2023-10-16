Plastics Pipeline
================================================================================
[Luigi](https://luigi.readthedocs.io/en/stable/)-based pipeline to sweep and select machine learning models for the plastics outcomes projection. This is used by [https://global-plastics-tool.org/](https://global-plastics-tool.org/).

<br>

Usage
--------------------------------------------------------------------------------
Most users can simply reference the output from the latest execution. That output is written to [https://global-plastics-tool.org/datapipeline.zip](https://global-plastics-tool.org/datapipeline.zip) and is publicly available under the [CC-BY-NC License](https://github.com/SchmidtDSE/plastics-pipeline/blob/main/LICENSE.md). That said, users may also leverage a local environment if desired.

<br>

Container Environment
--------------------------------------------------------------------------------
A containerized Docker environment is available for execution:

 - [Install Docker](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04)
 - Build the environment: `docker compose up --build``
 - Open a shell in the container: ``
 - Run the pipeline: ``

This will conduct the model sweeps and prepare the outputs required for the [front-end tool](https://github.com/SchmidtDSE/plastics-prototype).

<br>

Manual Environment
--------------------------------------------------------------------------------
In addition to the Docker container, a manual environment can be established simply by running `pip install -r requirements.txt`. This assumes that sqlite3 is installed.

<br>

Configuration
--------------------------------------------------------------------------------
The configuration for the job can be modified by providing a custom json file. See [task/job.json](https://github.com/SchmidtDSE/plastics-pipeline/blob/main/task/job.json) for an example. Note that the pipeline, by default, uses random forest even though a full sweep is conducted because that approach tends to yield better avoidance of overfitting.

<br>

Deployment
--------------------------------------------------------------------------------
This pipeline can be deployed by merging to the `deploy` branch of the repository, firing GitHub actions. This will cause the pipeline output files to be written to [https://global-plastics-tool.org/datapipeline.zip](https://global-plastics-tool.org/datapipeline.zip).

<br>

Development Standards
--------------------------------------------------------------------------------
CI / CD should be passing before merges to `main` which is used to stage pipeline deployments and `deploy`. Where possible, please follow the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html). Please note that tests on pipeline outputs and, thus, the pipeline are run as tasks in Luigi during pipeline execution itself.

<br>

Open Source
--------------------------------------------------------------------------------
This project is released as open source (BSD and CC-BY-NC). See [LICENSE.md](https://github.com/SchmidtDSE/plastics-pipeline/blob/main/LICENSE.md) for further details. In addition to this, please note that this project uses the following open source:

 - [Luigi](https://luigi.readthedocs.io/en/stable/index.html) under the [Apache v2 License](https://github.com/spotify/luigi/blob/master/LICENSE).
 - [Pathos](https://github.com/uqfoundation/pathos) under the [BSD License](https://github.com/uqfoundation/pathos/blob/master/LICENSE).
 - [scikit-learn](https://scikit-learn.org/stable/) under the [BSD License](https://github.com/scikit-learn/scikit-learn/blob/main/COPYING).

The following are also potentially used as executables like from the command line but are not statically linked to code:

 - [Docker](https://docs.docker.com/engine/) under the [Apache v2 License](https://github.com/moby/moby/blob/master/LICENSE).
 - [Docker Compose](https://docs.docker.com/compose/) under the [Apache v2 License](https://github.com/docker/compose/blob/main/LICENSE).
 - [Python 3.8](https://www.python.org/) under the [PSF License](https://docs.python.org/3/license.html).
