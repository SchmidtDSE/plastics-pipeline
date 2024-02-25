Plastics Pipeline
================================================================================
[Luigi](https://luigi.readthedocs.io/en/stable/)-based pipeline to sweep and select machine learning models for the plastics outcomes projection. This is used by [https://global-plastics-tool.org/](https://global-plastics-tool.org/).

<br>

Purpose
--------------------------------------------------------------------------------
Pipeline which executes pre-processing and machine learning tasks, working on the raw "input" data for the plastics business as usual projection model to make those projections multiple ways:

 - **Naive**: Simple polynomial curve fitting extrapoloation of past trends for trade, waste, and consumption.
 - **Curve**: Simple polynomial model that predicts trade, waste, and consumption having fit a curve against those response variables using population and GDP as input.
 - **ML**: A more sophisticated machine learning sweep which considers SVR, CART / trees, AdaBoost, and Random Forest.

In practice, the machine learning branch is used by the tool.

<br>

Usage
--------------------------------------------------------------------------------
Most users can simply reference the output from the latest execution. That output is written to [https://global-plastics-tool.org/datapipeline.zip](https://global-plastics-tool.org/datapipeline.zip) and is publicly available under the [CC-BY-NC License](https://github.com/SchmidtDSE/plastics-pipeline/blob/main/LICENSE.md). That said, users may also leverage a local environment if desired.

### Container Environment
A containerized Docker environment is available for execution:

 - [Install Docker](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04)
 - Build the environment: `docker build -t dse/plastics_pipeline .`
 - Run the container: `docker run -it --name pipeline_run dse/plastics_pipeline bash build.sh`

This will conduct the model sweeps and prepare the outputs required for the [front-end tool](https://github.com/SchmidtDSE/plastics-prototype).

### Manual Environment
In addition to the Docker container, a manual environment can be established simply by running `pip install -r requirements.txt`. This assumes that sqlite3 is installed. Afterwards, simply run `bash build.sh`.

### Configuration
The configuration for the Luigi pipeline can be modified by providing a custom json file. See [task/job.json](https://github.com/SchmidtDSE/plastics-pipeline/blob/main/task/job.json) for an example. Note that the pipeline, by default, uses random forest even though a full sweep is conducted because that approach tends to yield better avoidance of overfitting. Parallelization can be enabled by changing the value of `workers`.

<br>

Tool
--------------------------------------------------------------------------------
Note that an interactive tool for this model is also available at [https://github.com/SchmidtDSE/plastics-prototype](https://github.com/SchmidtDSE/plastics-prototype).

<br>

Local Environment
--------------------------------------------------------------------------------
Setup the local environment with `pip -r requirements.txt`.

<br>

Testing
--------------------------------------------------------------------------------
Some unit tests and other automated checks are available. The following is recommended:

```
$ pip install pycodestyle pyflakes nose2
$ pyflakes *.py
$ pycodestyle *.py
$ nose2
```

Note that unit tests and code quality checks are run in CI / CD.

<br>

Deployment
--------------------------------------------------------------------------------
This pipeline can be deployed by merging to the `deploy` branch of the repository, firing GitHub actions. This will cause the pipeline output files to be written to [https://global-plastics-tool.org/datapipeline.zip](https://global-plastics-tool.org/datapipeline.zip).

<br>

Development Standards
--------------------------------------------------------------------------------
CI / CD should be passing before merges to `main` which is used to stage pipeline deployments and `deploy`. Where possible, please follow the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html). Please note that tests run as part of the pipeline itself and separate test files are encouraged but not required. That said, developers should document which tasks are tests and expand these tests like typical unit tests as needed in the future. We allow lines to go to 100 characters.

<br>

Publication
--------------------------------------------------------------------------------
Papers are still in process. Please cite preprint at [10.48550/arXiv.2312.11359](https://arxiv.org/abs/2312.11359) for now. Thank you!

<br>

Open Source
--------------------------------------------------------------------------------
This project is released as open source (BSD and CC-BY-NC). See [LICENSE.md](https://github.com/SchmidtDSE/plastics-pipeline/blob/main/LICENSE.md) for further details. In addition to this, please note that this project uses the following open source:

 - [Luigi](https://luigi.readthedocs.io/en/stable/index.html) under the [Apache v2 License](https://github.com/spotify/luigi/blob/master/LICENSE).
 - [Pathos](https://github.com/uqfoundation/pathos) under the [BSD License](https://github.com/uqfoundation/pathos/blob/master/LICENSE).
 - [scikit-learn](https://scikit-learn.org/stable/) under the [BSD License](https://github.com/scikit-learn/scikit-learn/blob/main/COPYING).
 - [scipy](https://scipy.org/) under the [BSD License](https://github.com/scipy/scipy/blob/main/LICENSE.txt).

The following are also potentially used as executables like from the command line but are not statically linked to code:

 - [Docker](https://docs.docker.com/engine/) under the [Apache v2 License](https://github.com/moby/moby/blob/master/LICENSE).
 - [Python 3.8](https://www.python.org/) under the [PSF License](https://docs.python.org/3/license.html).
 - [SQLite 3](https://www.sqlite.org/index.html) which is in the [public domain](https://www.sqlite.org/copyright.html).
