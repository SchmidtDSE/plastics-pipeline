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
A containerized Docker environment is available for execution. This will conduct the model sweeps and prepare the outputs required for the [front-end tool](https://github.com/SchmidtDSE/plastics-prototype). See [COOKBOOK.md](https://github.com/SchmidtDSE/plastics-pipeline/blob/main/COOKBOOK.md) for more details.

### Manual Environment
In addition to the Docker container, a manual environment can be established simply by running `pip install -r requirements.txt`. This assumes that sqlite3 is installed. Afterwards, simply run `bash build.sh`.

### Configuration
The configuration for the Luigi pipeline can be modified by providing a custom json file. See [task/job.json](https://github.com/SchmidtDSE/plastics-pipeline/blob/main/task/job.json) for an example. Note that the pipeline, by default, uses random forest even though a full sweep is conducted because that approach tends to yield better avoidance of overfitting. Parallelization can be enabled by changing the value of `workers`.

### Extension
For examples of adding new regions or updating existing data, see [COOKBOOK.md](https://github.com/SchmidtDSE/plastics-pipeline/blob/main/COOKBOOK.md).

### Snapshot database
Inputs snapshot for reproducibility is located in `data/snapshot.db`. Use of this preformatted dataset is controlled through `const.USE_PREFORMATTED` which defaults to True meaning that the included SQLite snapshot is used. For more details see the `data` directory.

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

Data and Citation
--------------------------------------------------------------------------------
Citations for data in this repository:

 - [DESA, World Population Prospects 2022 (2022).](https://population.un.org/wpp/Download)
 - [R. Geyer, J. R. Jambeck, K. L. Law, Production, use, and fate of all plastics ever made. Sci. Adv. 3, e1700782 (2017).](https://www.science.org/doi/10.1126/sciadv.1700782)
 - C. Liu, S. Hu, R. Geyer. Manuscript in Process (2024).
 - [OECD, Real GDP long-term forecast (2023).](https://doi.org/10.1787/d927bc18-en)

Our thanks to those authors and resources. Manuscript in progress data available upon request to authors.

<br>

Related Repositories
--------------------------------------------------------------------------------
See also [source code for the web-based tool](https://github.com/SchmidtDSE/plastics-prototype) running at [global-plastics-tool.org](https://global-plastics-tool.org) and [source code for the GHG pipeline](https://github.com/SchmidtDSE/plastics-ghg-pipeline).

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

Additional license information:

 - DESA. "World Population Prospects 2022." United Nations, Department of Economic and Social Affairs, Population Division, 2022. https://population.un.org/wpp/Download. [CC BY 3.0 license IGO](https://creativecommons.org/licenses/by/3.0/igo/).
 - OECD. "Real GDP Long-Term Forecast." OECD, 2023. https://doi.org/10.1787/d927bc18-en. Part of [long term baseline projections](https://stats.oecd.org/BrandedView.aspx?oecd_bv_id=eo-data-en&doi=039dc6d6-en) available under a [permissive license](https://www.oecd.org/termsandconditions/) under terms Ic which carries an acknowledgement requirement.
