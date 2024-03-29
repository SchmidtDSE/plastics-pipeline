# Cookbook
Thank you for your interest in extending our work. This page offers examples and guidance for common developer operations within this repository.

<br>
<br>

## Executing pipeline with Docker
This pipeline trains new models and generates new output data for use in downstream pipelines and tools. For this modeling, instances are generally used as available and split between train, test, and validation. One exception is the temporally displaced out of sample step whose behavior can be changed by editting `is_out_sample_candidate` in `tasks_ml.py`.

The following steps will execute the pipeline from within Docker:

 - [Install Docker](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04)
 - Build the environment: `docker build -t dse/plastics_pipeline .`
 - Run the container: `docker run -it -d --name pipeline_run dse/plastics_pipeline bash`
 - Execute the pipeline: `docker exec -it pipeline_run bash build.sh`
 - Zip the result: `docker exec -it pipeline_run zip -r pipeline.zip output`
 - Get the result: `docker cp pipeline_run:/workspace/pipeline.zip pipeline.zip`
 - Shutdown container: `docker stop pipeline_run`

The result will be placed in `pipeline.zip`. Of course, note that some algorithms considered in this pipeline include stochastic elements.

<br>

## Adding a region
In general, adding a new region requires data for the new area in the form of consumption / production by sector, end of life fate propensities, and trade (waste, goods, resin).

The first location for region information is `regions.json` which outlines a number of files and constants where new regional data needs to be added. Starting with constants, each region should contain the following:

 - `agricultureSecondary`: Secondary sectorizing constant for the agriculture sector.
 - `constructionSecondary`: Secondary sectorizing constant for the construction sector.
 - `electronicSecondary`: Secondary sectorizing constant for the electronics sector.
 - `hlsSecondary`: Secondary sectorizing constant for the household leisure sports sector.
 - `otherSecondary`: Secondary sectorizing constant for the "other" sector.
 - `packagingSecondary`: Secondary sectorizing constant for the packaging sector.
 - `textileSecondary`: Secondary sectorizing constant for the textile sector.
 - `transportationSecondary`: Secondary sectorizing constant for the transportation sector.

Constants of the form `.*Secondary` are to be calculated using the following formula as described in Section 3.3 of [sectorizing secondary material](https://global-plastics-tool.org/pdf/sectorizing_secondary_material.pdf) where $l$ is yield loss and $p$ is the probability or ratio as a number from 0 to 1 such that $p_{polymer|recyclable}$ is the probability that a polymer is recyclable in the region and $p_{polymer|sector|region}$ is the probability that a ton of plastic in a region and sector is of a given polymer:

$(1 - l) * \Sigma(p_{polymer|recyclable} * p_{polymer|sector|region})$

Additionally, data files are required for the following where XX should be replaced by an identifying number not currently used by another file (see `data` directory):

| Data File                 | Purpose                                                                        | SQL File                 |
|---------------------------|--------------------------------------------------------------------------------|--------------------------|
| XXregionenduseandtype.csv | Provide sector-level polymer ratios for a region.                              | raw_end_use_region.sql   |
| XXeolregioncopy.csv       | End of life fate ratios / propensities for the region by year.                 | raw_eol_region.sql       |
| XXnettraderegion.csv      | Amount of imports (or negative if exports) per sector and year for the region. | raw_net_trade_region.sql |

File names are stripped of non-alphanumeric characters prior to pipeline execution so underscores and dashes can be used but will be ignored. Also, as indicated, these data files also require new queries be added to `sql/02_clean_inputs` with the new region key.

Finally, data needs to be added to the following files containing multiple regions:

 - `01_Production_of_Resin_(no_fiber).csv`: Resin production data in MMT over time and per region excluding fiber data and additives. Expecting 2005 to 2020.
 - `02_Production_Fiber.csv`: Resin production in MMT over time and per region for fibers only (no additives). Expecting 2005 to 2020.
 - `03_Production_Additives.csv`: Additives in MMT over time and per region. Expecting 2005 to 2020.
 - `04_Net_Import_Resin_no_fiber_copy.csv`: Resin imports excluding fibers over time and by region where negative values indicate exports. All in MMT and expecting 2005 to 2020.
 - `05_Net_Import_Fiber_copy.csv`: Imports in fibers over time and by region where negative values indicate exports. All in MMT and expecting 2005 to 2020.
 - `06_Net_import_plastic_articles copy.csv`: Import in plastic articles over time and per region where negative values indicate exports. All in MTT and expecting 2005 to 2020.
 - `07_Net_Import_plastic_in_finished_goods_no_fiber copy.csv`: Import in goods excluding fibers over time and per region where negative values indicate exports. All in MTT and expecting 2005 to 2020.
 - `13_1950-2004 copy.csv`: Production data prior to 2005 indicating in MMT unless expressed otherwise (new data should use MMT).
 - `22_waste_trade.csv`: Waste imports over time in MMT by region.
 - `23_historic.csv`: Sector consumption data prior to 2005 indicating in MMT by year and region.
 - `a3_regions.json`: Mapping from country 3 letter ISO to tool region name. Note that this only requires entries for non-RoW countries.

After updating, run the pipeline. If using Docker, developers may need to rebuild the enviornment after editing these files prior to execution. Of course, please check the model diagnostics including error reporting (mean aboslute error or "MAE") in files like `.*_sweep.csv` (see `test.*Target` for hidden set performance). Depending on the size of values provided for the new region, this error may not be acceptable (like for countries with very small plastics activity) in certain use cases. In this case, the project suggests aggregating with additional countries.

<br>

## Changing input data
Simply edit existing files in `data`. A description of each is given as follows:

 - `01_Production_of_Resin_(no_fiber).csv`: Resin production data in MMT over time and per region excluding fiber data and additives. Expecting 2005 to 2020.
 - `02_Production_Fiber.csv`: Resin production in MMT over time and per region for fibers only (no additives). Expecting 2005 to 2020.
 - `03_Production_Additives.csv`: Additives in MMT over time and per region. Expecting 2005 to 2020.
 - `04_Net_Import_Resin_no_fiber_copy.csv`: Resin imports excluding fibers over time and by region where negative values indicate exports. All in MMT and expecting 2005 to 2020.
 - `05_Net_Import_Fiber_copy.csv`: Imports in fibers over time and by region where negative values indicate exports. All in MMT and expecting 2005 to 2020.
 - `06_Net_import_plastic_articles copy.csv`: Import in plastic articles over time and per region where negative values indicate exports. All in MTT and expecting 2005 to 2020.
 - `07_Net_Import_plastic_in_finished_goods_no_fiber copy.csv`: Import in goods excluding fibers over time and per region where negative values indicate exports. All in MTT and expecting 2005 to 2020.
 - `[08-11]_.*.csv`: Sector-level polymer ratios for a region. 
 - `12_Lifetime.*.csv`: Definitions of lifecycle distributions per sector (log normal distributions).
 - `13_1950-2004 copy.csv`: Production data prior to 2005 indicating in MMT unless expressed otherwise (new data should use MMT).
 - `[14-17]_Eol_.*.csv`: End of life fate ratios / propensities for the region by year.
 - `[18-21]_Net_Trade_.*.csv`: Amount of imports (or negative if exports) per sector and year for the region.
 - `22_waste_trade.csv`: Waste imports over time in MMT by region.
 - `23_historic.csv`: Sector consumption data prior to 2005 indicating in MMT by year and region.
 - `a1_gdp_raw.csv`: GDP projections in purchase price parity (currently from OECD).
 - `a2_population_raw.csv`: Population actuals.
 - `a3_regions.json`: Mapping from country 3 letter ISO to tool region name. Note that this only requires entries for non-RoW countries.
 - `a4_pop_projection.csv`: Population projections (currently from UN).


Note that this model takes a mass flow approach: each piece of plastic produced must be accounted for in consumption and end of life. This means that the input data do not include consumption and waste volumes but, instead, derive them from fate propensities, trade, etc. This allows us to ensure our modeling is comprehensive of all plastic produced.

<br>

## Changing projection years
Data are either "actuals" or projected. Historic actuals currently end in 2020 and projections run from 2021 to 2050. These start and end years can be modified in `const.py` using `PROJECTION_START_YEAR` and `PROJECTION_END_YEAR`. Depending on the data changing, users may also need to edit `recirculate_secondary.sql` and `tasks_norm_lifecycle_template.py`.

<br>

## Using results downstream
Note that `pipeline.zip` provides outputs but downstream components will use the "official" versions by default. To carry changes forward to downstream steps, use the following:

 - For the [GHG / polymer pipeline](https://github.com/SchmidtDSE/plastics-ghg-pipeline), a [trade_inputs](https://global-plastics-tool.org/data/trade_inputs.csv) file is used. This only needs to be replaced if using different socioeconomic projections for GDP and population. Generally, data updates and additional regions do not require modification unless using updated data from the UN or OECD.
 - For the [web application](https://github.com/SchmidtDSE/plastics-prototype), replace `data/web.csv` with `pipeline.zip/overview_ml.csv`.

These files can be replaced prior to pipeline / application exeuction. Note that these actions otherwise take place in CI / CD.

<br>

## Units
Note that column or variable suffixes "MT" and "MMT" are used interchangibly at different moments in the pipeline for historical reasons that maintain compatibility with external datasets. Both mean million metric tons. The appropriate convention depends on the reference dataset being considered.
