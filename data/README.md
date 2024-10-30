# Citations
Citations for data in this repository:

 - [DESA, World Population Prospects 2022 (2022).](https://population.un.org/wpp/Download). [Data export link for population](https://population.un.org/wpp/Download/Files/1_Indicator%2520%5C(Standard%5C)/EXCEL_FILES/1_General/WPP2024_GEN_F01_DEMOGRAPHIC_INDICATORS_COMPACT.xlsx).
 - [R. Geyer, J. R. Jambeck, K. L. Law, Production, use, and fate of all plastics ever made. Sci. Adv. 3, e1700782 (2017).](https://www.science.org/doi/10.1126/sciadv.1700782)
 - C. Liu, S. Hu, R. Geyer. Manuscript in Process (2024).
 - [OECD, Real GDP long-term forecast (2023).](https://doi.org/10.1787/d927bc18-en). [Data export link for OECD](https://sdmx.oecd.org/archive/rest/data/OECD,DF_EO114_LTB,/.GDPVD.S0.A?startPeriod=1990&endPeriod=2060&dimensionAtObservation=AllDimensions&format=csvfilewithlabels).

Data may be requested from these authors or found at the links above. 

# Preformatted sqlite
Data for reproducibility are provided in SQLite format (`snapshot.db`). This file is used by default as `const.USE_PREFORMATTED` is set to True. Note that the SQLite database included starts at 2000 but the simulations may also optionally start from 1950 if `const.USE_PREFORMATTED` is set to False which enables using raw upstream inputs. This requires pulling the larger original upstream data as described above. Setting `const.USE_PREFORMATTED` to False results in minor improvements in accuracy at expense of increased computational cost. That said, these improvements to accuracy are generally negligible relative to predicted Monte Carlo distributions given simulation uncertainty estimations expected regardless of `const.USE_PREFORMATTED`. Starting at 2000 minimizes the dataset subset with limited contextual information.
