# Climate Data for the Environment services client (CliDEsc): Common Utilities

Code repository for CliDEsc's common helper Python package

## Description

**CliDEsc** is a web-based open source platform containing tools that allow end users to request data and products to be generated from the **CLIDE** ([CLImate Data for the Environment](http://www.bom.gov.au/climate/pacific/about-clide.shtml)) database.


[CLIDE](http://www.bom.gov.au/climate/pacific/about-clide.shtml) is a PostGRES database dedicated to the storage and management of hydroclimatic data. CLIDE was originally developed by the [Australian Bureau of Meteorology](http://www.bom.gov.au/) and is used in many countries in the southwest Pacific region.

Climate data analysis and visualisation tools (**climate services**) in CliDEsc are developed in [R](http://www.r-project.org/) and [Python](https://www.python.org/).
This repository contains the **Python component of CliDEsc**, including a module (`clide.py`) that provides the interface to the CLIDE database, utility functions to perform common tasks (e.g., calculate monthly statistics from daily or sub-daily data), and self-contained example Python scripts that implement climate services.


## Installation

### For Users (pip install)

To install the package and all dependencies, run the following command from the root of the repository:

```bash
pip install .
```

This will install the package as a standard Python package and make it available for import in your environment.

### For Developers (editable/development install)

If you want to make changes to the code and have them reflected immediately, use:

```bash
pip install -e .
```

### Requirements

- Python 3.7+
- pandas
- SQLAlchemy
- numpy

All required Python libraries are listed in `requirements.txt`. You can install them manually with:

```bash
pip install -r requirements.txt
```

You may not need the Sphinx documentation libraries so please modifiy the `requirements.txt` file for your needs.

### Manual Script Copy (legacy/server install)

On a typical installation of a clide / clidesc server, the `clide.py` script may be copied to the `./assets/generators/common/` directory (i.e., the same place where the `clidesc.r` source resides). However, using `pip install .` is the recommended approach for modern Python environments.

## Quick Start

```python
import clidesc.clide as clide
from sqlalchemy import create_engine

# Example: Connect to CLIDE database and get station metadata
engine = create_engine('postgresql://user:password@host:port/dbname')
conn = engine.connect()
stations_df = clide.stations(conn)
print(stations_df.head())
```
## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Documentation and Support 

Web documentation can be accessed at the [CliDEsc Common Library Documentation](https://niwa.github.io/clidesc-common/) site.

Questions and comments should be addressed to [James Sturman](mailto:james.sturman@niwa.co.nz), [Hannah Marley](mailto:hannah.marley@niwa.co.nz) or [Luke Wang](mailto:luke.wang@niwa.co.nz)

## Authors and acknowledgment

The development of the core CliDEsc system has been primarily funded by The Earth Sciences New Zealand. CliDEsc Product Generators have been developed and customised in collaboration with Pacific Island Meteorological and Hydrological Services, under projects funded by the Global Environment Facility (GEF) in collaboration with United Nations Development Programme, the World Bank Global Facility for Disaster Reduction and Recovery (GFDRR) Challenge Fund, and the New Zealand Ministry of Foreign Affairs and Trade (MFAT).


      

