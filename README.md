# Renewable Resource and Power Data tool (R2PD)

Power system modeler-friendly tool for downloading and formatting wind and solar  
weather, power and forecast data.

[Install](#install) | [Use](#use) | [Uninstall](#uninstall)

## Use

R2PD was designed primarily for use on the command line. The command-line
interface is accessible through `r2pd.py`. There are three primary commands:

    - actual-power
    - forecast-power
    - weather

These commands indicate the type of resource data to be retrieved from the external  
repository. For actual-power and forecast-power, a list of nodes (id, latitude,  
longitude) and a list of corresponding generator capacities (id, capacity (MW)).
For weather data only the node information is needed. Wind and solar resource data is
available for retrieval. Internal reshaping and reformatting the data can be accomplished
by specifying the desired timeseries temporal parameters or forecast parameters.

The resource data files are downloaded to a local data cache. The cache location and size
can be specified by the user (see /library/config.ini).

Once `R2PD` is installed, the CLI can be accessed with `R2PD` and has a fully documented help
menu. For a more detailed example, please see
[demo_R2PD_applied_to_rts_gmlc.ipynb](https://github.com/Smart-DS/demos/blob/master/demo_R2PD_applied_to_rts_gmlc.ipynb).   
`R2PD-lite` is a lite-weight cli that can be used to convert downloaded .hdf5 files from   
[DRPOWER](egriddata.org) into .csv files.

## Install

```
pip install git+https://github.com/Smart-DS/R2PD.git@master
```

or

```
pip install git+https://github.com/Smart-DS/R2PD.git@v0.1.0
```

## Uninstall

```
pip uninstall R2PD
```
