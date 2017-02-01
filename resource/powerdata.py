"""
This module transforms wind and solar PV resource data into forms usable by
power system modelers. Functionalities needed include:

Must have:
- Select raw resource data (wind speeds, irradiance) or power outputs
- Scale power timeseries to the desired capacity
- Provide data for the temporal extents and resolutions desired, 
  expressed in units of a user-chosen standard-time timezone
- Blend multiple resource data timeseries into composite curves for 
  distributed PV based on a default or user-supplied distribution of 
  orientations
- Reshape forecast data into forms usable by operation simulators

Nice to have:
- Sum multiple timeseries to represent the combined output over larger areas
  all tied into the same node
"""


