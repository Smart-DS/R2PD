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

class Node(object): 
    def assign_resource(self, resource_data): pass

class GeneratorNode(Node): 
    
    def get_power(self, timezone=None): pass

    def get_forecast(self, forecast_params, timezone=None): pass

    # not sure how to handle different formats. some might want to put 
    # power and forecasts in the same file? this api doesn't seem abstract 
    # enough yet.
    def save_power(self, filename, formatter=None): pass

    def save_forecast(self, filename, formatter=None): pass

class WindGeneratorNode(GeneratorNode): pass

class SolarPVGeneratorNode(GeneratorNode): pass

class WeatherNode(Node): pass

