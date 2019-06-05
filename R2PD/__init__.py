"""
R2PD package entry point
"""
import os
from R2PD.datastore import DRPower
from R2PD.powerdata import NodeCollection
from R2PD.tshelpers import TemporalParameters
cli_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                        'bin', 'r2pd.py')

META_ROOT = os.path.dirname(os.path.realpath(__file__))
META_ROOT = os.path.join(META_ROOT, 'library')
SOLAR_META_PATH = os.path.join(META_ROOT, 'solar_site_meta.json')
WIND_META_PATH = os.path.join(META_ROOT, 'wind_site_meta.json')
