import os
from .datastore import DRPower
from .powerdata import NodeCollection
from .queue import get_resource_data

cli_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                        'bin', 'r2pd.py')
