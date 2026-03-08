"""Package providing ETL job modules for each data layer.

This namespace aggregates bronze, silver and gold job subpackages used
for orchestrating layer-specific data processing tasks.
"""

from . import bronze as bronze
from . import gold as gold
from . import silver as silver
