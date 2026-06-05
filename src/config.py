# -- Geographic filter --------------------------------------------------------

# Center of the 50nm search area
CENTER_LAT = 55.225000
CENTER_LON = 14.245000
RADIUS_NM  = 50.0

# -- Time window --------------------------------------------------------------

START_DATE = "2021-12-01"
END_DATE   = "2021-12-31"

# -- Vessel state filters -----------------------------------------------------

# Vessels below this speed are considered stationary (anchored/docked)
MIN_SOG_KN = 0.5

# -- GPS noise filter ---------------------------------------------------------

# Implied speed above this between consecutive pings = GPS error
MAX_SPEED_KN = 50.0

# -- Collision detection ------------------------------------------------------

# Two vessels within this distance = potential collision candidate
COLLISION_DIST_NM = 0.1  # ~185 metres

# Spatial grid cell size for bucketing
# Vessels are only compared to others in the same or adjacent grid cell
GRID_CELL_DEG = 0.005

# Time bucket size
TIME_BUCKET_MINUTES = 1

# -- AIS CSV column names -----------------------------------------------------

COL_TIMESTAMP   = "# Timestamp"
COL_MMSI        = "MMSI"
COL_LAT         = "Latitude"
COL_LON         = "Longitude"
COL_SOG         = "SOG"
COL_COG         = "COG"
COL_NAME        = "Name"
COL_SHIP_TYPE   = "Ship type"
COL_MOBILE_TYPE = "Type of mobile"
COL_NAV_STATUS  = "Navigational status"

# -- Output -------------------------------------------------------------------

OUTPUT_DIR      = "/app/output"
MAP_FILENAME    = "collision_trajectory.html"
REPORT_FILENAME = "collision_report.txt"