import os

# =========================
# RAW / BRONZE / SILVER / GOLD BASE PATHS
# =========================

RAW_EVENTS_PATH = os.getenv(
    "RAW_EVENTS_PATH",
    "data/sample/events.json"   
)

BRONZE_BASE_PATH = os.getenv(
    "BRONZE_BASE_PATH",
    "data/out/bronze"
)

SILVER_BASE_PATH = os.getenv(
    "SILVER_BASE_PATH",
    "data/out/silver"
)

GOLD_BASE_PATH = os.getenv(
    "GOLD_BASE_PATH",
    "data/out/gold"
)

# =========================
# DERIVED SILVER PATHS
# =========================

SILVER_EVENTS_PATH = f"{SILVER_BASE_PATH}/events"
DIM_USER_PATH     = f"{SILVER_BASE_PATH}/dim_user"
DIM_VIDEO_PATH    = f"{SILVER_BASE_PATH}/dim_video"
DIM_SESSION_PATH  = f"{SILVER_BASE_PATH}/dim_session"
FACT_EVENTS_PATH  = f"{SILVER_BASE_PATH}/fact_events"

# =========================
# DERIVED GOLD PATHS
# =========================

GOLD_DAU_PATH       = f"{GOLD_BASE_PATH}/dau"
GOLD_FUNNEL_PATH    = f"{GOLD_BASE_PATH}/funnel"
