"""
Shared data definitions for Tasty Bytes Consulting demo.

Tasty Bytes Consulting is a fictional wealth management and financial
analytics firm that manages ~$2.5B in assets across institutional clients,
family offices, and high-net-worth individuals.

Usage
-----
    import sys; sys.path.insert(0, '.')
    from demo_data import create_spark_session, load_all

    spark = create_spark_session()
    data  = load_all(spark)

    clients_df      = data["clients"]
    assets_df       = data["assets"]
    portfolios_df   = data["portfolios"]
    transactions_df = data["transactions"]
    prices_df       = data["prices"]
    fx_rates_df     = data["fx_rates"]
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType, BooleanType,
)

COMPANY_NAME = "Tasty Bytes Consulting"
DEMO_TAG = "[DEMO DATA — Tasty Bytes Consulting]"


# ---------------------------------------------------------------------------
# SparkSession factory
# ---------------------------------------------------------------------------

def create_spark_session(app_name: str = "TastyBytesConsultingDemo") -> SparkSession:
    """Return a local SparkSession configured for demo use."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# dim_clients  — investment clients
# ---------------------------------------------------------------------------

CLIENTS_SCHEMA = StructType([
    StructField("client_id",           StringType(),  False),
    StructField("client_name",         StringType(),  True),
    StructField("client_type",         StringType(),  True),  # institutional | family_office | individual
    StructField("risk_profile",        StringType(),  True),  # conservative | moderate | aggressive
    StructField("region",              StringType(),  True),  # north_america | europe | apac
    StructField("relationship_manager",StringType(),  True),
    StructField("aum_usd",             DoubleType(),  True),  # assets under management
    StructField("onboarded_date",      StringType(),  True),
    StructField("is_active",           BooleanType(), True),
])

CLIENTS_DATA = [
    ("CLT-001", "Apex Pension Fund",              "institutional",  "moderate",     "north_america", "Sarah Chen",       420_000_000.0, "2018-03-15", True),
    ("CLT-002", "Sterling Endowment Trust",       "institutional",  "conservative", "north_america", "James Whitfield",  285_000_000.0, "2016-07-01", True),
    ("CLT-003", "Marcus Harrington",              "individual",     "aggressive",   "north_america", "Sarah Chen",        12_500_000.0, "2021-11-20", True),
    ("CLT-004", "Blackwood Family Office",        "family_office",  "moderate",     "europe",        "Helena Brandt",    180_000_000.0, "2019-05-08", True),
    ("CLT-005", "Pacific Growth Corp",            "institutional",  "aggressive",   "apac",          "David Lim",        310_000_000.0, "2017-02-14", True),
    ("CLT-006", "Eleanor Voss",                   "individual",     "conservative", "europe",         "Helena Brandt",     8_750_000.0, "2022-04-03", True),
    ("CLT-007", "Nordic Wealth Partners",         "institutional",  "moderate",     "europe",        "Helena Brandt",    195_000_000.0, "2015-09-22", True),
    ("CLT-008", "Chen Capital Group",             "family_office",  "aggressive",   "apac",          "David Lim",         95_000_000.0, "2020-08-11", True),
    ("CLT-009", "Riverside Healthcare Foundation","institutional",  "conservative", "north_america", "James Whitfield",  225_000_000.0, "2014-01-30", True),
    ("CLT-010", "Davidson & Sons Trust",          "family_office",  "moderate",     "north_america", "Sarah Chen",        75_000_000.0, "2023-06-19", True),
]

def create_clients_df(spark: SparkSession) -> DataFrame:
    """Dimension table of Tasty Bytes Consulting clients."""
    return spark.createDataFrame(CLIENTS_DATA, schema=CLIENTS_SCHEMA)


# ---------------------------------------------------------------------------
# dim_assets  — investable instruments
# ---------------------------------------------------------------------------

ASSETS_SCHEMA = StructType([
    StructField("asset_id",    StringType(), False),
    StructField("ticker",      StringType(), True),
    StructField("asset_name",  StringType(), True),
    StructField("asset_class", StringType(), True),  # equity | fixed_income | alternative | cash
    StructField("sector",      StringType(), True),
    StructField("currency",    StringType(), True),
    StructField("exchange",    StringType(), True),
    StructField("domicile",    StringType(), True),
])

ASSETS_DATA = [
    # Equities — USD
    ("AST-001", "AAPL",    "Apple Inc",              "equity",        "technology",    "USD", "NASDAQ", "US"),
    ("AST-002", "JPM",     "JPMorgan Chase & Co",    "equity",        "financials",    "USD", "NYSE",   "US"),
    ("AST-003", "UNH",     "UnitedHealth Group",     "equity",        "healthcare",    "USD", "NYSE",   "US"),
    ("AST-004", "MSFT",    "Microsoft Corporation",  "equity",        "technology",    "USD", "NASDAQ", "US"),
    # Equities — EUR / GBP
    ("AST-005", "SAP",     "SAP SE",                 "equity",        "technology",    "EUR", "XETRA",  "DE"),
    ("AST-006", "HSBA",    "HSBC Holdings",          "equity",        "financials",    "GBP", "LSE",    "GB"),
    # Fixed Income
    ("AST-007", "UST10Y",  "US 10Y Treasury",        "fixed_income",  "government",    "USD", "OTC",    "US"),
    ("AST-008", "EURBT5Y", "EUR 5Y Bund",            "fixed_income",  "government",    "EUR", "OTC",    "DE"),
    ("AST-009", "CORPIG",  "IG Corp Bond ETF",       "fixed_income",  "corporate",     "USD", "NYSE",   "US"),
    # Alternatives
    ("AST-010", "GOLD",    "Gold Spot",              "alternative",   "commodity",     "USD", "OTC",    "US"),
    ("AST-011", "REIT_US", "US REIT Index ETF",      "alternative",   "real_estate",   "USD", "NYSE",   "US"),
    # Cash
    ("AST-012", "CASH_USD","USD Cash",               "cash",          "cash",          "USD", "N/A",    "US"),
    ("AST-013", "CASH_EUR","EUR Cash",               "cash",          "cash",          "EUR", "N/A",    "EU"),
]

def create_assets_df(spark: SparkSession) -> DataFrame:
    """Dimension table of investable assets managed by Tasty Bytes."""
    return spark.createDataFrame(ASSETS_DATA, schema=ASSETS_SCHEMA)


# ---------------------------------------------------------------------------
# dim_portfolios  — client portfolios
# ---------------------------------------------------------------------------

PORTFOLIOS_SCHEMA = StructType([
    StructField("portfolio_id",    StringType(), False),
    StructField("client_id",       StringType(), True),
    StructField("portfolio_name",  StringType(), True),
    StructField("strategy",        StringType(), True),  # growth | income | balanced | capital_preservation
    StructField("benchmark",       StringType(), True),
    StructField("base_currency",   StringType(), True),
    StructField("inception_date",  StringType(), True),
    StructField("nav_usd",         DoubleType(), True),  # net asset value at last valuation
    StructField("is_active",       BooleanType(),True),
])

PORTFOLIOS_DATA = [
    ("PRT-001", "CLT-001", "Apex Growth Equity",          "growth",               "S&P 500",          "USD", "2018-06-01",  145_000_000.0, True),
    ("PRT-002", "CLT-001", "Apex Fixed Income",           "income",               "Bloomberg US Agg", "USD", "2018-06-01",  275_000_000.0, True),
    ("PRT-003", "CLT-002", "Sterling Conservative Core",  "capital_preservation", "60/40 Blend",      "USD", "2016-09-15",  285_000_000.0, True),
    ("PRT-004", "CLT-003", "Harrington High Growth",      "growth",               "Russell 2000",     "USD", "2021-12-01",   12_500_000.0, True),
    ("PRT-005", "CLT-004", "Blackwood Balanced",          "balanced",             "60/40 Blend",      "EUR", "2019-07-01",   95_000_000.0, True),
    ("PRT-006", "CLT-004", "Blackwood Alternatives",      "growth",               "HFRI Fund Wtd",    "USD", "2020-01-15",   85_000_000.0, True),
    ("PRT-007", "CLT-005", "Pacific Equity Asia",         "growth",               "MSCI EM",          "USD", "2017-04-01",  310_000_000.0, True),
    ("PRT-008", "CLT-006", "Voss Capital Preservation",   "capital_preservation", "EUR 3M EURIBOR",   "EUR", "2022-05-01",    8_750_000.0, True),
    ("PRT-009", "CLT-007", "Nordic Balanced",             "balanced",             "60/40 Blend",      "EUR", "2015-11-01",  195_000_000.0, True),
    ("PRT-010", "CLT-008", "Chen Aggressive Growth",      "growth",               "MSCI World",       "USD", "2020-10-01",   95_000_000.0, True),
    ("PRT-011", "CLT-009", "Riverside Income",            "income",               "Bloomberg US Agg", "USD", "2014-03-01",  225_000_000.0, True),
    ("PRT-012", "CLT-010", "Davidson Balanced Trust",     "balanced",             "60/40 Blend",      "USD", "2023-07-01",   75_000_000.0, True),
]

def create_portfolios_df(spark: SparkSession) -> DataFrame:
    """Dimension table of investment portfolios."""
    return spark.createDataFrame(PORTFOLIOS_DATA, schema=PORTFOLIOS_SCHEMA)


# ---------------------------------------------------------------------------
# fct_transactions  — trade and income events
# ---------------------------------------------------------------------------

TRANSACTIONS_SCHEMA = StructType([
    StructField("txn_id",          StringType(), False),
    StructField("portfolio_id",    StringType(), True),
    StructField("asset_id",        StringType(), True),
    StructField("txn_type",        StringType(), True),  # buy | sell | dividend | fee
    StructField("quantity",        DoubleType(), True),
    StructField("price",           DoubleType(), True),
    StructField("currency",        StringType(), True),
    StructField("fx_rate_to_usd",  DoubleType(), True),  # 1.0 for USD assets
    StructField("txn_date",        StringType(), True),
    StructField("settlement_date", StringType(), True),
    StructField("broker",          StringType(), True),
])

TRANSACTIONS_DATA = [
    # PRT-001 (Apex Growth Equity — equities)
    ("TXN-0001","PRT-001","AST-001","buy",     500.0,  182.50,"USD",1.0000,"2024-01-08","2024-01-10","Goldman Sachs"),
    ("TXN-0002","PRT-001","AST-004","buy",     300.0,  374.00,"USD",1.0000,"2024-01-08","2024-01-10","Goldman Sachs"),
    ("TXN-0003","PRT-001","AST-002","buy",     200.0,  167.30,"USD",1.0000,"2024-01-15","2024-01-17","Morgan Stanley"),
    ("TXN-0004","PRT-001","AST-001","buy",     250.0,  191.25,"USD",1.0000,"2024-02-05","2024-02-07","Goldman Sachs"),
    ("TXN-0005","PRT-001","AST-004","sell",    100.0,  402.10,"USD",1.0000,"2024-03-12","2024-03-14","Goldman Sachs"),
    ("TXN-0006","PRT-001","AST-002","dividend",  0.0,    1.05,"USD",1.0000,"2024-03-31","2024-04-02","Morgan Stanley"),
    ("TXN-0007","PRT-001","AST-003","buy",     150.0,  528.00,"USD",1.0000,"2024-04-03","2024-04-05","Morgan Stanley"),
    ("TXN-0008","PRT-001","AST-001","sell",    100.0,  218.75,"USD",1.0000,"2024-05-20","2024-05-22","Goldman Sachs"),
    ("TXN-0009","PRT-001","AST-012","buy",       1.0,50000.00,"USD",1.0000,"2024-06-01","2024-06-01","Internal"),
    ("TXN-0010","PRT-001","AST-001","buy",     200.0,  209.50,"USD",1.0000,"2024-07-10","2024-07-12","Goldman Sachs"),
    ("TXN-0011","PRT-001","AST-004","buy",     150.0,  435.20,"USD",1.0000,"2024-07-10","2024-07-12","Goldman Sachs"),
    ("TXN-0012","PRT-001","AST-001","fee",       0.0,  -125.00,"USD",1.0000,"2024-09-30","2024-09-30","Internal"),
    # PRT-002 (Apex Fixed Income)
    ("TXN-0013","PRT-002","AST-007","buy",    5000.0,   98.20,"USD",1.0000,"2024-01-10","2024-01-12","Barclays"),
    ("TXN-0014","PRT-002","AST-009","buy",    2500.0,  105.60,"USD",1.0000,"2024-01-10","2024-01-12","Barclays"),
    ("TXN-0015","PRT-002","AST-007","buy",    2000.0,   97.50,"USD",1.0000,"2024-02-14","2024-02-16","Barclays"),
    ("TXN-0016","PRT-002","AST-009","dividend",  0.0,    2.25,"USD",1.0000,"2024-03-31","2024-04-02","Barclays"),
    ("TXN-0017","PRT-002","AST-009","buy",    1500.0,  104.80,"USD",1.0000,"2024-04-18","2024-04-20","Barclays"),
    ("TXN-0018","PRT-002","AST-007","sell",   1000.0,   99.10,"USD",1.0000,"2024-06-20","2024-06-22","Barclays"),
    ("TXN-0019","PRT-002","AST-012","buy",       1.0,100000.00,"USD",1.0000,"2024-07-01","2024-07-01","Internal"),
    ("TXN-0020","PRT-002","AST-002","fee",       0.0,  -200.00,"USD",1.0000,"2024-09-30","2024-09-30","Internal"),
    # PRT-004 (Harrington High Growth)
    ("TXN-0021","PRT-004","AST-001","buy",      50.0,  185.00,"USD",1.0000,"2024-01-22","2024-01-24","Interactive Brokers"),
    ("TXN-0022","PRT-004","AST-004","buy",      30.0,  378.50,"USD",1.0000,"2024-01-22","2024-01-24","Interactive Brokers"),
    ("TXN-0023","PRT-004","AST-010","buy",     100.0, 2042.00,"USD",1.0000,"2024-02-01","2024-02-03","Interactive Brokers"),
    ("TXN-0024","PRT-004","AST-004","buy",      20.0,  415.30,"USD",1.0000,"2024-04-15","2024-04-17","Interactive Brokers"),
    ("TXN-0025","PRT-004","AST-001","sell",     10.0,  212.00,"USD",1.0000,"2024-05-30","2024-06-01","Interactive Brokers"),
    ("TXN-0026","PRT-004","AST-010","buy",      50.0, 2350.00,"USD",1.0000,"2024-07-25","2024-07-27","Interactive Brokers"),
    # PRT-005 (Blackwood Balanced — EUR base)
    ("TXN-0027","PRT-005","AST-005","buy",     300.0,  172.40,"EUR",1.0820,"2024-01-18","2024-01-20","Deutsche Bank"),
    ("TXN-0028","PRT-005","AST-006","buy",    1000.0,    6.48,"GBP",1.2650,"2024-01-18","2024-01-20","Deutsche Bank"),
    ("TXN-0029","PRT-005","AST-007","buy",    1000.0,   98.20,"USD",0.9260,"2024-02-08","2024-02-10","Deutsche Bank"),
    ("TXN-0030","PRT-005","AST-013","buy",       1.0,80000.00,"EUR",1.0820,"2024-02-08","2024-02-08","Internal"),
    ("TXN-0031","PRT-005","AST-005","buy",     200.0,  178.60,"EUR",1.0790,"2024-04-22","2024-04-24","Deutsche Bank"),
    ("TXN-0032","PRT-005","AST-006","dividend",  0.0,    0.32,"GBP",1.2680,"2024-06-28","2024-06-30","Deutsche Bank"),
    ("TXN-0033","PRT-005","AST-007","sell",    500.0,   99.00,"USD",0.9310,"2024-07-08","2024-07-10","Deutsche Bank"),
    ("TXN-0034","PRT-005","AST-005","fee",       0.0, -180.00,"EUR",1.0820,"2024-09-30","2024-09-30","Internal"),
    # PRT-007 (Pacific Equity Asia)
    ("TXN-0035","PRT-007","AST-001","buy",    1000.0,  188.00,"USD",1.0000,"2024-01-05","2024-01-07","JP Morgan"),
    ("TXN-0036","PRT-007","AST-004","buy",     600.0,  380.00,"USD",1.0000,"2024-01-05","2024-01-07","JP Morgan"),
    ("TXN-0037","PRT-007","AST-002","buy",     800.0,  169.50,"USD",1.0000,"2024-02-20","2024-02-22","JP Morgan"),
    ("TXN-0038","PRT-007","AST-003","buy",     400.0,  520.00,"USD",1.0000,"2024-03-05","2024-03-07","JP Morgan"),
    ("TXN-0039","PRT-007","AST-001","sell",    200.0,  215.00,"USD",1.0000,"2024-05-15","2024-05-17","JP Morgan"),
    ("TXN-0040","PRT-007","AST-004","sell",    100.0,  430.00,"USD",1.0000,"2024-06-10","2024-06-12","JP Morgan"),
    ("TXN-0041","PRT-007","AST-002","dividend",  0.0,    1.05,"USD",1.0000,"2024-06-30","2024-07-02","JP Morgan"),
    ("TXN-0042","PRT-007","AST-001","buy",     300.0,  220.00,"USD",1.0000,"2024-08-12","2024-08-14","JP Morgan"),
    ("TXN-0043","PRT-007","AST-011","buy",     500.0,   92.30,"USD",1.0000,"2024-08-12","2024-08-14","JP Morgan"),
    ("TXN-0044","PRT-007","AST-012","buy",       1.0,500000.00,"USD",1.0000,"2024-09-01","2024-09-01","Internal"),
    ("TXN-0045","PRT-007","AST-004","buy",     200.0,  440.00,"USD",1.0000,"2024-09-18","2024-09-20","JP Morgan"),
    ("TXN-0046","PRT-007","AST-001","fee",       0.0, -500.00,"USD",1.0000,"2024-09-30","2024-09-30","Internal"),
    # PRT-009 (Nordic Balanced — EUR)
    ("TXN-0047","PRT-009","AST-005","buy",     600.0,  174.00,"EUR",1.0800,"2024-01-12","2024-01-14","Nordea"),
    ("TXN-0048","PRT-009","AST-008","buy",    2000.0,   97.80,"EUR",1.0800,"2024-01-12","2024-01-14","Nordea"),
    ("TXN-0049","PRT-009","AST-006","buy",    2000.0,    6.52,"GBP",1.2640,"2024-02-25","2024-02-27","Nordea"),
    ("TXN-0050","PRT-009","AST-010","buy",     200.0, 2050.00,"USD",0.9260,"2024-03-15","2024-03-17","Nordea"),
    ("TXN-0051","PRT-009","AST-005","sell",    100.0,  181.20,"EUR",1.0810,"2024-05-08","2024-05-10","Nordea"),
    ("TXN-0052","PRT-009","AST-008","buy",    1000.0,   98.50,"EUR",1.0780,"2024-06-03","2024-06-05","Nordea"),
    ("TXN-0053","PRT-009","AST-005","buy",     300.0,  186.00,"EUR",1.0820,"2024-08-22","2024-08-24","Nordea"),
    ("TXN-0054","PRT-009","AST-013","buy",       1.0,120000.00,"EUR",1.0820,"2024-09-01","2024-09-01","Internal"),
    ("TXN-0055","PRT-009","AST-005","fee",       0.0, -220.00,"EUR",1.0820,"2024-09-30","2024-09-30","Internal"),
    # PRT-011 (Riverside Income — fixed income heavy)
    ("TXN-0056","PRT-011","AST-007","buy",    8000.0,   98.00,"USD",1.0000,"2024-01-03","2024-01-05","Citigroup"),
    ("TXN-0057","PRT-011","AST-009","buy",    4000.0,  105.20,"USD",1.0000,"2024-01-03","2024-01-05","Citigroup"),
    ("TXN-0058","PRT-011","AST-007","buy",    4000.0,   97.40,"USD",1.0000,"2024-03-20","2024-03-22","Citigroup"),
    ("TXN-0059","PRT-011","AST-009","dividend",  0.0,    2.25,"USD",1.0000,"2024-03-31","2024-04-02","Citigroup"),
    ("TXN-0060","PRT-011","AST-007","dividend",  0.0,    2.10,"USD",1.0000,"2024-06-30","2024-07-02","Citigroup"),
    ("TXN-0061","PRT-011","AST-009","buy",    2000.0,  104.50,"USD",1.0000,"2024-07-15","2024-07-17","Citigroup"),
    ("TXN-0062","PRT-011","AST-012","buy",       1.0,200000.00,"USD",1.0000,"2024-08-01","2024-08-01","Internal"),
    ("TXN-0063","PRT-011","AST-007","fee",       0.0, -300.00,"USD",1.0000,"2024-09-30","2024-09-30","Internal"),
    # PRT-010 (Chen Aggressive Growth)
    ("TXN-0064","PRT-010","AST-001","buy",     200.0,  189.00,"USD",1.0000,"2024-01-30","2024-02-01","UBS"),
    ("TXN-0065","PRT-010","AST-004","buy",     120.0,  390.00,"USD",1.0000,"2024-01-30","2024-02-01","UBS"),
    ("TXN-0066","PRT-010","AST-010","buy",     150.0, 2035.00,"USD",1.0000,"2024-02-20","2024-02-22","UBS"),
    ("TXN-0067","PRT-010","AST-011","buy",     400.0,   90.50,"USD",1.0000,"2024-03-10","2024-03-12","UBS"),
    ("TXN-0068","PRT-010","AST-004","sell",     50.0,  425.00,"USD",1.0000,"2024-05-22","2024-05-24","UBS"),
    ("TXN-0069","PRT-010","AST-001","buy",     100.0,  213.00,"USD",1.0000,"2024-07-01","2024-07-03","UBS"),
    ("TXN-0070","PRT-010","AST-010","buy",      75.0, 2345.00,"USD",1.0000,"2024-08-05","2024-08-07","UBS"),
    ("TXN-0071","PRT-010","AST-001","fee",       0.0, -150.00,"USD",1.0000,"2024-09-30","2024-09-30","Internal"),
    # PRT-012 (Davidson Balanced Trust)
    ("TXN-0072","PRT-012","AST-001","buy",     100.0,  219.00,"USD",1.0000,"2023-07-15","2023-07-17","Wells Fargo"),
    ("TXN-0073","PRT-012","AST-007","buy",    2000.0,   97.20,"USD",1.0000,"2023-07-15","2023-07-17","Wells Fargo"),
    ("TXN-0074","PRT-012","AST-011","buy",     300.0,   88.40,"USD",1.0000,"2023-08-10","2023-08-12","Wells Fargo"),
    ("TXN-0075","PRT-012","AST-002","buy",     150.0,  155.20,"USD",1.0000,"2024-01-25","2024-01-27","Wells Fargo"),
    ("TXN-0076","PRT-012","AST-007","dividend",  0.0,    2.10,"USD",1.0000,"2024-03-31","2024-04-02","Wells Fargo"),
    ("TXN-0077","PRT-012","AST-001","sell",     25.0,  210.50,"USD",1.0000,"2024-05-10","2024-05-12","Wells Fargo"),
    ("TXN-0078","PRT-012","AST-002","buy",      75.0,  198.30,"USD",1.0000,"2024-07-20","2024-07-22","Wells Fargo"),
    ("TXN-0079","PRT-012","AST-012","buy",       1.0, 50000.00,"USD",1.0000,"2024-09-01","2024-09-01","Internal"),
    ("TXN-0080","PRT-012","AST-001","fee",       0.0, -100.00,"USD",1.0000,"2024-09-30","2024-09-30","Internal"),
]

def create_transactions_df(spark: SparkSession) -> DataFrame:
    """Fact table of trade and income events across Tasty Bytes' portfolios."""
    return spark.createDataFrame(TRANSACTIONS_DATA, schema=TRANSACTIONS_SCHEMA)


# ---------------------------------------------------------------------------
# fct_daily_prices  — end-of-month close prices (simplified)
# ---------------------------------------------------------------------------

PRICES_SCHEMA = StructType([
    StructField("asset_id",    StringType(), False),
    StructField("price_date",  StringType(), True),
    StructField("open_price",  DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("currency",    StringType(), True),
])

PRICES_DATA = [
    # AAPL (AST-001)
    ("AST-001","2024-01-31",184.92,186.50,"USD"),("AST-001","2024-02-29",181.20,181.56,"USD"),
    ("AST-001","2024-03-28",171.20,171.48,"USD"),("AST-001","2024-04-30",170.35,170.33,"USD"),
    ("AST-001","2024-05-31",192.25,192.46,"USD"),("AST-001","2024-06-28",210.50,210.62,"USD"),
    ("AST-001","2024-07-31",218.80,218.24,"USD"),("AST-001","2024-08-30",226.50,226.84,"USD"),
    ("AST-001","2024-09-30",233.60,233.00,"USD"),
    # MSFT (AST-004)
    ("AST-004","2024-01-31",398.60,397.58,"USD"),("AST-004","2024-02-29",415.40,415.32,"USD"),
    ("AST-004","2024-03-28",421.80,420.54,"USD"),("AST-004","2024-04-30",395.50,395.00,"USD"),
    ("AST-004","2024-05-31",430.20,430.16,"USD"),("AST-004","2024-06-28",446.00,445.34,"USD"),
    ("AST-004","2024-07-31",448.50,448.25,"USD"),("AST-004","2024-08-30",428.60,428.67,"USD"),
    ("AST-004","2024-09-30",434.30,432.20,"USD"),
    # JPM (AST-002)
    ("AST-002","2024-01-31",170.50,170.11,"USD"),("AST-002","2024-02-29",186.40,185.52,"USD"),
    ("AST-002","2024-03-28",198.60,198.74,"USD"),("AST-002","2024-04-30",194.80,195.56,"USD"),
    ("AST-002","2024-05-31",200.10,200.11,"USD"),("AST-002","2024-06-28",205.80,205.55,"USD"),
    ("AST-002","2024-07-31",220.40,220.17,"USD"),("AST-002","2024-08-30",223.60,223.09,"USD"),
    ("AST-002","2024-09-30",224.50,224.31,"USD"),
    # SAP (AST-005)
    ("AST-005","2024-01-31",175.20,175.08,"EUR"),("AST-005","2024-02-29",178.90,178.44,"EUR"),
    ("AST-005","2024-03-28",183.40,183.10,"EUR"),("AST-005","2024-04-30",180.60,180.24,"EUR"),
    ("AST-005","2024-05-31",186.50,186.30,"EUR"),("AST-005","2024-06-28",190.20,190.14,"EUR"),
    ("AST-005","2024-07-31",195.60,195.42,"EUR"),("AST-005","2024-08-30",200.10,200.04,"EUR"),
    ("AST-005","2024-09-30",205.30,205.14,"EUR"),
    # UST10Y (AST-007)
    ("AST-007","2024-01-31", 97.80, 97.84,"USD"),("AST-007","2024-02-29", 96.50, 96.36,"USD"),
    ("AST-007","2024-03-28", 96.20, 96.14,"USD"),("AST-007","2024-04-30", 94.80, 94.70,"USD"),
    ("AST-007","2024-05-31", 95.60, 95.50,"USD"),("AST-007","2024-06-28", 96.40, 96.32,"USD"),
    ("AST-007","2024-07-31", 97.20, 97.18,"USD"),("AST-007","2024-08-30", 98.10, 98.12,"USD"),
    ("AST-007","2024-09-30", 99.30, 99.18,"USD"),
    # GOLD (AST-010)
    ("AST-010","2024-01-31",2034.00,2036.22,"USD"),("AST-010","2024-02-29",2095.00,2094.42,"USD"),
    ("AST-010","2024-03-28",2180.00,2178.38,"USD"),("AST-010","2024-04-30",2350.00,2336.70,"USD"),
    ("AST-010","2024-05-31",2340.00,2327.68,"USD"),("AST-010","2024-06-28",2330.00,2325.34,"USD"),
    ("AST-010","2024-07-31",2440.00,2426.24,"USD"),("AST-010","2024-08-30",2510.00,2503.40,"USD"),
    ("AST-010","2024-09-30",2650.00,2649.34,"USD"),
]

def create_prices_df(spark: SparkSession) -> DataFrame:
    """End-of-month close prices for key assets."""
    return spark.createDataFrame(PRICES_DATA, schema=PRICES_SCHEMA)


# ---------------------------------------------------------------------------
# dim_fx_rates  — monthly FX rates (to USD)
# ---------------------------------------------------------------------------

FX_SCHEMA = StructType([
    StructField("base_ccy",   StringType(), False),
    StructField("quote_ccy",  StringType(), False),
    StructField("rate_date",  StringType(), True),
    StructField("rate",       DoubleType(), True),  # 1 base_ccy = rate quote_ccy
])

FX_DATA = [
    # EUR/USD
    ("EUR","USD","2024-01-31",1.0835),("EUR","USD","2024-02-29",1.0790),
    ("EUR","USD","2024-03-28",1.0810),("EUR","USD","2024-04-30",1.0720),
    ("EUR","USD","2024-05-31",1.0830),("EUR","USD","2024-06-28",1.0740),
    ("EUR","USD","2024-07-31",1.0820),("EUR","USD","2024-08-30",1.1080),
    ("EUR","USD","2024-09-30",1.1160),
    # GBP/USD
    ("GBP","USD","2024-01-31",1.2703),("GBP","USD","2024-02-29",1.2646),
    ("GBP","USD","2024-03-28",1.2620),("GBP","USD","2024-04-30",1.2530),
    ("GBP","USD","2024-05-31",1.2718),("GBP","USD","2024-06-28",1.2640),
    ("GBP","USD","2024-07-31",1.2853),("GBP","USD","2024-08-30",1.3120),
    ("GBP","USD","2024-09-30",1.3310),
    # JPY/USD
    ("JPY","USD","2024-01-31",0.00665),("JPY","USD","2024-02-29",0.00668),
    ("JPY","USD","2024-03-28",0.00661),("JPY","USD","2024-04-30",0.00642),
    ("JPY","USD","2024-05-31",0.00638),("JPY","USD","2024-06-28",0.00625),
    ("JPY","USD","2024-07-31",0.00656),("JPY","USD","2024-08-30",0.00703),
    ("JPY","USD","2024-09-30",0.00706),
]

def create_fx_rates_df(spark: SparkSession) -> DataFrame:
    """Monthly FX rates used for multi-currency portfolio valuation."""
    return spark.createDataFrame(FX_DATA, schema=FX_SCHEMA)


# ---------------------------------------------------------------------------
# Convenience loader — returns all DataFrames
# ---------------------------------------------------------------------------

def load_all(spark: SparkSession) -> dict:
    """
    Create and return all Tasty Bytes Consulting DataFrames.

    Returns
    -------
    dict with keys:
        clients, assets, portfolios, transactions, prices, fx_rates
    """
    return {
        "clients":      create_clients_df(spark),
        "assets":       create_assets_df(spark),
        "portfolios":   create_portfolios_df(spark),
        "transactions": create_transactions_df(spark),
        "prices":       create_prices_df(spark),
        "fx_rates":     create_fx_rates_df(spark),
    }


# ---------------------------------------------------------------------------
# Self-contained smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = create_spark_session("demo_data_smoke_test")
    spark.sparkContext.setLogLevel("WARN")

    data = load_all(spark)
    for name, df in data.items():
        print(f"{name:15s}: {df.count():>4} rows,  columns: {df.columns}")

    print(f"\n{DEMO_TAG}")
    print("Smoke test passed.")
    spark.stop()
