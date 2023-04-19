# %%
from db import fdb
from siuba import *
from fusion_kf import DataLoader, Runner
from fusion_kf.kf_modules import NoCorrelationKFModule
from fusion_kf.callbacks import PivotLong, ConcactPartitions

# %%
# fetch/cleanup raw data
raw_flyer_attrs_national = (
    fdb.L2METRICS.DASH_ZZ_COMMON.MODELLED_0062_FLYERATTR(lazy=True)
    >> filter(
        _.METRIC_TYPE.isin(["QFlyerAttr_OfNotice", "QEFlyerAttr_OfNotice"]),
        _.SOURCE == "Competitor_Raw",
        _.REPORTED_REGION_CODE == 101,
    )
    >> select(
        _.RETAILER_CODE,
        _.FLYER_TYPE,
        _.MONTH_YEAR,
        _.REPORTED_REGION_CODE,
        _.OPTION_NUMBER,
        _.OPTION_NAME,
        _.SAMPLE_COUNT,
        _.SAMPLE_WEIGHT,
        _.METRIC_PERCENT,
    )
    >> collect()
)

# %%
# Dataloader is used to feed one partition of data to filter at a time.
# `id_cols` defines the partitions.
flyer_attrs_national_dl = DataLoader(
    table=raw_flyer_attrs_national,
    id_cols=[
        "FLYER_TYPE",
        "REPORTED_REGION_CODE",
        "RETAILER_CODE",
        "OPTION_NUMBER",
        "OPTION_NAME",
    ],
    date_col="MONTH_YEAR",
)
# %%
# `NoCorrelationKFModule` create and run filter for each partition.
flyer_attrs_kf_module = NoCorrelationKFModule(
    metric_col="METRIC_PERCENT", sample_size_col="SAMPLE_COUNT", process_std=0.010
)

# %%
# run the filter
# fmt: off
runner = Runner(
    callbacks=[
        PivotLong(),
        ConcactPartitions()
    ]
)
filtered_flyer_attrs_national = runner.run(
    models=[flyer_attrs_kf_module], 
    dataloaders=flyer_attrs_national_dl, 
    parallel=True
)
# fmt: on

# %%
# upload to snowflake
fdb.upload(
    filtered_flyer_attrs_national,
    database="FUSEDDATA",
    schema="DATASCI_LAB",
    table="FLYER_ATTRIBUTE",
    if_exists="replace",
)

# %%
# sample output
(
    filtered_flyer_attrs_national
    >> filter(_.RETAILER_CODE == 17, _.OPTION_NUMBER == 2, _.FLYER_TYPE == "Paper")
).plot(x="MONTH_YEAR", y=["METRIC_PERCENT", "METRIC_PERCENT_RTS"])

# %%
