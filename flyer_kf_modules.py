# %%
import pandas as pd
from fusion_kf import KFModule
from siuba import *
import numpy as np


# %%
class NoCorrelationKFModule(KFModule):
    def __init__(
        self, *, metric_col, output_col_prefix=None, sample_size_col, process_std
    ):
        self.sample_size_col = sample_size_col
        self.process_std = process_std
        super().__init__(metric_col=metric_col, output_col_prefix=output_col_prefix)

    def process_covariance(self, raw_df):
        n_dims = raw_df.loc[:, [self.metric_col]].shape[1]
        Q_var = np.ones((n_dims, n_dims)) * (self.process_std**2)
        return Q_var

    def measurement_covariance(self, raw_df):
        zs = raw_df.loc[:, [self.metric_col]].to_numpy(copy=True)
        zs[np.isnan(zs)] = 0

        ns = raw_df.loc[:, [self.sample_size_col]].to_numpy(copy=True)
        ns[np.isnan(ns)] = 0

        se = np.sqrt(np.abs(zs * (1 - zs) + 1e-8) / (ns + 1e-8)) + np.sqrt(0.25 / (ns + 1e-8))  # fmt: skip
        se_2 = se**2
        Rs = np.eye(se_2.shape[1]) * se_2[:, np.newaxis, :]
        return Rs
