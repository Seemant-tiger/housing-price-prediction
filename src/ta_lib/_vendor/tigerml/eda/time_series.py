from tigerml.core.reports import format_tables_in_report

from .base import EDAReport
from .plotters import TSMixin
from .segmented import SegmentedEDAReport


class TSReport(EDAReport, TSMixin):
    """Toolkit to get the basic summary of the time series data.

    To evaluate and generate reports. Given the target, perform the lag analysis, outlier analysis, get the periodicity, missing periods.

    Parameters
    ----------
    data : pd.DataFrame
        dataframe to be analyzed
    ts_column : string
        Name of the time series column
    y : string, default=None
        Name of the target column
    y_continuous : bool, default=None
        Set to True, for classificaiton target

    Examples
    --------
    >>> from tigerml.eda import TSReport
    >>> import pandas as pd
    >>> df = pd.read_csv("time_series_data.csv")
    >>> an = TSReport(df, ts_column = 'time_series_column',y = 'target', y_continuous = False)
    >>> an.get_report(quick = True)
    """

    def __init__(self, data, ts_column, y=None, y_continuous=None):
        assert ts_column in data.columns, f"{ts_column} does not exist in given data"
        self.ts_column = ts_column
        self.ts_identifiers = []
        # data.set_index(ts_column, inplace=True)
        # data.sort_values([self.ts_column], inplace=True)
        if not data.__module__.startswith("tigerml"):
            from tigerml.core.dataframe import DataFrame

            data = DataFrame(data)
            data = data.convert_datetimes()
        super().__init__(data, y, y_continuous)
        if ts_column not in self.dt_cols:
            raise TypeError(f"'{ts_column}' column is not of type datetime64.")
        self.data = self._ts_sorter()

    def create_report(self, y=None, quick=True, corr_threshold=None):
        """
        Creates a report.

        Parameters
        ----------
        y : str, default=None
        quick : boolean, default=True
            If true,calculate SHAP values and create bivariate plots
        corr_threshold : float, default=None
            To specify correlation threshold

        """
        super()._create_report(y=y, quick=quick, corr_threshold=corr_threshold)
        keys = list(self.report.keys())
        index = 2
        self.report.update({"time_series_analysis": self.get_time_series_analysis()})
        ts_key_drivers = self.get_ts_key_drivers(quick=quick)
        for col in self.y_cols:
            self.report["key_drivers"][col].update(ts_key_drivers[col])
        desirbale_order = keys[:index] + ["time_series_analysis"] + keys[index:]
        from collections import OrderedDict

        self.report = OrderedDict(self.report)
        for key in desirbale_order:
            self.report[key] = self.report.pop(key)

    def get_report(
        self,
        format=".html",
        name="",
        y=None,
        corr_threshold=None,
        quick=True,
        save_path="",
        tiger_template=False,
        light_format=True,
    ):
        """Returns the report."""
        self.create_report(y=y, quick=quick, corr_threshold=corr_threshold)
        if light_format:
            self.report = format_tables_in_report(self.report)
        return super()._save_report(
            format=format, name=name, save_path=save_path, tiger_template=tiger_template
        )


class SegmentedTSReport(SegmentedEDAReport, TSMixin):
    """Toolkit to get the basic summary of the segmented(grouped) time series data.

    To evaluate and generate reports.

    Given the target, perform the lag analysis, outlier analysis, get the periodicity, missing periods.

    Parameters
    ----------
    data : pd.DataFrame
        dataframe to be analyzed
    ts_column : string
        Name of the time series column
    ts_identifiers : list
        list of columns, to group the data
    y : string, default = None
        Name of the target column
    y_continuous : bool, default=None
        Set to True, for classificaiton target

    Examples
    --------
    >>> from tigerml.eda import SegmentedTSReport
    >>> import pandas as pd
    >>> df = pd.read_csv("time_series_data.csv")
    >>> an = SegmentedTSReport(df, ts_column = 'time_series_column', ts_identifiers = [], y = 'target', y_continuous = False)
    >>> an.get_report(quick = True)
    """

    def __init__(self, data, ts_column, ts_identifiers, y=None, y_continuous=None):
        assert ts_column in data.columns, f"{ts_column} does not exist in given data"
        self.ts_column = ts_column
        # data.set_index(ts_column, inplace=True)
        # data.sort_index(inplace=True)
        # data.sort_values([self.ts_column], inplace=True)
        self.ts_identifiers = ts_identifiers
        if not data.__module__.startswith("tigerml"):
            from tigerml.core.dataframe import DataFrame

            data = DataFrame(data)
            data = data.convert_datetimes()
        super().__init__(
            data, segment_by=ts_identifiers, y=y, y_continuous=y_continuous
        )
        if ts_column not in self.dt_cols:
            raise TypeError(f"'{ts_column}' column is not of type datetime64.")
        self.data = self._ts_sorter()

    def create_report(self, y=None, quick=True, corr_threshold=None):
        """Creates the report."""
        super().create_report(y=y, quick=quick, corr_threshold=corr_threshold)
        keys = list(self.report.keys())
        index = keys.index("segments_analysis")
        self.report["segments_analysis"].update(self.get_time_series_analysis())
        self.report["time_series_analysis"] = self.report.pop("segments_analysis")
        ts_key_drivers = self.get_ts_key_drivers(quick=quick)
        for col in self.y_cols:
            self.report["key_drivers"][col].update(ts_key_drivers[col])
        desirable_order = keys[:index] + ["time_series_analysis"] + keys[index + 1 :]
        from collections import OrderedDict

        self.report = OrderedDict(self.report)
        for key in desirable_order:
            self.report[key] = self.report.pop(key)
