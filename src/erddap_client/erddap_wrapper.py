import pandas as pd
from erddapy import ERDDAP, servers

GLIDER_SERVER_NAME = "ngdac"
GLIDER_URL = str(servers[GLIDER_SERVER_NAME].url)

GLIDER_SERVER_INFO = {
    "server": GLIDER_URL,
    "protocol": "tabledap",
    "response": "csv",
}


class GliderIngestor:

    def __init__(self):
        self.e = ERDDAP(**GLIDER_SERVER_INFO)

    def dataset_search(
        self,
        search_for='"sound speed" salinity temp',
        *,
        min_lat=None,
        max_lat=None,
        min_lon=None,
        max_lon=None,
        protocol=None,
        response="csv",
        **search_filters,
    ):
        """Search the glider ERDDAP catalog and return the results as a DataFrame."""
        bounds = {
            "min_lat": min_lat,
            "max_lat": max_lat,
            "min_lon": min_lon,
            "max_lon": max_lon,
        }
        search_params = {
            key: value
            for key, value in bounds.items()
            if value is not None
        }
        search_params.update(search_filters)

        url = self.e.get_search_url(
            search_for=search_for,
            response=response,
            protocol=protocol,
            **search_params,
        )
        return pd.read_csv(url).drop_duplicates()

    def get_dataset_metadata(self, dataset_id):
        target_dataset_id = dataset_id if dataset_id else self.e.dataset_id
        return pd.read_csv(self.e.get_info_url(target_dataset_id))


    def get_dataset(self, dataset_id, variables):
        self.e.dataset_id = dataset_id
        return pd.read_csv(self.e.get_download_url(variables=variables))
