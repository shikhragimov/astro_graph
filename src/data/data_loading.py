from typing import List, Union
from astroquery.gaia import Gaia
from astropy.table import Table


def get_query_pagination_like(columns: List[str], limit: int = 1000, begin_id:  Union[int, bool] = False) -> str:
    """
    compose query for GDR3 for extracting sorted rows
    :param columns: columns to extract
    :param limit: limit per extraction
    :param begin_id: id from which to begin extraction
    :return: query
    """
    if begin_id:
        return f"SELECT TOP {limit} {', '.join(columns)} FROM gaiadr3.gaia_source WHERE source_id > " \
               f"{begin_id} ORDER BY source_id"
    else:
        return f"SELECT TOP {limit} {', '.join(columns)} FROM gaiadr3.gaia_source ORDER BY source_id"


def simple_sequence_query_run(query: str) -> Table:
    """
    run async query as sync
    :param query: SQL query
    :return: Table containing results
    """
    job = Gaia.launch_job_async(query)
    return job.get_results()
