# -*- coding: utf-8 -*-

"""
clidesc.py provides helper functions to support the extraction of data from a CliDEsc database.

Author:
    James Sturman, NIWA

Date:
    July 2025

Usage:
    import this module into your Python script and call the functions as needed.

Note:
    When returning a table (e.g., the result of a SQL query to the clidesc database), this table is a Pandas DataFrame object,
    which is similar to (but more efficient than) an R dataframe, and makes slicing, column or row selection,
    resampling, etc. extremely convenient. When time-series are returned, the index of the dataframe (i.e., the 'rows'
    identifier) is a Pandas DatetimeIndex object resulting from the conversion of the lsd field to a Python datetime
    object and setting it as the index for the DataFrame. The name of the index is invariably 'timestamp'.
"""

# modules
import pandas as pd
import logging

################################################################

def stationMetadata(conn, stations: list = None, chain: bool = False) -> pd.DataFrame:
    """
    Return a DataFrame of station metadata extracted from the CliDEsc database Station table.

    Args:
        conn: A SQLAlchemy database connection object as returned by utils.db_open().
        stations (list, optional): List of station numbers (e.g., ['XXXXX', 'YYYYY', 12345]).
        chain (bool, optional): If True, return only station metadata for stations with station chains.

    Returns:
        pd.DataFrame: DataFrame containing station metadata, or None if query fails.
    """

    # create inputs string to capture the input values
    inputs = '{}, {}'.format(str(stations), str(chain.__bool__()))

    # Stations list must be a python list. Ensure station numbers are strings.
    if isinstance(stations, list):
        stations = [str(x) for x in stations]
    # If no stations then explicitly set stations to None for completeness
    else:
        stations = None

    # If chain option return only station info for stations with chains
    if chain and not stations:
        chainedStationTbl = getStationChainIDs(conn)
    elif chain and stations:
        chainedStationTbl = getStationChainIDs(conn, stations)
    else:
        chainedStationTbl = None

    # logging.debug(chainedStationTbl)

    if isinstance(chainedStationTbl, pd.DataFrame) and not chainedStationTbl.empty:
        stations = chainedStationTbl["clide_id1"].values.tolist()

    if stations:
        # build sql query to extract the station metadata from the Station table
        query = """SELECT * FROM "Station" WHERE "Identifier" in ('{}') """.format(stations)
        query = query.replace("[", "")
        query = query.replace("]", "")
        query = query.replace("''", "'")
    else:
        # build sql query to extract the station metadata from the Station table
        query = """SELECT * FROM "Station" """
        query = query.replace("[", "")
        query = query.replace("]", "")
        query = query.replace("''", "'")

    try:
        table = pd.read_sql(query, conn)
        return table
    except Exception as e:
        logging.error(f"Exception in stationMetadata: {e}")
        logging.error(f"Query failed for {inputs}. Check station numbers and database connection.")
        return None


# # ################################################################
# # def getChain(conn, station=None):
# #     """
# #     ```clidesc.getChain``` queries the CliDEsc database and returns a dictionary containing the primary station ID and chained station IDs
# #
# #     :param conn: A SQLAlchemy database connection object as returned by db_open()
# #     :type conn: db connection
# #     :param station: Primary station number
# #     :type station: int or string
# #     :return: Data dictionary containing the primary station ID and chained station IDs
# #     :rtype: dictionary
# #     """
# #
# #     # create inputs string to capture the input values
# #     inputs = '{}'.format(str(station))
# #
# #     # In the CliDEsc DB stations are allocated a alternative ID called ID
# #     # So first step is to get the matching CliDEsc DB station ID for the given station number
# #     # build sql query
# #
# #     if station:
# #         query = """SELECT "ID" FROM "Station" WHERE "Identifier" = '{}'""".format(str(station))
# #     else:
# #         query = '''SELECT "ID" FROM "Station"'''
# #
# #     try:
# #         # run sql query
# #         table = pd.read_sql(query, conn).ID.values.tolist()
# #
# #         if not table.empty():
# #             clidesc_id = table.ID[0]
# #         else:
# #             clidesc_id = None
# #
# #         # Next if there is a CliDEsc ID returned above use it to find any chained stations
# #         if clidesc_id:
# #
# #             # build sql query. The clidesc_id must be a string to insert into the sql query string
# #             query = """SELECT "Station2ID" as ID FROM "StationChain" WHERE "Station1ID" in ('{}')""".format(str(clidesc_id))
# #             query = query.replace("[", "")
# #             query = query.replace("]", "")
# #             query = query.replace("''", "'")
# #
# #             # run sql query to return a list of chained IDs
# #             chainIDs = pd.read_sql(query, conn).id.values
# #
# #             # if chained station ID are returned above go back to the Station table and find the matching CLiDE DB
# #             # station numbers.
# #             if chainIDs.__len__() > 0:
# #
# #                 # build sql query. The chainIDs list must be converted to a concatenated string to insert into the query string
# #                 query = """SELECT "Identifier" FROM "Station" WHERE "ID" in ('{}')""".format([str(x) for x in chainIDs])
# #                 query = query.replace("[", "")
# #                 query = query.replace("]", "")
# #                 query = query.replace("''", "'")
# #
# #                 stations = pd.read_sql(query, conn).Identifier.values.tolist()
# #
# #                 stations_dict = {
# #                     'primary_station': station,
# #                     'chained_stations': stations
# #                 }
# #
# #                 return stations_dict
# #
# #             # if no matching ID in the clidesc DB just return the station number and no chained stations
# #             else:
# #                 stations_dict = {
# #                     'primary_station': station,
# #                     'chained_stations': None
# #                 }
# #
# #             return stations_dict
# #
# #         else:
# #             print("Warning: station {} not found in CliDEsc DB stations table".format(inputs))
# #             return None
# #
# #     except Exception as e:
# #         print("Exception: " + e)
# #         print('query failed for {}, was probably malformed'.format(inputs))
# #         return None
# #
#
# ################################################################
# def chainData(data, stations):
#     """
#     ```clidesc.chainData``` takes a data table containing data from a primary climate station and additional
#     data from chained climate stations. It uses the chained climate station data to complete any missing data for the\
#     primary climate station.
#
#     :param data: a Pandas.DataFrame of climate station data as returned from the ```clide.clide_obs``` function
#     :type data:  Pandas.DataFrame
#     :param stations: a dictionary of stations numbers
#     :type stations: dictionary
#     :return: A Pandas.DataFrame containing the consolidated data
#     :rtype: Pandas.DataFrame
#     """
#
#     primaryStation = stations['primary_station']
#     chainedStations = stations['chained_stations']
#
#     # if chained stations are None then just return the data unchanged
#     if chainedStations is None:
#         return data
#     else:
#         primaryData = data[data.station_no == primaryStation]
#         chainedData = data[data.station_no.isin(chainedStations)]
#
#         # don't need table rows where all variables are NaN
#         chainedData = chainedData.dropna(how='all', subset=[x for x in chainedData.columns.to_list() if x not in ['lsd', 'station_no']])
#
#         # make sure there are data to work with
#         if data.__len__() > 0:
#
#             # make empty table with unique timestamps
#             table = pd.DataFrame(data.index.unique())
#             table.index = table.timestamp
#
#             # join data for primary stations
#             table = table.join(primaryData)
#
#             # loop over chained station numbers and fill nan values
#             for stn in chainedStations:
#                 table = table.fillna(chainedData[chainedData.station_no == str(stn)])
#
#             table.drop('timestamp', axis=1, inplace=True)
#
#             return table
#
#         else:
#             print("No data in table.")
#             return None
#

################################################################

def getStationChainIDs(conn, stations: list = None) -> pd.DataFrame:
    """
    Query the CliDEsc database and return a DataFrame containing Station Chain metadata.

    Args:
        conn: A SQLAlchemy database connection object as returned by db_open().
        stations (list, optional): List of station numbers (e.g., ['XXXXX', 'YYYYY', 12345]).

    Returns:
        pd.DataFrame: DataFrame of Station Chain metadata, or None if query fails.
    """

    # If stations list a string reformat the string
    if isinstance(stations, str) and ',' in stations:
        stations = stations.replace(',', '\',\'')
    # If stations list a python list ensure station numbers are strings
    elif isinstance(stations, list):
        stations = [str(x) for x in stations]

    if not stations:
        query = """SELECT "StationChain"."Station1ID", "StationChain"."Station2ID",
                "Station"."Identifier" as clide_id1,
                "Station2"."Identifier" as clide_id2,
                "Station"."Name" as name1,
                "Station2"."Name" as name2
                FROM "Station"
                INNER JOIN "StationChain"
                ON "Station"."ID"="StationChain"."Station1ID"
                JOIN "Station" AS "Station2"
                ON "StationChain"."Station2ID"="Station2"."ID"
                 """
    else:
        query = """SELECT "StationChain"."Station1ID", "StationChain"."Station2ID",
                "Station"."Identifier" as clide_id1,
                "Station2"."Identifier" as clide_id2,
                "Station"."Name" as name1,
                "Station2"."Name" as name2
                FROM "Station"
                INNER JOIN "StationChain"
                ON "Station"."ID"="StationChain"."Station1ID"
                JOIN "Station" AS "Station2"
                ON "StationChain"."Station2ID"="Station2"."ID"
                WHERE "Station"."Identifier" IN ('{}')
                 """.format(str(stations))

    query = query.replace("[", "")
    query = query.replace("]", "")
    query = query.replace("''", "'")

    try:
        table = pd.read_sql(query, conn)
        return table
    except Exception as e:
        logging.error(f"Exception in getStationChainIDs: {e}")
        logging.error(f"Query failed for {query}")
        return None


## Getting the connection set up, and getting classes from ss_clidesc
def getStationsByClass(conn, country: str) -> pd.DataFrame:
    """
    Retrieve stations by country from the "Station" table using the specified connection.

    Args:
        conn: The database connection object.
        country (str): The country or comma-separated list of countries to filter stations.

    Returns:
        pd.DataFrame: DataFrame containing station information matching the provided country.

    Example:
        connection = create_connection()  # Function to create a database connection
        stations_df = getStationsByClass(connection, 'USA')
        print(stations_df)
    """
    try:
        if ',' in country:
            country = country.replace(',', "','")
        stations_query = f"SELECT * FROM \"Station\" WHERE \"CountryCode\" IN ('{country}')"
        stations_query = stations_query.replace("[","").replace("]","")
        table = pd.read_sql(stations_query, conn)
        return table
    except Exception as e:
        logging.error(f"Exception in getStationsByClass: {e}")
        return None


