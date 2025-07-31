"""
clide.py provides helper functions to support the extraction of data from a CliDE database.

Author:
    James Sturman, NIWA

Date:
    December 2022

Usage:
    import this module into your Python script and call the functions as needed.

Note:
    When returning a table (e.g., the result of a SQL query to the clide database), this table is a Pandas DataFrame object,
    which is similar to (but more efficient than) an R dataframe, and makes slicing, column or row selection,
    resampling, etc. extremely convenient. When time-series are returned, the index of the dataframe (i.e., the 'rows'
    identifier) is a Pandas DatetimeIndex object resulting from the conversion of the lsd field to a Python datetime
    object and setting it as the index for the DataFrame. The name of the index is invariably 'timestamp'.
"""

# -*- coding: utf-8 -*-


import logging
from datetime import datetime
import pandas as pd
import clidesc.clidesc as clidesc

################################################################

def stations(conn, stations: list = None) -> pd.DataFrame:
    """
    Return all station metadata for a given station number, list of station numbers, or all stations.

    Args:
        conn: A SQLAlchemy database connection object as returned by utils.db_open().
        stations (list or str, optional): A station number or list of station numbers (e.g., [XXXXX, YYYYY, ZZZZZ] or 'XXXXX, YYYYY, ZZZZZ').

    Returns:
        pd.DataFrame: DataFrame containing the station information, or None if query fails.
    """

    # If stations list a string reformat the string
    if isinstance(stations, str) and ',' in stations:
        stations = stations.replace(',', '\',\'')
    # If stations list a python list ensure station numbers are strings
    elif isinstance(stations, list):
        stations = [str(x) for x in stations]

    if stations:
        # builds the query string
        query = """SELECT * FROM stations WHERE station_no IN ('{}') ORDER BY station_no""".format(stations)
        query = query.replace("[", "")
        query = query.replace("]", "")
        query = query.replace("''", "'")
    else:
        query = """SELECT * FROM stations ORDER BY station_no"""

    # get the table returned by the query as a pandas dataframe
    try:
        table = pd.read_sql(query, conn)
        return table
    except Exception as e:
        logging.error(f"Exception in stations: {e}")
        if stations:
            logging.error(f'Query failed for {stations}. Check station numbers and database connection')
        else:
            logging.error(f'Query failed for "{query}". Check station numbers and database connection')
        return None


################################################################

def getStationsByCountry(conn, country: str) -> pd.DataFrame:
    """
    Execute a SQL query on the CliDE database station table to extract station information for a given country code.

    Args:
        conn: A SQLAlchemy database connection object as returned by utils.db_open().
        country (str): The two-letter country identifier code (e.g., 'WS' for Samoa, 'FJ' for Fiji).

    Returns:
        pd.DataFrame: DataFrame containing station information for the country, or None if query fails.
    """

    if ',' in country:
        country = country.replace(',', '\',\'')

    # builds the query string
    query = """SELECT * FROM stations WHERE country_code IN ('{}') ORDER BY country_code, station_no""".format(country)

    query = query.replace("[", "")
    query = query.replace("]", "")
    query = query.replace("''", "'")

    # get the table returned by the query as a pandas dataframe
    try:
        table = pd.read_sql(query, conn)
        return table
    except Exception as e:
        logging.error(f"Exception in getStationsByCountry: {e}")
        logging.error(f'Query failed for {country}. Check country code and database connection')
        return None


################################################################
def stationsComprehensive(conn, stations=None, tablename='stations', channels=None, minObs=0, _and=False,
                          from_date=None, to_date=None):
    """
    Get all the station details for a station number or list of station numbers, with optional filters.

    This function is an extension of `clide.stations` and accepts additional parameters to filter the
    returned station information (e.g., by the amount of data in observation tables such as 'obs_daily',
    'obs_subdaily', or by date range).

    Args:
        conn: A SQLAlchemy database connection object as returned by utils.db_open().
        stations (list, optional): The station list (e.g., [XXXXX, YYYYY]). A single station number should also be provided as a list (e.g., [ZZZZZ]). Unlike `clide.stations`, this method only accepts Python lists and not strings.
        tablename (str, optional): Name of a single observation table (e.g., 'obs_daily' or 'obs_subdaily').
        channels (list, optional): The variable channels in the table. Only used if tablename is not set to 'stations'.
        minObs (int, optional): Minimum number of observations (default 0). Returns stations with at least this many observations for the specified variable channels.
        _and (bool, optional): If True, minObs is tested against all elements (SQL AND operator); all variables must have more observations than minObs. If False, any variable can have more observations than minObs (SQL OR operator). Default is False.
        from_date (str, optional): ISO 8601 standard start date string (e.g., '2014-01-01').
        to_date (str, optional): ISO 8601 standard end date string (e.g., '2014-01-01').

    Returns:
        pd.DataFrame: DataFrame containing the station information, or None if query fails.
    """

    # If stations list a string reformat the string
    if isinstance(stations, str) and ',' in stations:
        stations = stations.replace(',','\',\'')
    # If stations list a python list ensure station numbers are strings
    elif isinstance(stations, list):
        stations = [str(x) for x in stations]

    if _and:
        operator = ' AND'
    else:
        operator = ' OR'

    # Extract the full station number list for checking incoming station numbers
    station_list_sql = """SELECT station_no FROM stations GROUP BY station_no ORDER BY station_no"""
    stations_table = pd.read_sql(station_list_sql, conn)
    stations_list = stations_table.station_no.tolist()

    # Before going any further check if a list of stations has been provided and if so are the listed station numbers in
    # the full station list extracted above.
    if stations:
        # check if provided station numbers exist in the station list
        for station in stations:
            if station not in stations_list:
                raise Exception(f"Station number {station} not found in stations table. Please check the provided station numbers")

    # dictionary for sql strings used for accessing the stations table.
    dict = {
        'tablename': tablename,
        'stations': str(stations).strip("[]") if stations else None,
        'from_date': from_date if from_date else '1000-01-01',
        'to_date': to_date if to_date else str(datetime.now().date())
    }

    try:
        # If the table name = stations, which is the default
        if tablename == 'stations':
            # if a list of stations has been provided
            if stations:
                # sql where clause for extracting only the listed stations from the stations table.
                where = """
                        station_no IN ({0}) AND (start_date BETWEEN TO_TIMESTAMP('{1}', 'yyyy-mm-dd')
                        AND TO_TIMESTAMP('{2}', 'yyyy-mm-dd'))
                        """.format(dict['stations'], dict['from_date'], dict['to_date'])

            # if no list of stations provided assume all stations requested
            else:
                # sql where clause for extracting only the listed stations from the stations table.
                where = """
                        start_date BETWEEN TO_TIMESTAMP('{0}', 'yyyy-mm-dd')
                        AND TO_TIMESTAMP('{1}', 'yyyy-mm-dd')
                        """.format(dict['from_date'], dict['to_date'])

            # builds the query string
            query = """
                    SELECT * FROM {0}
                    WHERE {1}
                    ORDER BY station_no""".format(dict['tablename'], where)

            # Export table. This is returned by this function
            table = pd.read_sql(query, conn)

        # if table name other than stations i.e. obs_daily then test what stations have data in the provided table name
        # count the data for the various columns/elements. If no elements listed count all elements
        # currently 1 table can be queried at a time
        elif dict['tablename'] != 'stations':
            # SQL query to list all numeric col names for given table name
            col_list_sql = """
                           SELECT column_name, data_type
                           FROM information_schema.columns
                           WHERE table_name='{0}' AND data_type = 'numeric'
                           """.format(dict['tablename'])

            col_table = pd.read_sql(col_list_sql, conn)
            col_list = col_table.column_name.tolist()

            # if a list of stations has been provided
            if stations:
                # sql where clause for extracting only the listed stations.
                inner_where = """
                              station_no IN ({0})
                              AND (lsd BETWEEN TO_TIMESTAMP('{1}', 'yyyy-mm-dd') AND TO_TIMESTAMP('{2}', 'yyyy-mm-dd'))
                              """.format(dict['stations'], dict['from_date'], dict['to_date'])

            else:
                # if no list of stations provided assume all stations requested
                inner_where = """
                              lsd >= TO_TIMESTAMP('{0}', 'yyyy-mm-dd')
                              AND lsd <= TO_TIMESTAMP('{1}', 'yyyy-mm-dd')
                              """.format(dict['from_date'], dict['to_date'])

            # if a list of elements has been provided
            if channels:
                # test if element names provided match
                for element in channels:
                    if element not in col_list:
                        raise Exception("column name {} not in table {}. Please check the element list provided".format(element, tablename))

                channels_select = "station_no, " + str(['count({0}) as {0}_obs_count'.format(x) for x in channels]).strip("[]").replace("'", "")
                channels_count = "station_no, " + str(['{}_obs_count'.format(x) for x in channels]).strip("[]").replace("'", "")
                channels_where = str(['{0}_obs_count >= {1}'.format(x, minObs) for x in channels]).strip("[]").replace("'", "").replace(",", operator)

            # if no elements assume test all numeric elements in table
            else:
                channels_select = "station_no, " + str(['count({0}) as {0}_obs_count'.format(x) for x in col_list]).strip("[]").replace("'", "")
                channels_count = "station_no, " + str(['{}_obs_count'.format(x) for x in col_list]).strip("[]").replace("'", "")
                channels_where = str(['{0}_obs_count >= {1}'.format(x, minObs) for x in col_list]).strip("[]").replace("'", "").replace(",", operator)

            # builds the query string
            query = """
                    SELECT * FROM
                    (SELECT * FROM stations GROUP BY id ORDER BY station_no) as stations
                    RIGHT JOIN        
                    (SELECT {0} FROM
                    (SELECT {1}
                    FROM {2} 
                    WHERE {3}
                    GROUP BY station_no
                    ORDER BY station_no) as foo
                    WHERE {4}) as bar
                    ON (stations.station_no = bar.station_no)
                    """.format(channels_count, channels_select, tablename, inner_where, channels_where)

            table = pd.read_sql(query, conn)
        else:
            table = pd.DataFrame()

        return table if not table.empty else None

    except Exception as e:
        print(f"Exception: {e}")
        return None


################################################################
def TenMinStationList(conn):
    """
    Return a DataFrame of stations recording 10-minute observations from the obs_aws table.

    Args:
        conn: A SQLAlchemy database connection object as returned by utils.db_open().

    Returns:
        pd.DataFrame: DataFrame of stations recording 10-minute observations, or None if query fails.
    """

    # builds the query string
    query = """select id.station_no,name_primary,name_secondary,latitude,longitude,country_code from (
        select distinct station_no from obs_aws where to_char(lsd ,'MI') = '10'
        intersect
        select distinct station_no from obs_aws where to_char(lsd ,'MI') = '20'
        intersect
        select distinct station_no from obs_aws where to_char(lsd ,'MI') = '30'
        intersect
        select distinct station_no from obs_aws where to_char(lsd ,'MI') = '40'
        intersect
        select distinct station_no from obs_aws where to_char(lsd ,'MI') = '50'
        intersect
        select distinct station_no from obs_aws where to_char(lsd ,'MI') = '00') as id,
        stations stn
        where id.station_no = stn.station_no """
    ## bug found in the previous author's code, presence of aquare brackets
    # are ignoring 2 stations - where the '[' open and clode ']'.
    query = query.replace("[", "")
    query = query.replace("]", "")

    try:
        # get the table returned by the query as a pandas dataframe
        table = pd.read_sql(query, conn)
        return table
    except Exception as e:
        print(f"Exception: {e}")
        print('query failed. Check database connection and the obs_aws table')
        return None


def _obs_query(table, stations, from_date, to_date, channels=None):
    """
    Build a SQL query string for extracting observation data.

    Args:
        table (str): Table name.
        stations (list or str): Station numbers.
        from_date (str): Start date (YYYY-MM-DD).
        to_date (str): End date (YYYY-MM-DD).
        channels (list or str, optional): Channel names.

    Returns:
        str: SQL query string.
    """

    # If stations list a string reformat the string
    if isinstance(stations, str) and ',' in stations:
        stations = stations.replace(',', '\',\'')
    # If stations list a python list ensure station numbers are strings
    elif isinstance(stations, list):
        stations = [str(x) for x in stations]

    # If channels a python list ensure channel elements are a single string
    if isinstance(channels, list):
        channels = str(channels).strip("[]").replace("'", "")
        query = f"""SELECT station_no,
                lsd,
                {channels}
                FROM {table}
                WHERE station_no IN ('{stations}')
                AND (lsd BETWEEN TO_TIMESTAMP('{from_date}', 'yyyy-mm-dd') AND TO_TIMESTAMP('{to_date}', 'yyyy-mm-dd'))
                ORDER BY lsd"""

    else:
        query = f"""SELECT * FROM {table}
                WHERE station_no IN ('{stations}')
                AND (lsd BETWEEN TO_TIMESTAMP('{from_date}', 'yyyy-mm-dd') AND TO_TIMESTAMP('{to_date}', 'yyyy-mm-dd'))
                ORDER BY lsd"""

    query = query.replace("[", "")
    query = query.replace("]", "")
    query = query.replace("''", "'")

    return query


################################################################
def Obs(conn, table, stations, from_date, to_date, channels=None, chained=False, clidesc_conn=None):
    """
    Generic function for executing a SQL query on the CliDE database to extract observation data.

    Args:
        conn: A SQLAlchemy database connection object as returned by utils.db_open().
        table (str): CliDE database table name.
        stations (list or str): The station number or list (e.g., [XXXXX, YYYYY, ZZZZZ] or "XXXXX, YYYYY, ZZZZZ").
        from_date (str): ISO 8601 standard start date string (e.g., '2021-09-01').
        to_date (str): ISO 8601 standard end date string (e.g., '2021-09-30').
        channels (list or str, optional): Variable channels in the table (e.g., ['rain_24h', ...] or "rain_24h, ...").
        chained (bool, optional): If True, include station chains.
        clidesc_conn: A SQLAlchemy database connection object for the clidesc DB.

    Returns:
        pd.DataFrame: DataFrame containing climate observation data, or None if query fails.
    """

    query = _obs_query(table, stations, from_date, to_date, channels)

    inputs = '{0}::{1}::{2}::{3}::{4}::{5}::{6}'.format(channels, table, stations, from_date, to_date, chained, query)

    # print(inputs)

    # get the table returned by the query as a pandas dataframe
    try:
        table1 = pd.read_sql(query, conn)
        # the index of the pandas DataFrame is the 'lsd' field, correctly
        # cast to pandas datetime index and named 'datetime'
        table1.index = pd.to_datetime(table1.lsd)
        table1.index.name = 'timestamp'

        # print(table1)

        # if chained station option used
        if chained:
            stationChainDF = clidesc.getStationChainIDs(clidesc_conn, stations)
            stations = stationChainDF['clide_id2'].values.tolist()
            query = _obs_query(table, stations, from_date, to_date, channels)
            table2 = pd.read_sql(query, conn)

            dfs = []

            # stationChainDF.to_csv('chain.csv')
            # print(stationChainDF['clide_id1'])

            print(stationChainDF[['clide_id1', 'clide_id2']])

            # loop over Station numbers
            for id in stationChainDF['clide_id1'].unique():
                # Get data for the primary station number
                primaryStation = id
                primaryData = table1[table1.station_no == primaryStation]
                print(primaryData)

                # Get matching chained station numbers as a list
                chainedStations = stationChainDF[stationChainDF['clide_id1'] == str(primaryStation)]['clide_id2'].values.tolist()
                # Use list to get data for the chained stations
                chainedData = table2[table2.station_no.isin(chainedStations)]

                # If any chained stations returned
                if chainedStations:
                    print("Primary Station: {}".format(primaryStation))
                    print("Chained Stations: {}".format(chainedStations))

                    # don't need table rows where all variables are NaN
                    chainedData = chainedData.dropna(how='all', subset=[x for x in chainedData.columns.to_list() if x not in ['lsd', 'station_no']])

                    print("Primary Data: ".format(primaryData))
                    print("Chained Data: ".format(chainedData))

                    # If both primary and chained dataframes have data
                    if not primaryData.empty and not chainedData.empty:
                        # make empty table with unique timestamps
                        df = pd.DataFrame(pd.concat([primaryData, chainedData]).index.unique())
                        df.index = df.timestamp

                        # join data for primary stations
                        df = df.join(primaryData)

                        # loop over chained station numbers and fill nan values
                        for stn in chainedStations:
                            df = df.fillna(chainedData[chainedData.station_no == str(stn)])

                        df.drop('timestamp', axis=1, inplace=True)

                        dfs.append(df)

                    else:
                        dfs.append(primaryData)

            table = pd.concat(dfs)

            return table

        else:
            return table1

    except Exception as e:
        print(f"Exception: {e}")
        print(f'query failed for {inputs}, was probably malformed')
        return None


################################################################
def ObsDaily(conn, stations, from_date, to_date, channels=None):
    """
    Return the daily observations table for the requested stations, channels, and date range.

    Args:
        conn: A SQLAlchemy database connection object as returned by utils.db_open().
        stations (list or str): The station number or list (e.g., [XXXXX, YYYYY, ZZZZZ] or "XXXXX, YYYYY, ZZZZZ").
        from_date (str): ISO 8601 standard start date string (e.g., '2021-09-01').
        to_date (str): ISO 8601 standard end date string (e.g., '2021-09-30').
        channels (list or str, optional): Variable channels in the table (e.g., ['rain_24h', ...] or "rain_24h, ...").

    Returns:
        pd.DataFrame: DataFrame containing daily climate observation data, or None if query fails.
    """

    # some string formatting on the stations if more than one
    if isinstance(stations, str) and ',' in stations:
        stations = stations.replace(',', '\',\'')
    elif isinstance(stations, list):
        stations = [str(x) for x in stations]

    # If channels a python list ensure channel elements are a single string
    if isinstance(channels, list):
        channels = str(channels).strip("[]").replace("'", "")
        query = """SELECT station_no, TO_CHAR(lsd, 'yyyy-mm-dd') as LSD,
                {0} FROM obs_daily WHERE station_no IN ('{1}') 
                AND (lsd BETWEEN TO_TIMESTAMP('{2}', 'yyyy-mm-dd') AND TO_TIMESTAMP('{3}', 'yyyy-mm-dd'))
                ORDER BY lsd""".format(channels, stations, from_date, to_date)
    # if no channel list return data for all channels
    else:
        query = """SELECT * FROM obs_daily WHERE station_no IN ('{0}') 
                AND (lsd BETWEEN TO_TIMESTAMP('{1}', 'yyyy-mm-dd') AND TO_TIMESTAMP('{2}', 'yyyy-mm-dd'))
                ORDER BY lsd""".format(stations, from_date, to_date)

    inputs = '{0}::{1}::{2}::{3}::'.format(channels, stations, from_date, to_date, query)

    ## bug found in the previous author's code, presence of aquare brackets
    # are ignoring 2 stations - where the '[' open and clode ']'.
    query = query.replace("[", "")
    query = query.replace("]", "")
    query = query.replace("''", "'")

    # get the table returned by the query as a pandas dataframe
    try:
        table = pd.read_sql(query, conn)
        # the index of the pandas DataFrame is the 'lsd' field, correctly
        # cast to pandas datetime index and named 'timestamp'
        table.index = pd.to_datetime(table.lsd)
        table.index.name = 'timestamp'
        return table
    except Exception as e:
        print(f"Exception: {e}")
        print(f'query failed for {inputs}, was probably malformed')
        return None


################################################################
def ObsSubDaily(conn, channels, stations, from_date, to_date):
    """
    Return the sub-daily observations table for the requested stations, channels, and date range.

    Args:
        conn: A SQLAlchemy database connection object as returned by utils.db_open().
        channels (list or str): Variable channels in the table (e.g., ['rain_3h', ...] or "rain_3h, ...").
        stations (list or str): The station number or list (e.g., [XXXXX, YYYYY, ZZZZZ] or "XXXXX, YYYYY, ZZZZZ").
        from_date (str): ISO 8601 standard start date string (e.g., '2021-09-01').
        to_date (str): ISO 8601 standard end date string (e.g., '2021-09-30').

    Returns:
        pd.DataFrame: DataFrame containing sub-daily climate observation data, or None if query fails.
    """

    inputs = '{0}::{1}::{2}::{3}'.format(channels, stations, from_date, to_date)

    # some string formatting on the stations if more than one
    if isinstance(stations, str) and ',' in stations:
        stations = stations.replace(',', '\',\'')
    elif isinstance(stations, list):
        stations = [str(x) for x in stations]

    # If channels a python list ensure channel elements are a single string
    if isinstance(channels, list):
        channels = str(channels).strip("[]").replace("'", "")

    # builds the query string
    query = """SELECT station_no
            , TO_CHAR(lsd, 'yyyy-mm-dd') as LSD
            , {0}
            FROM obs_subdaily
            WHERE station_no IN ('{1}')
            AND (lsd BETWEEN TO_TIMESTAMP('{2}', 'yyyy-mm-dd') AND TO_TIMESTAMP('{3}', 'yyyy-mm-dd'))
            ORDER BY lsd""".format(channels, stations, from_date, to_date)

    query = query.replace("[", "")
    query = query.replace("]", "")
    query = query.replace("''", "'")

    # get the table returned by the query as a pandas dataframe
    try:
        table = pd.read_sql(query, conn)
        # the index of the pandas DataFrame is the 'lsd' field, correctly
        # cast to pandas datetime index and named 'timestamp'
        table.index = pd.to_datetime(table.lsd)
        table.index.name = 'timestamp'
        return table
    except Exception as e:
        print(f"Exception: {e}")
        print(f'query failed for {inputs}, was probably malformed')
        return None


################################################################
def ObsAws(conn, stations, from_date, to_date, channels=None, aggregation=None, statistic='AVG'):
    """
    Execute a SQL query on the CliDE database obs_aws table to extract AWS observation data.
    Supports raw data or aggregated data (daily, monthly, yearly).

    Args:
        conn: A SQLAlchemy database connection object as returned by utils.db_open().
        stations (list or str): The station number or list.
        from_date (str): ISO 8601 standard start date string (e.g., 'yyyy-mm-dd').
        to_date (str): ISO 8601 standard end date string (e.g., 'yyyy-mm-dd').
        channels (list or str, optional): Variable channels to include in the query.
        aggregation (str, optional): Aggregation level ('daily', 'monthly', 'yearly'). If None, returns raw data.
        statistic (str, optional): Aggregation function to apply (e.g., 'AVG', 'SUM', 'MAX', 'MIN').

    Returns:
        pd.DataFrame: DataFrame containing AWS observation data, or None if query fails.
    """

    inputs = f"{stations}::{from_date}::{to_date}::{channels}::{aggregation}::{statistic}"

    # Format stations for SQL
    if isinstance(stations, str) and ',' in stations:
        stations = stations.replace(',', '\',\'')
    elif isinstance(stations, list):
        stations = [str(x) for x in stations]

    # Format channels for SQL
    if isinstance(channels, list):
        channels = ', '.join([f"{statistic}({channel}) AS {channel}" for channel in channels])
    else:
        channels = '*'

    # Determine aggregation level
    if aggregation == 'daily':
        group_by = "DATE_TRUNC('day', lsd)"
    elif aggregation == 'monthly':
        group_by = "DATE_TRUNC('month', lsd)"
    elif aggregation == 'yearly':
        group_by = "DATE_TRUNC('year', lsd)"
    else:
        group_by = None  # No aggregation

    # Build the SQL query
    if group_by:
        query = f"""
            SELECT 
                station_no,
                {group_by} AS lsd,
                {channels}
            FROM obs_aws
            WHERE station_no IN ('{stations}')
              AND (lsd BETWEEN TO_TIMESTAMP('{from_date}', 'yyyy-mm-dd') AND TO_TIMESTAMP('{to_date}', 'yyyy-mm-dd'))
            GROUP BY station_no, {group_by}
            ORDER BY station_no, lsd
        """
    else:
        query = f"""
            SELECT 
                station_no,
                lsd,
                {channels}
            FROM obs_aws
            WHERE station_no IN ('{stations}')
              AND (lsd BETWEEN TO_TIMESTAMP('{from_date}', 'yyyy-mm-dd') AND TO_TIMESTAMP('{to_date}', 'yyyy-mm-dd'))
            ORDER BY station_no, lsd
        """

    query = query.replace("[", "").replace("]", "").replace("''", "'")

    # Execute the query and return the result as a Pandas DataFrame
    try:
        table = pd.read_sql(query, conn)
        table.index = pd.to_datetime(table.lsd)
        table.index.name = 'timestamp'
        return table
    except Exception as e:
        print(f"Exception: {e}")
        print(f"Query failed for {inputs}, was probably malformed")
        return None


################################################################
def rain24h(conn, stations, from_date, to_date):
    """
    Read the rain_24h channel from the obs_daily table for the given stations and date range.

    Args:
        conn: A SQLAlchemy database connection object as returned by utils.db_open().
        stations (list or str): The station number or list (e.g., [XXXXX, YYYYY, ZZZZZ] or "XXXXX, YYYYY, ZZZZZ").
        from_date (str): ISO 8601 standard start date string (e.g., '2021-09-01').
        to_date (str): ISO 8601 standard end date string (e.g., '2021-09-30').

    Returns:
        pd.DataFrame: DataFrame containing the rainfall data, or None if query fails.
    """

    try:
        return ObsDaily(conn, stations, from_date, to_date, channels=['rain_24h'])
    except Exception as e:
        print(f"Exception: {e}")
        return None


################################################################
def lastXDaysRain(conn, stations, from_date, to_date):
    """
    Return the sum of rainfall for the requested stations and date range.

    Args:
        conn: A SQLAlchemy database connection object as returned by utils.db_open().
        stations (list or str): A station number or list (e.g., [XXXXX, YYYYY, ZZZZZ] or 'XXXXX, YYYYY, ZZZZZ').
        from_date (str): ISO 8601 standard start date string (e.g., '2014-01-01').
        to_date (str): ISO 8601 standard end date string (e.g., '2014-05-01').

    Returns:
        pd.DataFrame: DataFrame containing the rainfall data, or None if query fails.
    """

    # If stations list a string reformat the string
    if isinstance(stations, str) and ',' in stations:
        stations = stations.replace(',', '\',\'')
    # If stations list a python list ensure station numbers are strings
    elif isinstance(stations, list):
        stations = [str(x) for x in stations]

    query = """SELECT  sum(A.rain_24h) as rain_sum, A.station_no, B.latitude, B.longitude
               FROM obs_daily A  
               Left Outer Join 
               stations B on A.station_no = B.station_no 
               WHERE A.station_no IN ({0}) and (lsd BETWEEN TO_TIMESTAMP('{1}','yyyy-mm-dd') and
               TO_TIMESTAMP('{2}','yyyy-mm-dd'))
               GROUP BY A.station_no, B.latitude, B.longitude""".format(stations, from_date, to_date)

    query = query.replace("[", "")
    query = query.replace("]", "")
    query = query.replace("''", "'")

    try:
        table = pd.read_sql(query, conn)
        return table
    except Exception as e:
        print(f"Exception: {e}")
        return None


################################################################
def scopic_original(conn, station, from_date, to_date):
    """
    Execute a SQL query on the CliDE database to retrieve rainfall data for a specified station and date range.

    This function fetches daily and monthly rainfall data for a given station between the specified `from_date` and `to_date`.
    The query aggregates the data, calculates missing data counts, and formats the results.

    Args:
        conn: Database connection object.
        station (str): Station identifier.
        from_date (str): Start date in 'YYYY-MM-DD' format.
        to_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: DataFrame containing the query results, or None if an exception occurs.

    Example:
        >>> conn = create_connection()
        >>> df = scopic(conn, 'STATION123', '2022-01-01', '2022-12-31')
        >>> print(df)
    """
    inputs = '{0}::{1}::{2}'.format(station, from_date, to_date)

        # Define the SQL query with station_no parameter using .format()
    query = """
    SELECT
        COALESCE(daily.year, monthly.year) AS year,
        COALESCE(daily.month, monthly.month) AS month,
        TO_CHAR(COALESCE(daily.data, monthly.data), '99990.0') AS data,
        data_count,
        COALESCE(missing_cons, 0) AS missing_cons,
        days_in_month
    FROM
        (
            SELECT
                EXTRACT(year FROM od_date_series.date_seq) AS year,
                EXTRACT(month FROM od_date_series.date_seq) AS month,
                SUM(CASE WHEN od.rain_24h IS NULL THEN 0 ELSE 1 END) AS data_count,
                EXTRACT(day FROM (DATE_TRUNC('month', MIN(od.lsd) + '1 month'::INTERVAL) - '1 day'::INTERVAL)) AS days_in_$
                missing_cons_rain('{0}'::text, TO_CHAR(MIN(od.lsd), 'yyyy'), TO_CHAR(MIN(od.lsd), 'mm')) AS missing_cons,
                SUM(od.rain_24h) AS data
            FROM
                obs_daily od
            RIGHT JOIN
                (
                    SELECT
                        GENERATE_SERIES('{1}'::TIMESTAMP WITHOUT TIME ZONE, '{2}'::TIMESTAMP WITHOUT TIME ZONE, '1 day'::I$
                ) od_date_series ON (UPPER(od.station_no) = '{0}'::text AND od.lsd = od_date_series.date_seq)
            GROUP BY
                year, month
        ) daily
    RIGHT JOIN
        (
            SELECT
                EXTRACT(year FROM om_date_series.date_seq) AS year,
                EXTRACT(month FROM om_date_series.date_seq) AS month,
                SUM(om.tot_rain) AS data
            FROM
                obs_monthly om
            RIGHT JOIN
                (
                    SELECT
                        GENERATE_SERIES('{1}'::TIMESTAMP WITHOUT TIME ZONE, '{2}'::TIMESTAMP WITHOUT TIME ZONE, '1 month':$
                ) om_date_series ON (UPPER(om.station_no) = '{0}'::text AND DATE_TRUNC('month', om.lsd) = DATE_TRUNC('mont$
            GROUP BY
                year, month
        ) monthly ON (daily.year = monthly.year AND daily.month = monthly.month),
        (
         SELECT
                MIN(a.firstyear) AS firstyear,
                MAX(a.lastyear) AS lastyear
            FROM
                (
                    SELECT
                        MIN(EXTRACT(year FROM od.lsd)) AS firstyear,
                        MAX(EXTRACT(year FROM od.lsd)) AS lastyear
                    FROM
                        obs_daily od
                    WHERE
                        UPPER(station_no) = '{0}'::text
                    UNION
                    SELECT
                        MIN(EXTRACT(year FROM om.lsd)) AS firstyear,
                        MAX(EXTRACT(year FROM om.lsd)) AS lastyear
                    FROM
                        obs_monthly om
                    WHERE
                        UPPER(station_no) = '{0}'::text
                ) a
        ) years
    WHERE
        daily.year >= years.firstyear
        AND daily.year <= years.lastyear
        AND monthly.year >= years.firstyear
        AND monthly.year <= years.lastyear
    ORDER BY
        year, month;
    """.format(station, from_date, to_date)

    try:
#        table = pd.read_sql(query, conn, params={"station": str(station),
#                                                 "from_date": from_date,
#                                                 "to_date": to_date
#                                                }
#                           )
        table = pd.read_sql(query, conn)
        return table
    except Exception as e:
        print("Exception: " + str(e))
        print("Inputs {}".format(inputs))
        return None
    # finally:
    #     conn.close()  # Close the database connection


################################################################
def scopic(conn, station, from_date, to_date):
    """
    Execute a SQL query on the CliDE database to retrieve rainfall data for a specified station and date range.

    This function fetches daily rainfall data for a given station between the specified `from_date` and `to_date`.
    The query aggregates the data, calculates missing data counts, and formats the results.

    Note:
        This function is a simplified version of the original `scopic` function. It only retrieves daily rainfall data.
        There does not appear to be any data in the obs_monthly table in Samoa.

    Args:
        conn: Database connection object.
        station (str): Station identifier.
        from_date (str): Start date in 'YYYY-MM-DD' format.
        to_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: DataFrame containing the query results, or None if an exception occurs.

    Example:
        >>> conn = create_connection()
        >>> df = scopic(conn, 'STATION123', '2022-01-01', '2022-12-31')
        >>> print(df)
    """
    
    inputs = '{0}::{1}::{2}'.format(station, from_date, to_date)

    # Define the SQL query with station_no parameter using .format()
    query = """
    SELECT
        EXTRACT(year FROM od_date_series.date_seq) AS year,
        EXTRACT(month FROM od_date_series.date_seq) AS month,
        SUM(CASE WHEN od.rain_24h IS NULL THEN 0 ELSE 1 END) AS data_count,
        EXTRACT(day FROM (DATE_TRUNC('month', MIN(od.lsd) + '1 month'::INTERVAL) - '1 day'::INTERVAL)) AS days_in_month,
        CASE
            WHEN MIN(od.lsd) IS NOT NULL THEN missing_cons_rain('{0}'::text, TO_CHAR(MIN(od.lsd), 'yyyy'), TO_CHAR(MIN(od.lsd), 'mm'))
            ELSE 0
        END AS missing_cons,
        SUM(od.rain_24h) AS data
    FROM
        obs_daily od
    RIGHT JOIN
        (
            SELECT
                GENERATE_SERIES('{1}'::TIMESTAMP WITHOUT TIME ZONE, '{2}'::TIMESTAMP WITHOUT TIME ZONE, '1 day'::INTERVAL) AS date_seq
        ) od_date_series ON (UPPER(od.station_no) = '{0}'::text AND od.lsd = od_date_series.date_seq)
    GROUP BY
        year, month
    ORDER BY
        year, month;Thu Nov 28 09:30:01 AM +11 
        
    """.format(station, from_date, to_date)

    try:
        table = pd.read_sql(query, conn)
        return table
        
    except Exception as e:
        print(f"Exception: {e}")
        print(f"Inputs {inputs}")
        return None

    # finally:
    #     conn.close()  # Close the database connection
        
###############################################################################
def scopic_multi(conn, stations, from_date, to_date):

    dfs = []
    for stn in stations:
        df = scopic(conn, stn, from_date, to_date)
        df['station_no'] = stn
        dfs.append(df)       

    out_df = pd.concat(dfs)
    out_df.reset_index(inplace=True)
    out_df = out_df.dropna(subset=['data'])

    return out_df
        
def scopic_daily_data(conn, station, from_date, to_date):
    query = """
    SELECT
        EXTRACT(year FROM od_date_series.date_seq) AS year,
        EXTRACT(month FROM od_date_series.date_seq) AS month,
        SUM(CASE WHEN od.rain_24h IS NULL THEN 0 ELSE 1 END) AS data_count,
        EXTRACT(day FROM (DATE_TRUNC('month', MIN(od.lsd) + '1 month'::INTERVAL) - '1 day'::INTERVAL)) AS days_in_month,
        CASE
            WHEN MIN(od.lsd) IS NOT NULL THEN missing_cons_rain('{0}'::text, TO_CHAR(MIN(od.lsd), 'yyyy'), TO_CHAR(MIN(od.lsd), 'mm'))
            ELSE 0
        END AS missing_cons,
        SUM(od.rain_24h) AS data
    FROM
        obs_daily od
    RIGHT JOIN
        (
            SELECT
                GENERATE_SERIES('{1}'::TIMESTAMP WITHOUT TIME ZONE, '{2}'::TIMESTAMP WITHOUT TIME ZONE, '1 day'::INTERVAL) AS date_seq
        ) od_date_series ON (UPPER(od.station_no) = '{0}'::text AND od.lsd = od_date_series.date_seq)
    GROUP BY
        year, month;
    """.format(station, from_date, to_date)

    try:
        daily_data = pd.read_sql(query, conn)
        return daily_data
    except Exception as e:
        print(f"Exception: {e}")
        return None

def scopic_monthly_data(conn, station, from_date, to_date):
    query = """
    SELECT
        EXTRACT(year FROM om_date_series.date_seq) AS year,
        EXTRACT(month FROM om_date_series.date_seq) AS month,
        SUM(om.tot_rain) AS data
    FROM
        obs_monthly om
    RIGHT JOIN
        (
            SELECT
                GENERATE_SERIES('{1}'::TIMESTAMP WITHOUT TIME ZONE, '{2}'::TIMESTAMP WITHOUT TIME ZONE, '1 month'::INTERVAL) AS date_seq
        ) om_date_series ON (UPPER(om.station_no) = '{0}'::text AND DATE_TRUNC('month', om.lsd) = DATE_TRUNC('month', om_date_series.date_seq))
    GROUP BY
        year, month;
    """.format(station, from_date, to_date)

    try:
        monthly_data = pd.read_sql(query, conn)
        return monthly_data
    except Exception as e:
        print(f"Exception: {e}")
        return None

    
def scopic_years_data(conn, station):
    query = """
    SELECT
        MIN(a.firstyear) AS firstyear,
        MAX(a.lastyear) AS lastyear
    FROM
        (
            SELECT
                MIN(EXTRACT(year FROM od.lsd)) AS firstyear,
                MAX(EXTRACT(year FROM od.lsd)) AS lastyear
            FROM
                obs_daily od
            WHERE
                UPPER(station_no) = '{0}'::text
            UNION
            SELECT
                MIN(EXTRACT(year FROM om.lsd)) AS firstyear,
                MAX(EXTRACT(year FROM om.lsd)) AS lastyear
            FROM
                obs_monthly om
            WHERE
                UPPER(station_no) = '{0}'::text
        ) a;
    """.format(station)

    try:
        years_data = pd.read_sql(query, conn)
        return years_data
    except Exception as e:
        print(f"Exception: {e}")
        return None