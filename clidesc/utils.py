# -*- coding: utf-8 -*-

"""
utils.py provides general utility functions to support writing new Python product generator scripts for CliDEsc.

Author:
    James Sturman, NIWA

Date:
    July 2025

Usage:
    import this module into your Python script and call the utility functions as needed.
"""

# modules
import sys
import io
import logging

from sqlalchemy import create_engine, inspect

import pickle
import sqlite3
from sqlite3 import Error

import numpy as np
from matplotlib import pyplot as plt
import pandas as pd
from PIL import Image

### =========================================================
# Functions

################################################################
def get_creds_r(r_file: str) -> dict:
    """
    Reads an R script file, interprets assignments, and returns variables.

    WARNING: This function uses exec() on file content. Only use with trusted files.

    Args:
        r_file (str): The path to the R script file containing variable assignments.

    Returns:
        dict: A dictionary containing the variables defined in the R script.
    """
    with open(r_file, 'r') as f:
        r_script_content = f.read()
    python_script_content = r_script_content.replace('<-', '=')
    result = {}
    try:
        exec(python_script_content, result)
    except Exception as e:
        logging.error(f"Error executing R script as Python: {e}")
        return {}
    result.pop('__builtins__', None)
    return result
  

def get_creds(file: str) -> dict:
    """
    get_creds function for obtaining CliDE credentials from a pickled (serialized) Python file or .pass text file.

    Args:
        file (str): Full path to a pickled .py file containing database credentials

    Returns:
        dict: A dictionary containing the credentials obtained from the file.
    """
    try:
        if file.endswith('.py'):
            with open(file, "rb") as f:
                credentials = pickle.load(f)
        elif file.endswith('.pass'):
            credentials = {}
            with open(file, 'r') as f:
                for line in f:
                    if '=' in line:
                        key, value = line.strip().split('=', 1)
                        credentials[key.strip()] = value.strip()
        else:
            raise ValueError("Unsupported credentials file type.")
        return credentials
    except Exception as e:
        logging.error(f"Exception in get_creds: {e}")
        return {}


################################################################
def db_open(database: str, user: str, password: str, dbhost: str = None, dbport: str = None):
    """
    Establish a connection to a CliDE or CliDEsc PostgreSQL database using SQLAlchemy.

    Args:
        database (str): Database name (for CliDE, usually 'clideDB').
        user (str): The username.
        password (str): The password.
        dbhost (str, optional): The host server name. Typically an IP address (e.g., 'XXX.XXX.XX.XX').
        dbport (str, optional): Port number (defaults to 5432 if not provided).

    Returns:
        sqlalchemy.engine.Engine or None: A database connection object as returned by create_engine(), or None if connection fails.

    For full SQLAlchemy Engine and Connection documentation see:
    https://docs.sqlalchemy.org/en/14/core/engines.html
    """

    # dialect+driver://username:password@host:port/database
    try:
        if not dbport and not dbhost:
            conn = create_engine(f"postgresql://{user}:{password}@localhost/{database}")
        elif not dbport:
            conn = create_engine(f"postgresql://{user}:{password}@{dbhost}/{database}")
        else:
            conn = create_engine(f"postgresql://{user}:{password}@{dbhost}:{dbport}/{database}")
        return conn
    except Exception as e:
        logging.error(f"Exception in db_open: {e}")
        return None


################################################################
def db_close(conn) -> None:
    """
    Close a SQLAlchemy database connection engine.

    Args:
        conn: A SQLAlchemy database connection object as returned by db_open().

    Returns:
        None

    For full SQLAlchemy Engine and Connection documentation see:
    https://docs.sqlalchemy.org/en/14/core/engines.html
    """

    try:
        conn.dispose()
    except Exception as e:
        logging.error(f"Exception in db_close: {e}")
    return None


################################################################
def listTables(conn) -> list:

    """
    Return a list of table names for the given database connection.

    Args:
        conn: A SQLAlchemy database connection object as returned by db_open().

    Returns:
        list: A list of database tables (including views).
    """

    # create inspecto object from db connection
    insp = inspect(conn)
    try:
        tablelist = insp.get_table_names(include_views=True)
        return tablelist
    except Exception as e:
        logging.error(f"Exception in listTables: {e}")
        return []


################################################################
def getColumns(conn, table: str) -> list:
    """
    Return the column names of a specified table.

    Args:
        conn: A SQLAlchemy database connection object as returned by utils.db_open().
        table (str): The table name (e.g. 'obs_daily', 'obs_subdaily').

    Returns:
        list: A list of column names.

    Example:
        columns = utils.getColumns(conn, 'obs_subdaily')
        columns = utils.getColumns(conn, 'obs_daily')
    """

    try:
        insp = inspect(conn)
        columns = [x['name'] for x in insp.get_columns(table)]
        return columns
    except Exception as e:
        logging.error(f"Exception in getColumns for {table}: {e}")
        return []


################################################################
def create_connection(db_file: str) -> None:

    """
    Create a SQLite database (or connect if it exists) and immediately close the connection.

    Args:
        db_file (str): Path to a SQLite database file.

    Returns:
        None
    """

    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        logging.error(f"SQLite error in create_connection: {e}")
    finally:
        if conn:
            conn.close()
    return None


################################################################
def db_table(db_file: str, sql: str) -> None:
    """
    Run a SQLite table query. Can be used to create, drop tables, or request data.

    Args:
        db_file (str): Path to the SQLite database file.
        sql (str): A SQL TABLE statement.

    Returns:
        None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        logging.error(f"SQLite error in db_table: {e}")
    finally:
        if conn:
            try:
                c = conn.cursor()
                c.execute(sql)
            except Error as e:
                logging.error(f"SQLite error executing SQL in db_table: {e}")
    return None


###############################################################
def db_table_query(db_file: str, sql: str, records: list) -> None:
    """
    Run a SQLite table query to insert, update, or delete data rows from tables.

    Args:
        db_file (str): Path to the SQLite database file.
        sql (str): SQL statement defining insert, update, or delete database operation.
        records (list): A list of tuples containing the data to be inserted, updated, or deleted.

    Returns:
        None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        logging.error(f"SQLite error in db_table_query: {e}")
    finally:
        if conn:
            try:
                c = conn.cursor()
                c.executemany(sql, records)
                conn.commit()
                conn.close()
            except Error as e:
                logging.error(f"SQLite error executing SQL in db_table_query: {e}")
    return None


################################################################
def make_thumbnail(srcfile: str, thumbfile: str, height: int, width: int) -> None:
    """
    Create a thumbnail image from a source image file and save it to a new file.

    Args:
        srcfile (str): Path to the source image file.
        thumbfile (str): Path where the thumbnail image will be saved.
        height (int): Maximum height of the thumbnail in pixels.
        width (int): Maximum width of the thumbnail in pixels.

    Returns:
        None
    """
    with Image.open(srcfile) as im:
        im.thumbnail((width, height), Image.Resampling.LANCZOS)
        im.save(thumbfile)


################################################################
def cm2inch(*tupl) -> tuple:
    """
    Convert centimeters to inches.

    Args:
        *tupl: A single tuple of numbers representing centimeters (e.g., (width_cm, height_cm)).

    Returns:
        tuple: The corresponding values in inches.

    Raises:
        ValueError: If no arguments are provided or input is not a tuple.

    Example:
        inches = cm2inch((29.7, 21.0))
    """
    inch = 2.54
    if len(tupl) == 0:
        raise ValueError("No arguments provided to cm2inch.")
    if isinstance(tupl[0], tuple):
        return tuple(i/inch for i in tupl[0])
    else:
        raise ValueError("Input to cm2inch must be a tuple.")


################################################################
def clidesc_Figure(size: str = 'A4'):
    """
    Initialize a matplotlib figure and axes with the correct size and DPI for reports.

    Args:
        size (str, optional): The desired figure size. Options are 'A4' (default), 'A5', or '16/12in'.

    Returns:
        tuple: (f, ax) where f is the matplotlib Figure and ax is the Axes object. Returns (None, None) if size is invalid.

    Example:
        f, ax = clidesc_Figure('A4')
    """
    if size == 'A4':
        figsize = cm2inch((29.7, 21.0))
        f, ax = plt.subplots(figsize=figsize, dpi=300)
    elif size == 'A5':
        figsize = cm2inch((21.0, 14.8))
        f, ax = plt.subplots(figsize=figsize, dpi=150)
    elif size == '16/12in':
        figsize = cm2inch((40.64, 30.48))  # 16in x 12in in cm
        f, ax = plt.subplots(figsize=figsize, dpi=75)
    else:
        f = None
        ax = None
    return f, ax


################################################################
def conform_calendar(df: pd.DataFrame, freq: str = 'D') -> pd.DataFrame:
    """
    Placeholder for a function to conform a DataFrame to a specified calendar frequency.

    Args:
        df (pd.DataFrame): Input DataFrame with a DatetimeIndex.
        freq (str, optional): Frequency string (e.g., 'D' for daily, 'M' for monthly). Default is 'D'.

    Returns:
        pd.DataFrame: DataFrame conformed to the specified calendar frequency.

    Raises:
        NotImplementedError: This function is not yet implemented.
    """
    raise NotImplementedError("conform_calendar is not yet implemented.")


################################################################
def calc_monthly_stat(df: pd.DataFrame, min_periods: int = 20, stat: str = 'mean') -> pd.Series:
    """
    Calculate monthly statistics (mean or sum) for a DataFrame, requiring a minimum number of non-NA values.

    Args:
        df (pd.DataFrame): Input DataFrame with a DatetimeIndex.
        min_periods (int, optional): Minimum number of non-NA values required for the statistic. Default is 20.
        stat (str, optional): Statistic to calculate ('mean' or 'sum'). Default is 'mean'.

    Returns:
        pd.Series: Series of monthly statistics, with NaN where insufficient data.

    Example:
        monthly_means = calc_monthly_stat(df, min_periods=20, stat='mean')
    """
    def agg_func(x):
        if x.notna().sum() >= min_periods:
            if stat == 'mean':
                return x.mean()
            elif stat == 'sum':
                return x.sum()
        return np.NaN
    return df.resample('M').apply(agg_func)


################################################################
def calc_daily_stat(df: pd.DataFrame, min_periods: float = 0.7, stat: str = 'mean') -> pd.Series:
    """
    Calculate daily statistics (mean or sum) for a DataFrame, requiring a minimum fraction of non-NA values.

    Args:
        df (pd.DataFrame): Input DataFrame with a DatetimeIndex.
        min_periods (float, optional): Minimum fraction of non-NA values required for the statistic (0-1). Default is 0.7.
        stat (str, optional): Statistic to calculate ('mean' or 'sum'). Default is 'mean'.

    Returns:
        pd.Series: Series of daily statistics, with NaN where insufficient data.

    Example:
        daily_means = calc_daily_stat(df, min_periods=0.7, stat='mean')
    """
    def agg_func(x):
        required = int(len(x) * min_periods)
        if x.notna().sum() >= required:
            if stat == 'mean':
                return x.mean()
            elif stat == 'sum':
                return x.sum()
        return np.NaN
    return df.resample('D').apply(agg_func)



