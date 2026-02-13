import json
import logging
import os
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, Generator, List, Optional

import pandas as pd
import pyodbc as odbc
from dotenv import load_dotenv
from pandas import DataFrame
from pyodbc import Connection, Cursor, Row

from OtherUtils import safeCast as sc
from Types import Machine, ProductionEvent, TableProfile

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

load_dotenv()

PROFILES_PATH: str = os.path.normpath(os.getenv("PROFILES_JSON_PATH", ""))
CON_STRING: str = os.getenv("DB_CONNECTION_STRING", "")
DB_LIST: List[str] = json.loads(os.getenv("DB_LIST", "[]"))


def initTableProfile(*, name: str = "", rowCount: int = -1, columns: Optional[List[Dict[str, str]]] = None, dateRefreshed: str = "") -> TableProfile:
    """
    Initialize a TableProfile object with the provided parameters.

    Args:
        name (str, optional): The name of the table. Defaults to "".
        rowCount (int, optional): The number of rows in the table. Defaults to -1.
        columns (Optional[List[Dict[str, str]]], optional): A list of dictionaries containing column information. Defaults to None.
        dateRefreshed (str, optional): The date when the table was last refreshed. Defaults to "".

    Returns:
        TableProfile: A TableProfile object initialized with the provided parameters.
    """
    if columns is None:
        columns = []
    return TableProfile(name= name, rowCount= rowCount, columns= columns, dateRefreshed= dateRefreshed)


@contextmanager
def getConnection(*, connectionString: str) -> Generator[Connection, None, None]:
    """
    Context manager that establishes and manages a database connection.

    Args:
        connectionString (str): The ODBC connection string used to establish the database connection.
            Must be provided as a keyword argument.

    Yields:
        Connection: An active ODBC database connection object.

    Raises:
        odbc.Error: If the connection attempt fails.

    Example:
        >>> with getConnection(connectionString="DRIVER={...};SERVER=...") as conn:
        ...     cursor = conn.cursor()
        ...     cursor.execute("SELECT * FROM table")

    Note:
        The connection is automatically closed when exiting the context manager,
        regardless of whether an exception occurred.
    """
    cnxn: Optional[Connection] = None
    try:
        cnxn = odbc.connect(connectionString)
        log.debug("Connection secured")
        yield cnxn
    except odbc.Error as e:
        log.error(f"Connection failed: {e}")
        raise
    finally:
        if cnxn:
            cnxn.close()
            log.debug("Connection closed")


@contextmanager
def getCursor(*, cnxn: Connection) -> Generator[Cursor, None, None]:
    """
    Context manager that provides a database cursor and ensures proper cleanup.

    This function is a generator-based context manager that yields a cursor from
    the provided database connection. It guarantees that the cursor will be closed
    after use, even if an exception occurs.

    Args:
        cnxn (Connection): A database connection object from which to create a cursor.
                          Must be passed as a keyword argument.

    Yields:
        Cursor: A database cursor object that can be used to execute SQL queries.

    Example:
        with getCursor(cnxn=connection) as cursor:
            cursor.execute("SELECT * FROM table")
            results = cursor.fetchall()

    Note:
        - The cursor is automatically closed when exiting the context manager.
        - Debug logs are generated when the cursor is opened and closed.
    """
    crs: Optional[Cursor] = None
    try:
        crs = cnxn.cursor()
        log.debug("Cursor opened")
        yield crs
    finally:
        if crs is not None:
            crs.close()
            log.debug("Cursor closed")


def showTablesDB(*, cnxn: Connection) -> List[str]:
    """
    Retrieve and return a list of all table names in the connected database.

    This function uses a database connection to query the database metadata
    and fetch the names of all tables available in the database.

    Args:
        cnxn (Connection): An active database connection object used to query
            the database for table information.

    Returns:
        List[str]: A list of table names present in the database.
    """
    table_names: List[str] = []
    with getCursor(cnxn= cnxn) as crs:
        for table in crs.tables():
            table_names.append(str(table[2]))
    return table_names


def getColsTypes(*, cnxn: Connection, table: str) -> Generator[tuple[str, str], None, None]:
    """
    Retrieve column names and their data types from a specified database table.

    This function queries the database metadata to get information about columns
    in the specified table and yields tuples containing column names and their
    corresponding data types.

    Args:
        cnxn (Connection): An active database connection object used to query
            the table metadata.
        table (str): The name of the table to retrieve column information from.

    Yields:
        tuple[str, str]: A tuple containing (column_name, column_type) for each
            column in the specified table.

    Raises:
        odbc.Error: Logs an error message if the table is not found or if there's
            an issue accessing the table metadata.

    Returns:
        None: Returns early if an odbc.Error is encountered.

    Example:
        >>> for col_name, col_type in getColsTypes(cnxn=connection, table="users"):
        ...     print(f"{col_name}: {col_type}")
    """
    with getCursor(cnxn= cnxn) as crs:
        try: 
            crs.columns(table= table)
            for ct in crs:
                yield (str(ct[3]), str(ct[5]))
        except odbc.Error as e:
            log.error(f"Table [{table}] not found: {e}")
            return


def getProfile(*, cnxn: Connection, table: str) -> TableProfile:
    """
    Retrieves profiling information for a specified database table.
    This function connects to a database table and extracts metadata including
    the row count and column information (names and types). The profile is
    timestamped with the current date and time.
    Args:
        cnxn (Connection): An active database connection object.
        table (str): The name of the table to profile.
    Returns:
        TableProfile: A dictionary-like object containing:
            - name: The table name
            - dateRefreshed: Timestamp when the profile was generated
            - rowCount: Number of rows in the table (-1 if error occurred)
            - columns: List of dictionaries with 'name' and 'type' for each column
    Raises:
        No exceptions are raised. ODBC errors are caught and logged, resulting
        in a profile with rowCount of -1 and empty columns list.
    Note:
        Uses a cursor context manager via getCursor() to ensure proper resource cleanup.
        Errors during profiling are logged but do not interrupt execution.
    """
    
    with getCursor(cnxn= cnxn) as crs:
        profile: TableProfile = initTableProfile(name= table, dateRefreshed= str(datetime.now()))
        try:
            crs.execute(f'SELECT COUNT(*) FROM "{table}"')
            if res := crs.fetchone():
                profile["rowCount"] = res[0]
            
            for ct in getColsTypes(cnxn= cnxn, table= table):
                profile["columns"].append({"name": ct[0], "type": ct[1]})
        except odbc.Error as e:
            log.error(f"Failed to profile table [{table}]: {e}")
            profile["rowCount"] = -1
            profile["columns"] = []
    
    return profile


def getTableProfiles(*, cnxn: Connection) -> List[TableProfile]:
    """
    Refresh and update database table profiles by scanning all tables in the database.
    This function iterates through all tables in the database connection, profiles each table,
    and saves the collected profile data to a JSON file.
    Args:
        cnxn (Connection): A database connection object used to access the database tables.
    Returns:
        List[TableProfile]: A list of table profiles if profiles were successfully written to the JSON file, an empty list if an 
              IOError occurred during the write operation.
    Raises:
        IOError: Logged (not raised) when there's an error writing to the JSON file.
    Side Effects:
        - Writes profile data to the file specified by PROFILES_PATH
        - Logs debug messages for each table being profiled
        - Logs debug message on successful write
        - Logs error message if write fails
    """
    dataContainer: List[TableProfile] = []

    with getCursor(cnxn= cnxn) as crs:
        for table in crs.tables():
            dataContainer.append(getProfile(cnxn= cnxn, table= table[2]))
    
    return dataContainer


def qryToDataFrame(*, cnxn: Connection, query: str) -> DataFrame:
    """
    Execute a SQL query and return the results as a pandas DataFrame.
    Args:
        cnxn (Connection): A database connection object used to execute the query.
        query (str): The SQL query string to be executed.
    Returns:
        pd.DataFrame: A DataFrame containing the results of the executed query.
    Note:
        This function uses a cursor context manager to execute the query and
        fetch results. It assumes that pandas is imported as pd.
    """
    with getCursor(cnxn= cnxn) as crs:
        crs.execute(query)
        rows: List[Row] = crs.fetchall()
        cols: List[str] = [col[0] for col in crs.description]
        
        df = pd.DataFrame.from_records(rows, columns= cols) # type: ignore
        if df.empty:
            log.warning("Query returned no results.")

        return df
    

def peek(*, cnxn: Connection, table: str) -> None:
    """
    Peek at the top 100 rows of a database table by exporting to a temporary CSV file and opening it in Excel.
    Args:
        cnxn (Connection): A database connection object used to execute the query.
        table (str): The name of the table to peek at.
    Side Effects:
        - Creates a temporary CSV file with the top 100 rows of the specified table.
        - Opens the CSV file in Microsoft Excel for viewing.
    Note:
        This function assumes that Microsoft Excel is installed and accessible via the command line.
    """
    query: str = f"SELECT * FROM {table} ORDER BY date_Creation DESC FETCH FIRST 100 ROWS ONLY"
    df: DataFrame = qryToDataFrame(cnxn= cnxn, query= query)

    with tempfile.NamedTemporaryFile(delete= False, suffix= ".csv") as tmp_file:
        temp_csv_path: str = tmp_file.name
        df.to_csv(temp_csv_path, index= False)
        log.debug(f"Temporary CSV created at {temp_csv_path}")
    
    subprocess.Popen(["start", "excel.exe", temp_csv_path], shell= True)
    log.debug("Excel opened with temporary CSV")


def forEachDB(func: Callable[..., Any], *args: Any, collect_results: bool = False, **kwargs: Any) -> Optional[Dict[str, Any]]:
    """
    Execute an arbitrary function across all databases in DB_LIST.
    
    This function iterates through all databases defined in the DB_LIST environment variable,
    establishes a connection to each one, and executes the provided function with that connection.
    
    Args:
        func (Callable[..., Any]): A function to execute for each database. The function must
            accept 'cnxn' as its first keyword argument (a Connection object).
        *args: Positional arguments to pass to the function (after cnxn).
        collect_results (bool, optional): If True, collects and returns the results from each
            database execution. Defaults to False.
        **kwargs: Additional keyword arguments to pass to the function.
    
    Returns:
        Optional[Dict[str, Any]]: If collect_results is True, returns a dictionary mapping
            database names to their respective results. Returns None otherwise.
    
    Raises:
        ValueError: If DB_LIST or CON_STRING are not properly configured.
    
    Example:
        >>> # Execute refreshProfiles on all databases
        >>> forEachDB(refreshProfiles)
        
        >>> # Execute a custom function with additional parameters
        >>> def customFunc(*, cnxn: Connection, table: str) -> int:
        ...     # Your logic here
        ...     return some_value
        >>> results = forEachDB(customFunc, table="MyTable", collect_results=True)
        
        >>> # Use peek function across all databases
        >>> forEachDB(peek, table="Customers")
    
    Note:
        - The CON_STRING must contain a '?' placeholder that will be replaced with each database name
        - If a database fails, the error is logged and execution continues with the next database
        - The function will log info messages for each database being processed
    """
    if not DB_LIST:
        log.error("DB_LIST is empty or not configured")
        raise ValueError("DB_LIST must be configured with at least one database")
    
    if not CON_STRING or "?" not in CON_STRING:
        log.error("CON_STRING is not properly configured")
        raise ValueError("CON_STRING must be configured with a '?' placeholder for database name")
    
    results: Dict[str, Any] = {} if collect_results else {}
    
    for dbName in DB_LIST:
        try:
            connStr = CON_STRING.replace("?", dbName)
            
            with getConnection(connectionString=connStr) as cnxn:
                result = func(*args, cnxn=cnxn, **kwargs)
                
                if collect_results:
                    results[dbName] = result
            
        except Exception as e:
            log.error(f"Failed to process database {dbName}: {e}")
            if collect_results:
                results[dbName] = [{"error": str(e)}]
    
    return results if collect_results else None


def refreshProfiles() -> bool:
    """
    Refreshes and saves table profiles from all databases to a JSON file.
    This function retrieves table profiles from all databases using the forEachDB
    helper function, then saves the collected profile data to a JSON file specified
    by PROFILES_PATH.
    Returns:
        bool: True if profiles were successfully collected and written to file,
              False if no data was collected (dataContainer is None).
    Raises:
        IOError: Logged as an error if writing to the JSON file fails, but does
                 not propagate the exception.
    Side Effects:
        - Writes profile data to the file specified by PROFILES_PATH
        - Logs debug message on successful write
        - Logs error message if file write fails
    """
    dataContainer: Optional[Dict[str, List[TableProfile]]] = forEachDB(getTableProfiles, collect_results= True)

    if dataContainer is not None:
        try:
            with open(PROFILES_PATH, 'w') as json_file:
                json.dump(dataContainer, json_file, indent=4)
            log.debug(f"Write successful to {PROFILES_PATH}")
        except IOError as e:
            log.error(f"Error writing to json: {e}")
        return True 
    return False


def getMachines() -> List[Machine]: 
    with getConnection(connectionString= CON_STRING.replace("?", "Data_Events")) as cnxn:
        df = qryToDataFrame(cnxn= cnxn, query="""
            SELECT
                ID_Machine,
                MachineName,
                MaxNumberOfColors
            FROM
                Events_Machine
            WHERE
                ID_Machine IN ('101', '102', '103', '104', '105', '106', '107')
        """)
        machines: List[Machine] = [Machine(machineId= row['ID_Machine'], machineName= row['MachineName'], heads= row['MaxNumberOfColors']) for _, row in df.iterrows()]
        machines.sort(key= lambda m: m.heads, reverse= True)
        machines[0].heads = 30 # temp hack to fix issues with some orders being bigger than the stryker capacity
    return machines

def getUnscheduledOrders(lookBackRange: int, lookAheadRange: int) -> List[ProductionEvent]:
    query: str = f"""
        SELECT
            eodl.id_Order,
            eod.ct_DesignName,
            eodl.Location,
            eodl.ColorsTotal,
            eodl.FlashesTotal,
            eodl.cn_QtyToProduce,
            eo.date_OrderRequestedToShip
        FROM 
            Events_OrderDesLoc eodl
        INNER JOIN 
            Events_Order eo ON eodl.id_Order = eo.ID_Order
        INNER JOIN
            Events_OrderDes eod ON eodl.id_Order = eod.id_Order AND eod.id_DesignType = 1
        WHERE
            eodl.date_Creation >= '01/01/2025'
            AND eo.date_OrderRequestedToShip >= '{(date.today() - timedelta(days=lookBackRange)).strftime("%m/%d/%Y")}'
            AND eo.date_OrderRequestedToShip <= '{(date.today() + timedelta(days=lookAheadRange)).strftime("%m/%d/%Y")}'
            AND eodl.ColorsTotal > 0
            AND eodl.cn_QtyToProduce > 0
            AND eo.id_OrderType = 11
            AND eo.cn_sts_HoldOrder = 0
            AND eo.sts_ArtDone = 1
            AND eo.sts_Purchased = 1
            
            AND eo.id_SalesStatus IN (0, 1)
        ORDER BY
            eo.date_OrderRequestedToShip ASC
    """
    # AND eo.sts_Received = 1
    try:
        with getConnection(connectionString= CON_STRING.replace("?", "Data_Events")) as cnxn:
            df = qryToDataFrame(cnxn= cnxn, query= query)
            events: List[ProductionEvent] = []
            for _, row in df.iterrows():
                event = ProductionEvent(
                    orderId=sc(row['id_Order'], int),
                    orderDesignName=sc(row['ct_DesignName'], str),
                    printLocation=sc(row['Location'], str),
                    colorsTotal=sc(row['ColorsTotal'], int, 0),
                    flashesTotal=sc(row['FlashesTotal'], int, 0),
                    quantity=sc(row['cn_QtyToProduce'], int, 0),
                    priority=0,
                    requestedShipDate=row['date_OrderRequestedToShip']
                )
                events.append(event)
            return events
    except Exception as e:
        print(f"Error fetching unscheduled orders: {str(e)}")
        return []

def getHistoricalScheduledOrders(minDate: date = date(2025, 1, 1), maxDate: date = date(2026, 1, 1)) -> List[ProductionEvent]:
    query: str = f"""
        SELECT
            eodl.id_Order,
            eod.ct_DesignName,
            eodl.Location,
            eodl.ColorsTotal,
            eodl.FlashesTotal,
            eodl.cn_QtyToProduce,
            eo.date_OrderRequestedToShip
        FROM 
            Events_OrderDesLoc eodl
        INNER JOIN 
            Events_Order eo ON eodl.id_Order = eo.ID_Order
        INNER JOIN
            Events_OrderDes eod ON eodl.id_Order = eod.id_Order AND eod.id_DesignType = 1
        WHERE
            eodl.date_Creation >= '{minDate.strftime("%m/%d/%Y")}'
            AND eo.date_OrderRequestedToShip >= '{minDate.strftime("%m/%d/%Y")}'
            AND eo.date_OrderRequestedToShip <= '{maxDate.strftime("%m/%d/%Y")}'
            AND eodl.ColorsTotal > 0
            AND eodl.cn_QtyToProduce > 0
            AND eo.id_OrderType = 11
            AND eo.sts_Shipped = 1
        ORDER BY
            eo.date_OrderRequestedToShip ASC
    """

    try:
        with getConnection(connectionString= CON_STRING.replace("?", "Data_Events")) as cnxn:
            df = qryToDataFrame(cnxn= cnxn, query= query)
            events: List[ProductionEvent] = []
            for _, row in df.iterrows():
                event = ProductionEvent(
                    orderId=sc(row['id_Order'], int),
                    orderDesignName=sc(row['ct_DesignName'], str),
                    printLocation=sc(row['Location'], str),
                    colorsTotal=sc(row['ColorsTotal'], int, 0),
                    flashesTotal=sc(row['FlashesTotal'], int, 0),
                    quantity=sc(row['cn_QtyToProduce'], int, 0),
                    priority=0,
                    requestedShipDate=row['date_OrderRequestedToShip']
                )
                events.append(event)
            return events
    except Exception as e:
        print(f"Error fetching historical scheduled orders: {str(e)}")
        return []
    
