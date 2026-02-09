import json
import os
from typing import Any, Dict, List

from dotenv import load_dotenv

from Types import TableProfile

load_dotenv()

PROFILES_PATH: str = os.path.normpath(os.getenv("PROFILES_JSON_PATH", ""))

def getColumnsWithSubstring(*, substring: str) -> Dict[str, Dict[str, Any]]:
    """
    Search for columns containing a specific substring across all table profiles.
    This function reads table profiles from a JSON file and searches for columns
    whose names contain the specified substring (case-insensitive).
    Args:
        substring (str): The substring to search for in column names. The search
            is case-insensitive.
    Returns:
        Dict[str, Dict[str, Any]]: A dictionary mapping table names to dictionaries
            containing 'columns' (list of matching column names) and 'rowCount'
            (number of rows in the table). Only tables with matching columns
            are included in the result.
    Example:
        >>> getColumnsWithSubstring(substring="id")
        {'users': {'columns': ['user_id', 'profile_id'], 'rowCount': 1000}, 
         'orders': {'columns': ['order_id', 'user_id'], 'rowCount': 5000}}
    Note:
        - The function reads from a global PROFILES_PATH constant
        - The search is case-insensitive
        - Returns an empty dictionary if no matches are found
    """
    matching_columns: Dict[str, Dict[str, Any]] = {}
    with open(PROFILES_PATH, "r", encoding="utf-8") as f:
        profiles: List[TableProfile] = json.load(f)
        for profile in profiles:
            table_name: str = profile["name"]
            for column in profile["columns"]:
                col_name: str = column["name"]
                if substring.lower() in col_name.lower():
                    if table_name not in matching_columns:
                        matching_columns[table_name] = {"columns": [], "rowCount": profile["rowCount"]}
                    matching_columns[table_name]["columns"].append(col_name)
        
        return matching_columns


def findTablesIncludingColumns(*, columnNames: List[str], matchAll: bool = False, showRows: bool = False) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Find database tables that contain columns matching the specified column names.
    This function searches through database profiles to identify tables containing columns
    whose names match (case-insensitive substring match) the provided column names.
    Args:
        columnNames: A list of column name substrings to search for. The search is
            case-insensitive and performs substring matching.
        matchAll: If True, only returns tables that contain matches for ALL column names
            in the columnNames list. If False, returns tables that contain matches for
            ANY of the column names.
    Returns:
        A nested dictionary with the following structure:
        {
            'database_name': {
                'table_name': {
                    'search_substring': ['matching_column1', 'matching_column2', ...],
                    ...
                },
                ...
            },
            ...
        }
        Where:
        - The outer key is the database name
        - The middle key is the table name
        - The inner dictionary maps each search substring to a list of actual column
          names in that table that matched the substring
    Example:
        >>> findTablesIncludingColumns_v2(columnNames=['user', 'email'], matchAll=True)
        {
            'mydb': {
                'users': {
                    'user': ['user_id', 'username'],
                    'email': ['email_address']
                }
            }
        }
    Note:
        Requires PROFILES_PATH to be defined and point to a valid JSON file containing
        database profile information.
    """
    matching_tables: Dict[str, Dict[str, Dict[str, Any]]] = {}
    with open(PROFILES_PATH, "r", encoding="utf-8") as f:
        profilesData: Dict[str, List[TableProfile]] = json.load(f)
        for db_name, profiles in profilesData.items():
            for profile in profiles:
                table_name: str = profile["name"]
                table_row_count: int = profile["rowCount"] if showRows else 0
                table_column_names: List[str] = [col["name"] for col in profile["columns"]]
                
                matches: Dict[str, List[str]] = {}
                for col_substring in columnNames:
                    matching_cols = [col for col in table_column_names 
                                   if col_substring.lower() in col.lower()]
                    if matching_cols:
                        matches[col_substring] = matching_cols
             
                should_include = len(matches) == len(columnNames) if matchAll else len(matches) > 0
                if should_include:
                    if db_name not in matching_tables:
                        matching_tables[db_name] = {}
                    matching_tables[db_name][table_name] = {
                        "rowCount": table_row_count,
                        "matches": matches
                    }
        
        return matching_tables


def showTablesJSON() -> List[str]:
    """Returns a list of table names from the profiles JSON file."""
    with open(PROFILES_PATH, "r", encoding="utf-8") as f:
        profiles: List[TableProfile] = json.load(f)
        return [profile["name"] for profile in profiles]
    
def safeCast(val: Any, toType: type, default: Any = None) -> Any:
    try:
        return toType(val)
    except (ValueError, TypeError):
        return default













