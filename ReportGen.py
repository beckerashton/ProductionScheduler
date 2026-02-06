# type: ignore
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import scipy.stats
from DbUtils import getConnection, getCursor, qryToDataFrame
import logging
import pandas as pd
import time
import os
import dotenv

dotenv.load_dotenv()

CON_STRING: str = os.getenv("DB_CONNECTION_STRING", "")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

plt.style.use('Solarize_Light2')

def colors() -> None:
    try:
        with getConnection(connectionString= CON_STRING.replace("?", "Data_Events")) as cnxn:
            distribution: pd.DataFrame = qryToDataFrame(
                cnxn= cnxn,
                query="""
                    SELECT 
                        cn_ColorsTotal, cd_OrderRequestedToShip
                    FROM 
                        Events
                    WHERE
                        date_Creation between '01/01/2025' and '01/01/2026' 
                      AND
                        cn_ColorsTotal between 1 AND 18                   
                """
            )

            distribution['MonthRequestedToShip'] = pd.to_datetime(distribution['cd_OrderRequestedToShip'], errors='coerce').dt.month
            correlation: float = distribution['cn_ColorsTotal'].corr(distribution['MonthRequestedToShip'])
            log.info(f"Correlation between ColorsTotal and MonthRequestedToShip: {correlation:.4f}")
            
            grouped_distribution = distribution.groupby('MonthRequestedToShip')['cn_ColorsTotal'].mean().reset_index()
            print(grouped_distribution)
            
            # bar chart 
            plt.bar(distribution['ColorsTotal'], distribution['Count'], color='cyan')
            plt.xlabel('ColorsTotal')
            plt.ylabel('Count')
            plt.title('ColorsTotal Distribution')
            plt.xticks(distribution['ColorsTotal'])
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            plt.show()
            
    except Exception as e:
        log.error(f"Error in colors: {str(e)}")
    

def main() -> None:
    try:
        with getConnection(connectionString= CON_STRING.replace("?", "Data_Events")) as cnxn:
            
    except Exception as e:
        log.error(f"Failed to fetch data: {e}")
        raise


if __name__ == "__main__":
    startTime: float = time.perf_counter()
    # colors()
    main()
    endTime: float = time.perf_counter()
    log.info(f"Script Time: {(endTime - startTime):.4f}s")