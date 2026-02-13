# type: ignore
import logging
import os
import time

import dotenv
import matplotlib as mpl
import matplotlib.pyplot as plt
import mplcursors
import numpy as np
import pandas as pd
import scipy.stats

from DbUtils import *
from MyModel import *
from OtherUtils import *
from Types import *

dotenv.load_dotenv()

CON_STRING: str = os.getenv("DB_CONNECTION_STRING", "")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

plt.style.use('Solarize_Light2')

class Scheduler:
    # container for Scheduling related reports and analyses
    def valueDistribution(self) -> None:
        # distribution of schedule values across all events
        # fetch up sorted_evaluated.json and plot distribution of scheduleValue
        pd.read_json('Outputs/sorted_evaluated.json').plot(y='scheduleValue', kind='hist', bins=20, color='cyan', edgecolor='black')
        plt.title('Distribution of Schedule Values')
        plt.xlabel('Schedule Value')
        plt.ylabel('Frequency')
        plt.grid(axis='y', alpha=0.75)
        mplcursors.cursor(hover=True)
        plt.show()

        pass

    def reqVsShippedDate(self) -> None:
        fig, axes = plt.subplots(2, 2, figsize=(14, 6), sharex=True, sharey=True)

        # First plot: historical_rescheduled.json
        data1 = pd.read_json('Outputs/300-3-nolookback-goal1.json')
        data1['requestedShipDate'] = pd.to_datetime(data1['requestedShipDate'])
        data1['scheduledStartDate'] = pd.to_datetime(data1['scheduledStartDate'])
        data1 = data1.sort_values('requestedShipDate')
        axes[0, 0].scatter(data1['requestedShipDate'], data1['scheduledStartDate'], color='cyan')
        axes[0, 0].plot(data1['requestedShipDate'], data1['requestedShipDate'], color='red', linestyle='--')
        axes[0, 0].set_title('Goal 1: Maximize On-Time Deliveries')
        axes[0, 0].set_xlabel('Requested Ship Date')
        axes[0, 0].set_ylabel('Scheduled Start Date')
        axes[0, 0].grid()

        # Second plot: historical_rescheduled2.json
        data2 = pd.read_json('Outputs/200-6-nolookback-goal2-6hr+30min-received.json')
        data2['requestedShipDate'] = pd.to_datetime(data2['requestedShipDate'])
        data2['scheduledStartDate'] = pd.to_datetime(data2['scheduledStartDate'])
        data2 = data2.sort_values('requestedShipDate')
        axes[1, 0].scatter(data2['requestedShipDate'], data2['scheduledStartDate'], color='cyan')
        axes[1, 0].plot(data2['requestedShipDate'], data2['requestedShipDate'], color='red', linestyle='--')
        axes[1, 0].set_title('Goal 2: Maximize On-Time Deliveries with Heavy Penalty for Very Late')
        axes[1, 0].set_xlabel('Requested Ship Date')
        axes[1, 0].set_ylabel('Scheduled Start Date')
        axes[1, 0].grid()

        data3 = pd.read_json('Outputs/300-3-nolookback-goal3.json')
        data3['requestedShipDate'] = pd.to_datetime(data3['requestedShipDate'])
        data3['scheduledStartDate'] = pd.to_datetime(data3['scheduledStartDate'])
        data3 = data3.sort_values('requestedShipDate')
        axes[0, 1].scatter(data3['requestedShipDate'], data3['scheduledStartDate'], color='cyan')
        axes[0, 1].plot(data3['requestedShipDate'], data3['requestedShipDate'], color='red', linestyle='--')
        axes[0, 1].set_title('Goal 3: Minimize Lateness')
        axes[0, 1].set_xlabel('Requested Ship Date')
        axes[0, 1].set_ylabel('Scheduled Start Date')
        axes[0, 1].grid()

        data4 = pd.read_json('Outputs/200-6-nolookback-goal4-6hr+30min-received.json')
        data4['requestedShipDate'] = pd.to_datetime(data4['requestedShipDate'])
        data4['scheduledStartDate'] = pd.to_datetime(data4['scheduledStartDate'])
        data4 = data4.sort_values('requestedShipDate')
        axes[1, 1].scatter(data4['requestedShipDate'], data4['scheduledStartDate'], color='cyan')
        axes[1, 1].plot(data4['requestedShipDate'], data4['requestedShipDate'], color='red', linestyle='--')
        axes[1, 1].set_title('Goal 4: Maximize On-Time Deliveries with 6hr+30min Setup Time Penalty')
        axes[1, 1].set_xlabel('Requested Ship Date')
        axes[1, 1].set_ylabel('Scheduled Start Date')
        axes[1, 1].grid()
        
        mplcursors.cursor(hover=True)
        plt.tight_layout()
        plt.show()

    def moreDetailedModelAnalysis(self) -> None:
        data = pd.read_json('Outputs/t1.json')
        # data = pd.read_json('Outputs/scheduled_orders.json')
        data['requestedShipDate'] = pd.to_datetime(data['requestedShipDate'])
        data['scheduledStartDate'] = pd.to_datetime(data['scheduledStartDate'])
        data = data.sort_values('requestedShipDate')
        # plot scheduledStartDate vs requestedShipDate colored by machineId
        plt.scatter(data['requestedShipDate'], data['scheduledStartDate'], c=data['assignedMachineId'], cmap='tab10')
        plt.plot(data['requestedShipDate'], data['requestedShipDate'], color='red', linestyle='--')
        plt.title('Goal 4: Maximize On-Time Deliveries with 6hr+30min Setup Time Penalty')
        plt.xlabel('Requested Ship Date')
        plt.ylabel('Scheduled Start Date')
        # add legend for machineId colors
        cbar = plt.colorbar()
        cbar.set_label('Assigned Machine ID')
        
        plt.grid()
        mplcursors.cursor(hover=True)
        plt.show()

    def reqVsShippedDateActual(self) -> None:
        with getConnection(connectionString= CON_STRING.replace("?", "Data_Events")) as cnxn:
            data: pd.DataFrame = qryToDataFrame(
                cnxn= cnxn,
                query="""
                    SELECT 
                        date_OrderRequestedToShip as requestedDate, date_OrderShipped as shippedDate
                    FROM
                        Events_Order
                    WHERE
                        date_Creation between '01/01/2025' and '01/01/2026' 
                    AND
                        date_OrderRequestedToShip IS NOT NULL
                    AND
                        date_OrderShipped IS NOT NULL
                """
            )
            # query="""
            #         SELECT 
            #             cd_OrderRequestedToShip as requestedDate, date_Scheduled as scheduledDate
            #         FROM 
            #             Events
            #         WHERE
            #             date_Creation between '01/01/2025' and '01/01/2026' 
            #           AND
            #             date_RequestShip IS NOT NULL
            #           AND
            #             date_Shipped IS NOT NULL                    
            #     """

            data['requestedDate'] = pd.to_datetime(data['requestedDate'], errors='coerce')
            data['shippedDate'] = pd.to_datetime(data['shippedDate'], errors='coerce')
            data = data.sort_values('requestedDate')
            
            data.plot(x='requestedDate', y='shippedDate', kind='scatter', color='cyan')
            plt.title('Requested Ship Date vs Actual Shipped Date')
            plt.xlabel('Requested Ship Date')
            plt.ylabel('Actual Shipped Date')
            plt.grid()
            mplcursors.cursor(hover=True)
            #draw a line y=x for reference
            plt.plot(data['requestedDate'], data['requestedDate'], color='red', linestyle='--')
            plt.show()

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
            plt.bar(distribution['cn_ColorsTotal'], distribution['Count'], color='cyan')
            plt.xlabel('ColorsTotal')
            plt.ylabel('Count')
            plt.title('ColorsTotal Distribution')
            plt.xticks(distribution['cn_ColorsTotal'])
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            plt.show()
            
    except Exception as e:
        log.error(f"Error in colors: {str(e)}")

def main() -> None:
    try:
        with getConnection(connectionString= CON_STRING.replace("?", "Data_Events")) as cnxn:
            pass
    except Exception as e:
        log.error(f"Failed to fetch data: {e}")
        raise


if __name__ == "__main__":
    startTime: float = time.perf_counter()
    # colors()
    # main()

    # Scheduler().reqVsShippedDate()
    # Scheduler().reqVsShippedDateActual()
    Scheduler().moreDetailedModelAnalysis()
    endTime: float = time.perf_counter()
    log.info(f"Script Time: {(endTime - startTime):.4f}s")