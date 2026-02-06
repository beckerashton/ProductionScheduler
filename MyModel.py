

from datetime import date, timedelta
import json
from time import perf_counter
from typing import List
from ProductionTypes import *
from DbUtils import getConnection, getMachines, qryToDataFrame
from OtherUtils import safeCast as sc

import os
import dotenv

dotenv.load_dotenv()

CON_STRING: str = os.getenv("DB_CONNECTION_STRING", "")

def fetchMachines() -> List[Machine]:
    try:
        with open("machines.json", "r", encoding="utf-8") as mf:
            data: List[Machine] = json.load(mf)
            return [Machine.from_dict(item) for item in data]
    except Exception as e:
        machines: List[Machine] = getMachines()
        writeMachinesToJson(machines)
        return machines

def writeMachinesToJson(machines: List[Machine]) -> None:
    with open("machines.json", "w", encoding="utf-8") as mf:
        json.dump([machine.to_dict() for machine in machines], mf, indent=4)

def fetchUnscheduledOrders(lookBackRange: int, lookAheadRange: int) -> List[ProductionEvent]:
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
            AND eo.sts_Received = 1
            AND eo.id_SalesStatus IN (0, 1)
        ORDER BY
            eo.date_OrderRequestedToShip ASC
    """
    # AND eo.date_OrderRequestedToShip >= '{(date.today() - timedelta(days=lookBackRange)).isoformat()}'
    #         AND eo.date_OrderRequestedToShip <= '{(date.today() + timedelta(days=lookAheadRange)).isoformat()}'

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

def DEBUG_loadUnscheduledOrdersFromJson() -> List[ProductionEvent]:
    try:
        with open('Outputs/unscheduled_orders.json', "r", encoding="utf-8") as f:
            data = json.load(f)
            return [ProductionEvent.from_dict(item) for item in data]
    except Exception as e:
        print(f"Error loading unscheduled orders from JSON: {str(e)}")
        return []

class SchedulingAgent:
    machines: List[Machine]

    def __init__(self, machines: List[Machine]):
        self.machines = machines

    # now that im thinking about it this could just be a parameter in the dataclass but ill leave it here for now
    def calculateScheduleValue(self, event: ProductionEvent) -> int:
        # This whole function is arbitrary and experimental - just a starting point for testing scheduling logic
        # some other factors to consider at some point are
        #  - similar designs running to encourage grouping? maybe different stricter system though
        #  - limitations of machines
        daysOut = (event.requestedShipDate - date.today()).days * 2
        latenessBonus = max(0, -daysOut) * 10  # Arbitrary bonus for lateness
        complexityBonus = event.headsTotal * 5  # Arbitrary bonus for more complex jobs
        return event.priority * 100 - daysOut + latenessBonus + complexityBonus

    def evaluateAllEvents(self, events: List[ProductionEvent]) -> None:
        for event in events:
            event.scheduleValue = self.calculateScheduleValue(event)

    def showValues(self, file: str | None = None) -> None:
        s = sorted(unscheduledOrders, key=lambda e: e.scheduleValue if e.scheduleValue is not None else 0, reverse=True)
        if file:
            with open(file, "w", encoding="utf-8") as f:
                json.dump([event.to_dict() for event in s], f, indent=4)
        else:
            for event in s:
                print(event)

    def scheduleEvent(self, event: ProductionEvent) -> None:
        pass

if __name__ == "__main__":
    opStart = perf_counter()

    machines: List[Machine] = fetchMachines() # fetch up static machine data
    unscheduledOrders: List[ProductionEvent] = DEBUG_loadUnscheduledOrdersFromJson()

    agent = SchedulingAgent(machines)
    agent.evaluateAllEvents(unscheduledOrders)

    agent.showValues('Outputs/sorted_evaluated.json')

    print(f"Execution time: {perf_counter() - opStart:.2f} seconds")