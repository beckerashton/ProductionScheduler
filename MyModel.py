

import json
import logging
import os
from datetime import date
from time import perf_counter
from typing import List

import dotenv

from DbUtils import getMachines, getUnscheduledOrders
from Types import Machine, ProductionEvent, WeekSchedule

dotenv.load_dotenv()

CON_STRING: str = os.getenv("DB_CONNECTION_STRING", "")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def fetchMachines() -> List[Machine]:
    try:
        with open("Outputs/machines.json", "r", encoding="utf-8") as mf:
            data: List[Machine] = json.load(mf)
            return [Machine.from_dict(item) for item in data]  # type: ignore (limitations of Dict based dataclass type hinting)          
    except:
        machines: List[Machine] = getMachines()
        writeMachinesToJson(machines)
        return machines

def writeMachinesToJson(machines: List[Machine]) -> None:
    with open("Outputs/machines.json", "w", encoding="utf-8") as mf:
        json.dump([machine.to_dict() for machine in machines], mf, indent=4)

def writeUnscheduledOrdersToJson(events: List[ProductionEvent]) -> None:
    with open("Outputs/unscheduled_orders.json", "w", encoding="utf-8") as f:
        json.dump([event.to_dict() for event in events], f, indent=4)

def DEBUG_loadUnscheduledOrdersFromJson() -> List[ProductionEvent]:
    try:
        with open('Outputs/unscheduled_orders.json', "r", encoding="utf-8") as f:
            data = json.load(f)
            return [ProductionEvent.from_dict(item) for item in data]
    except Exception as e:
        print(f"Error loading unscheduled orders from JSON: {str(e)}")
        return []

# Testing function
def showValues(orders: List[ProductionEvent], file: str | None = None) -> None:
    s = sorted(orders, key=lambda e: e.scheduleValue if e.scheduleValue is not None else 0, reverse=True)
    if file:
        with open(file, "w", encoding="utf-8") as f:
            json.dump([event.to_dict() for event in s], f, indent=4)
    else:
        for event in s:
            print(event)

class SchedulingAgent:
    machines: List[Machine]

    def __init__(self, machines: List[Machine]):
        # sort machines by heads capacity descending
        self.machines = sorted(machines, key=lambda m: m.heads, reverse=True)

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

    # Note that this mutates the events list by setting scheduleValue on each event, the return value is just for convenience
    def evaluateAllEvents(self, events: List[ProductionEvent], sort: bool = True) -> List[ProductionEvent]:
        for event in events:
            event.scheduleValue = self.calculateScheduleValue(event)
        if sort:
            events.sort(key=lambda e: e.scheduleValue if e.scheduleValue is not None else 0, reverse=True)
        return events

    def assignAllMachineSchedules(self, newSchedule: WeekSchedule, save: bool = False) -> None:
        for machine in self.machines:
            machine.schedule = newSchedule
        if save:
            writeMachinesToJson(self.machines)

    # For scheduling to be optimized they need to be batched together (later implementation for singles will just be a forced insertion)
    def scheduleEvents(self, events: List[ProductionEvent]) -> List[ProductionEvent]:
        # Assumes events have been (1) evaluated and (2) sorted by scheduleValue descending
        # Naive algo goes as so
        # Cycle through each machine from highest capacity to lowest
        # Each machine check will do a check top down through values and pick the biggest thing it can fit wthin the top X
        # (Add some data analysis later to ensure this doesn't prio vortex simple jobs)
        #
        # Problems:
        # Does not consider how long events will take
        # No hard prio vortex prevention
        window: int = 10  # how many of the highest value jobs to consider for fitting
        assignedEvents: List[ProductionEvent] = []
        while events:
            if len(events) == 3:
                log.info("Three events remain: " + ", ".join(str(e) for e in events))
                break
            # log.info(f"Scheduling pass, {len(events)} events remaining.")
            for machine in self.machines:
                topEvents = events[:window] # Top X events by valu
                fittingEvents = [e for e in topEvents if e.headsTotal <= machine.heads] # Filter by if they fit
                fittingEvents.sort(key= lambda e: e.headsTotal, reverse= True) # Sort by heads desc
                if fittingEvents: # something fits
                    selectedEvent = fittingEvents[0]
                    selectedEvent.assignedMachineId = machine.machineId
                    assignedEvents.append(selectedEvent)
                    events.remove(selectedEvent)
                else: # nothing fits, just grab next 
                    # log.info(f"No fitting events for machine {machine.machineName} (heads: {machine.heads}) in top {window} events.")
                    for e in topEvents:
                        if e.headsTotal <= machine.heads:
                            e.assignedMachineId = machine.machineId
                            assignedEvents.append(e)
                            events.remove(e)
        
        return assignedEvents

if __name__ == "__main__":
    opStart = perf_counter()

    # writeUnscheduledOrdersToJson(getUnscheduledOrders(lookBackRange= 30, lookAheadRange= 30)) # fetch up to 3 months of orders and write to json for testing

    newSchedule = WeekSchedule(startDate= date.today(), hours= 10)

    machines: List[Machine] = fetchMachines() # fetch up static machine data
    unscheduledOrders: List[ProductionEvent] = DEBUG_loadUnscheduledOrdersFromJson() # fetch static uunscheduled orders from json

    agent = SchedulingAgent(machines)
    agent.assignAllMachineSchedules(newSchedule)
    agent.evaluateAllEvents(unscheduledOrders)
    scheduledOrders = agent.scheduleEvents(unscheduledOrders)
    
    showValues(scheduledOrders, 'Outputs/sorted_evaluated.json')

    print(f"Execution time: {perf_counter() - opStart:.2f} seconds")