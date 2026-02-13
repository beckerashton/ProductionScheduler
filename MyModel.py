

import bisect
import json
import logging
import math
import os
from datetime import date, timedelta
from time import perf_counter
from typing import Dict, List, Optional, Tuple

import dotenv

from DbUtils import getHistoricalScheduledOrders, getMachines, getUnscheduledOrders #type: ignore
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

def writeUnscheduledOrdersToJson(file: str, events: List[ProductionEvent]) -> None:
    with open(file, "w", encoding="utf-8") as f:
        json.dump([event.to_dict() for event in events], f, indent=4)

def DEBUG_loadUnscheduledOrdersFromJson(file: str, filterOld: bool = False) -> List[ProductionEvent]:
    try:
        import pandas as pd
        with open(file, "r", encoding="utf-8") as f:
            # return [ProductionEvent.from_dict(item) for item in json.load(f)] where if filterOld is true we filter out events with requestedShipDate in the past
            data = json.load(f)
            events = [ProductionEvent.from_dict(item) for item in data]
            if filterOld:
                today = date.today()
                events = [event for event in events if event.requestedShipDate >= today]
            return events
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

    def _duration_minutes(self, event: ProductionEvent) -> int:
        hours = event.quantity / 200 + event.headsTotal / 6 + 0.5
        return max(1, math.ceil(hours * 60))

    def _get_machine_schedule(self, machine: Machine) -> WeekSchedule:
        if machine.schedule is not None:
            return machine.schedule
        return WeekSchedule(startDate=date.today(), hours=8)

    def _build_work_calendar(self, schedule: WeekSchedule, start: date, end: date) -> Tuple[List[Tuple[date, int, int]], List[int]]:
        day_flags = schedule.week
        minutes_per_day = schedule.hours * 60
        entries: List[Tuple[date, int, int]] = []
        end_minutes: List[int] = []
        total = 0
        days = (end - start).days
        for offset in range(days + 1):
            current = start + timedelta(days=offset)
            if day_flags[current.weekday()]:
                day_start = total
                day_end = total + minutes_per_day
                entries.append((current, day_start, day_end))
                end_minutes.append(day_end)
                total = day_end
        return entries, end_minutes

    def _due_minute(self, due_date: date, entries: List[Tuple[date, int, int]], end_minutes: List[int]) -> int:
        if not entries:
            return 0
        dates = [entry[0] for entry in entries]
        idx = bisect.bisect_right(dates, due_date) - 1
        if idx < 0:
            return 0
        return end_minutes[idx]

    def _work_minutes_to_date(self, minute: int, entries: List[Tuple[date, int, int]], end_minutes: List[int]) -> Optional[date]:
        if not entries:
            return None
        idx = bisect.bisect_right(end_minutes, minute)
        if idx >= len(entries):
            return entries[-1][0]
        return entries[idx][0]

    def scheduleEventsCpSat(self, events: List[ProductionEvent], time_limit_sec: int = 10) -> List[ProductionEvent]:
        try:
            from ortools.sat.python import cp_model
        except Exception as exc:
            raise RuntimeError("OR-Tools is required for CP-SAT scheduling. Install with 'pip install ortools'.") from exc

        if not events:
            return []

        # Defines the the scheduling horizon as the time until the latest due date plus the sum of all expected production time
        max_due = max(event.requestedShipDate for event in events)
        total_minutes = sum(self._duration_minutes(event) for event in events)
        total_capacity_per_week = 0
        for machine in self.machines:
            schedule = self._get_machine_schedule(machine)
            work_days_per_week = max(1, schedule.daysScheduledCount)
            total_capacity_per_week += schedule.hours * 60 * work_days_per_week

        weeks_needed = math.ceil(total_minutes / max(1, total_capacity_per_week))
        horizon_end = max_due + timedelta(days=weeks_needed * 7)

        # Translate human readable schedules into minute-based continuous calendars for each machine
        machine_calendars: Dict[int, Tuple[List[Tuple[date, int, int]], List[int], int]] = {}
        max_horizon = 0
        for m_idx, machine in enumerate(self.machines):
            schedule = self._get_machine_schedule(machine)
            entries, end_minutes = self._build_work_calendar(schedule, schedule.startDate, horizon_end)
            horizon = end_minutes[-1] if end_minutes else schedule.hours * 60
            machine_calendars[m_idx] = (entries, end_minutes, horizon)
            max_horizon = max(max_horizon, horizon)

        # Initialize CP-SAT model variables
        model = cp_model.CpModel()
        machine_ids: List[int] = [machine.machineId for machine in self.machines]
        machine_index: Dict[int, int] = {mid: idx for idx, mid in enumerate(machine_ids)}

        interval_vars: Dict[Tuple[int, int], cp_model.IntervalVar] = {}
        start_vars: Dict[Tuple[int, int], cp_model.IntVar] = {}
        end_vars: Dict[Tuple[int, int], cp_model.IntVar] = {}
        presence_vars: Dict[Tuple[int, int], cp_model.BoolVar] = {}

        event_end_vars: Dict[int, cp_model.IntVar] = {}
        event_start_vars: Dict[int, cp_model.IntVar] = {}
        assigned_machine_vars: Dict[int, cp_model.IntVar] = {}
        lateness_vars: Dict[int, cp_model.IntVar] = {}

        # 
        for i, event in enumerate(events):
            duration = self._duration_minutes(event)
            event.estTime = duration  # Store estimated time
            eligible_machines = [m for m in self.machines if event.headsTotal <= m.heads]
            if not eligible_machines:
                log.warning("No eligible machine for event %s", event.orderId)
                continue

            presence_list = []
            start_var = model.NewIntVar(0, max_horizon, f"start_{i}")
            end_var = model.NewIntVar(0, max_horizon, f"end_{i}")
            event_start_vars[i] = start_var
            event_end_vars[i] = end_var

            for machine in eligible_machines:
                m_idx = machine_index[machine.machineId]
                _entries, _end_minutes, horizon = machine_calendars[m_idx]
                presence = model.NewBoolVar(f"present_{i}_{m_idx}")
                s = model.NewIntVar(0, horizon, f"s_{i}_{m_idx}")
                e = model.NewIntVar(0, horizon, f"e_{i}_{m_idx}")
                interval = model.NewOptionalIntervalVar(s, duration, e, presence, f"int_{i}_{m_idx}")
                interval_vars[(i, m_idx)] = interval
                start_vars[(i, m_idx)] = s
                end_vars[(i, m_idx)] = e
                presence_vars[(i, m_idx)] = presence
                presence_list.append(presence)

                model.Add(start_var == s).OnlyEnforceIf(presence)
                model.Add(end_var == e).OnlyEnforceIf(presence)

            model.AddExactlyOne(presence_list)

            assigned_machine = model.NewIntVar(0, len(self.machines) - 1, f"machine_{i}")
            assigned_machine_vars[i] = assigned_machine
            model.AddAllowedAssignments(
                [assigned_machine],
                [[machine_index[m.machineId]] for m in eligible_machines],
            )
            for machine in eligible_machines:
                m_idx = machine_index[machine.machineId]
                model.Add(assigned_machine == m_idx).OnlyEnforceIf(presence_vars[(i, m_idx)])

        for m_idx, _machine in enumerate(self.machines):
            machine_intervals = [
                interval_vars[key]
                for key in interval_vars
                if key[1] == m_idx
            ]
            if machine_intervals:
                model.AddNoOverlap(machine_intervals)

        # Track lateness (days late) for each event
        for i, event in enumerate(events):
            # event_end_vars[i] is in minutes, event.requestedShipDate is a datetime
            due_minutes = int((event.requestedShipDate - events[0].requestedShipDate).total_seconds() // 60)
            # lateness = max(0, end_time - due_minutes)
            lateness = model.NewIntVar(0, max_horizon, f"lateness_{i}")
            model.Add(lateness >= event_end_vars[i] - due_minutes)
            model.Add(lateness >= 0)
            lateness_vars[i] = lateness

        # Track on-time events (finish by due date)
        on_time_vars: List[cp_model.BoolVar] = []
        for i, event in enumerate(events):
            if i not in event_end_vars:
                continue
            per_machine_on_time: List[cp_model.BoolVar] = []
            for machine in self.machines:
                m_idx = machine_index[machine.machineId]
                if (i, m_idx) not in presence_vars:
                    continue
                entries, end_minutes, _horizon = machine_calendars[m_idx]
                due_minute = self._due_minute(event.requestedShipDate, entries, end_minutes)
                on_time_m = model.NewBoolVar(f"on_time_{i}_{m_idx}")
                model.Add(event_end_vars[i] <= due_minute).OnlyEnforceIf(on_time_m)
                model.Add(event_end_vars[i] > due_minute).OnlyEnforceIf(on_time_m.Not(), presence_vars[(i, m_idx)])
                model.Add(on_time_m <= presence_vars[(i, m_idx)])
                per_machine_on_time.append(on_time_m)

            on_time = model.NewBoolVar(f"on_time_{i}")
            if per_machine_on_time:
                model.Add(sum(per_machine_on_time) == on_time)
            else:
                model.Add(on_time == 0)
            on_time_vars.append(on_time)

        # Track very late events (more than a week late)
        very_late_vars: List[cp_model.BoolVar] = []
        for i, event in enumerate(events):
            if i not in event_end_vars:
                continue
            per_machine_very_late: List[cp_model.BoolVar] = []
            for machine in self.machines:
                m_idx = machine_index[machine.machineId]
                if (i, m_idx) not in presence_vars:
                    continue
                entries, end_minutes, _horizon = machine_calendars[m_idx]
                one_week_late_date = event.requestedShipDate + timedelta(days=7)
                one_week_late_minute = self._due_minute(one_week_late_date, entries, end_minutes)
                very_late_m = model.NewBoolVar(f"very_late_{i}_{m_idx}")
                model.Add(event_end_vars[i] > one_week_late_minute).OnlyEnforceIf(very_late_m)
                model.Add(event_end_vars[i] <= one_week_late_minute).OnlyEnforceIf(very_late_m.Not(), presence_vars[(i, m_idx)])
                model.Add(very_late_m <= presence_vars[(i, m_idx)])
                per_machine_very_late.append(very_late_m)

            very_late = model.NewBoolVar(f"very_late_{i}")
            if per_machine_very_late:
                model.Add(sum(per_machine_very_late) == very_late)
            else:
                model.Add(very_late == 0)
            very_late_vars.append(very_late)

        

        # Multi-objective: maximize on-time deliveries and heavily penalize very late events
        # model.Maximize(sum(on_time_vars)) # 1
        model.Maximize(sum(on_time_vars) - 100 * sum(very_late_vars)) # 2
        # model.minimize(sum(lateness_vars[i] * (1000 if very_late_vars[i] else 1) for i in range(len(events)) if i in lateness_vars))
        # model.minimize(sum(lateness_vars[i] for i in range(len(events)) if i in lateness_vars)) # 3

        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = time_limit_sec
        solver.parameters.num_search_workers = 16
        result = solver.Solve(model)

        log.info(f"CP-SAT Solver Status: {solver.StatusName(result)}")
        if result not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            raise RuntimeError("CP-SAT did not find a feasible schedule.")

        assigned_events: List[ProductionEvent] = []
        for i, event in enumerate(events):
            if i not in assigned_machine_vars:
                continue
            m_idx = solver.Value(assigned_machine_vars[i])
            event.assignedMachineId = self.machines[m_idx].machineId
            start_min = solver.Value(event_start_vars[i])
            entries, end_minutes, _horizon = machine_calendars[m_idx]
            event.scheduledStartDate = self._work_minutes_to_date(start_min, entries, end_minutes)
            assigned_events.append(event)

        return assigned_events

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
                topEvents = events[:window] # Top X events by value
                fittingEvents = [e for e in topEvents if e.headsTotal <= machine.heads] # Filter by if they fit
                fittingEvents.sort(key= lambda e: e.headsTotal, reverse= True) # Sort by heads desc
                if fittingEvents: # something fits
                    selectedEvent = fittingEvents[0]
                    selectedEvent.assignedMachineId = machine.machineId
                    assignedEvents.append(selectedEvent)
                    events.remove(selectedEvent)
                else: # nothing fits, just grab next 
                    for e in topEvents:
                        if e.headsTotal <= machine.heads:
                            e.assignedMachineId = machine.machineId
                            assignedEvents.append(e)
                            events.remove(e)
        
        return assignedEvents

    def scheduleEventsHistorical(self, events: List[ProductionEvent], reference_date: Optional[date] = None) -> List[ProductionEvent]:
        """
        Schedule events as if they were being scheduled on a historical reference date.
        This allows validation of the scheduler against completed work.
        
        Args:
            events: Historical ProductionEvent objects with past requestedShipDate values
            reference_date: The date to treat as "today" for scheduling purposes. 
                           If None, uses the earliest requestedShipDate from events.
        
        Returns:
            Scheduled events with scheduledStartDate and assignedMachineId set
        """
        if not events:
            return []
        
        # Determine the reference point for this historical schedule
        if reference_date is None:
            reference_date = min(event.requestedShipDate for event in events) - timedelta(days=7)
        
        # Temporarily override the calendar start date for all machines
        original_schedules = {}
        for machine in self.machines:
            original_schedules[machine.machineId] = machine.schedule
            current_schedule = self._get_machine_schedule(machine)
            machine.schedule = WeekSchedule(
                startDate=reference_date,
                hours=current_schedule.hours,
                monday=current_schedule.monday,
                tuesday=current_schedule.tuesday,
                wednesday=current_schedule.wednesday,
                thursday=current_schedule.thursday,
                friday=current_schedule.friday,
                saturday=current_schedule.saturday,
                sunday=current_schedule.sunday,
            )
        
        try:
            # Schedule using the historical context
            scheduled_events = self.scheduleEventsCpSat(events, time_limit_sec=30)
        except Exception as e:
            log.error(f"Error during historical scheduling: {str(e)}")
            scheduled_events = []
        finally:
            # Restore original schedules
            for machine in self.machines:
                machine.schedule = original_schedules[machine.machineId]
        print("success. returning events")
        return scheduled_events
    
if __name__ == "__main__":
    opStart = perf_counter()

    # writeUnscheduledOrdersToJson(getUnscheduledOrders(lookBackRange= 30, lookAheadRange= 90)) # fetch up to 3 months of orders and write to json for testing

    newSchedule = WeekSchedule(startDate= date.today(), hours= 6)

    # machines: List[Machine] = fetchMachines() # fetch up static machine data
    # unscheduledOrders: List[ProductionEvent] = DEBUG_loadUnscheduledOrdersFromJson() # fetch static uunscheduled orders from json

    # agent = SchedulingAgent(machines)
    # agent.assignAllMachineSchedules(newSchedule)
    # agent.evaluateAllEvents(unscheduledOrders)
    # scheduledOrders = agent.scheduleEventsCpSat(unscheduledOrders)
    
    # showValues(scheduledOrders, 'Outputs/sorted_evaluated.json')

    # real
    machines: List[Machine] = fetchMachines() # fetch up static machine data
    
    # unscheduledOrders: List[ProductionEvent] = DEBUG_loadUnscheduledOrdersFromJson("Outputs/unscheduled_orders.json", filterOld= True) # fetch static uunscheduled orders from json
    unscheduledOrders: List[ProductionEvent] = getUnscheduledOrders(lookBackRange= 0, lookAheadRange= 90) # fetch up to 3 months of orders for testing
    agent = SchedulingAgent(machines)
    agent.assignAllMachineSchedules(newSchedule, save= True)
    agent.evaluateAllEvents(unscheduledOrders)
    scheduledOrders = agent.scheduleEventsCpSat(unscheduledOrders, time_limit_sec= 120)
    showValues(scheduledOrders, 'Outputs/200-6-nolookback-goal2-6hr+30min-received.json')

    # # historical 
    # machines: List[Machine] = fetchMachines()
    # # writeUnscheduledOrdersToJson("Outputs/historical_preschedule.json", getHistoricalScheduledOrders(minDate= date(2025, 9, 1), maxDate= date(2026, 1, 10))) # fetch past 3 months of orders and write to json for testing
    # historical_orders: List[ProductionEvent] = DEBUG_loadUnscheduledOrdersFromJson("Outputs/historical_preschedule.json") # load from json for testing
    # # historical_orders: List[ProductionEvent] = getHistoricalScheduledOrders(minDate= date(2025, 9, 1), maxDate= date(2026, 1, 10))
    # agent = SchedulingAgent(machines)
    # agent.evaluateAllEvents(historical_orders)
    
    # # Re-schedule historical orders in their original context
    # rescheduled_orders = agent.scheduleEventsHistorical(historical_orders)
    # print(f"Rescheduled {len(rescheduled_orders)} historical orders")
    # # write to json
    # with open('Outputs/historical_rescheduled2.json', 'w', encoding='utf-8') as f:
    #     json.dump([event.to_dict() for event in rescheduled_orders], f, indent=4)
    # # Compare scheduled vs actual results
    # # showValues(rescheduled_orders, 'Outputs/historical_validation.json')

    print(f"Execution time: {perf_counter() - opStart:.2f} seconds")