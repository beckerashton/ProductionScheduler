from asyncio import events
import collections
from pyexpat import model
import re
import stat
from matplotlib.pylab import f
import matplotlib.pyplot as plt
from datetime import date, timedelta
import numpy as np
from ortools.sat.python import cp_model
from pandas import DataFrame
from pydantic import BaseModel, Field, PositiveInt, NonNegativeFloat, model_validator

from OtherUtils import tempShow
from Types import DummyEvent
from DbUtils import peek, refresh 

# Util func to convert date to int for cpsat model relative to a fixed start date
# needed for compatibility with cp models constraints
def _date_to_int(this_date: date | str, start_date: date = date.today()) -> int:
    if isinstance(this_date, str):
        this_date = date.fromisoformat(this_date)
    return (this_date - start_date).days

# Container for the inputs to a scheduler instance
class SchedulerInstance(BaseModel):
    events: list[DummyEvent] = Field(..., description="List of events to be scheduled.")
    machines: list[dict] = Field(..., description="List of machines available for scheduling.")

    @model_validator(mode="before")
    def validate_input(cls, data):
            # Basic validation to ensure required fields are present
        if "events" not in data or not isinstance(data["events"], list):
            raise ValueError("Input must contain a list of events.")
        if "machines" not in data or not isinstance(data["machines"], list):
            raise ValueError("Input must contain a list of machines.")
        return data

    @model_validator(mode="after")
    def reformat_events_dates(self):
        for event in self.events:
            if isinstance(event.requestedShipDate, str) or isinstance(event.requestedShipDate, date):
                event.requestedShipDate = _date_to_int(event.requestedShipDate) * 8 * 60
        return self

# Container for the configs of a scheduler instance
class SchedulerSolverConfig(BaseModel):
    time_limit_seconds: PositiveInt = Field(60, description="Time limit for the solver in seconds.")
    log_search_progress: bool = Field(False, description="Whether to log search progress during solving.")
    optimization_tolerance: NonNegativeFloat = Field(0.01, description="Tolerance for optimization.")
    num_search_workers: PositiveInt = Field(1, description="Number of parallel workers for the solver.")

# Container for the outputs of a scheduler instance
class SchedulerSolution(BaseModel):
    schedule: list[dict] = Field(..., description="List of scheduled events with assigned machines and times.")
    objective_value: float = Field(..., description="Objective value of the solution.")
    status: str = Field(..., description="Status of the solver after attempting to solve the scheduling problem.")


def _event_duration(event: DummyEvent) -> int:
    duration = getattr(event, "duration", None)
    if duration is None:
        duration = getattr(event, "estTime", None)
    if not isinstance(duration, int) or duration <= 0:
        raise ValueError(f"Event {event.orderId} must have a positive integer duration/estTime.")
    return duration


class _EventSchedulingVars:
    def __init__(self, instance: SchedulerInstance, model: cp_model.CpModel):
        self.event_to_machine = {}
        self.event_start = {}
        self.event_end = {}
        self.event_presence = {}
        self.machine_intervals = collections.defaultdict(list)

        self.horizon = sum(_event_duration(event) for event in instance.events) + max(event.requestedShipDate for event in instance.events)

        for event in instance.events:
            duration = _event_duration(event)
            requested_ship_date = event.requestedShipDate
            if not isinstance(requested_ship_date, int):
                raise ValueError(f"Event {event.orderId} requestedShipDate must be an integer for this prototype.")

            eligible_machine_ids = [m["id"] for m in instance.machines if m["capability"] >= event.complexity]
            if not eligible_machine_ids:
                raise ValueError(f"Event {event.orderId} has no eligible machines.")

            machine_var = model.new_int_var_from_domain(
                cp_model.Domain.FromValues(eligible_machine_ids),
                f"event_{event.orderId}_machine"
            )
            start_var = model.new_int_var(0, self.horizon - duration, f"event_{event.orderId}_start")
            end_var = model.new_int_var(duration, self.horizon, f"event_{event.orderId}_end")

            presence_vars = []
            for machine_id in eligible_machine_ids:
                presence = model.new_bool_var(f"event_{event.orderId}_on_machine_{machine_id}")
                interval_var = model.new_optional_interval_var(
                    start_var,
                    duration,
                    end_var,
                    presence,
                    f"event_{event.orderId}_interval_machine_{machine_id}",
                )
                self.event_presence[event.orderId, machine_id] = presence
                self.machine_intervals[machine_id].append(interval_var)
                model.add(machine_var == machine_id).only_enforce_if(presence)
                presence_vars.append(presence)

            model.add_exactly_one(presence_vars)

            self.event_to_machine[event.orderId] = machine_var
            self.event_start[event.orderId] = start_var
            self.event_end[event.orderId] = end_var
        

class SchedulerSolver:
    def __init__(self, instance: SchedulerInstance, config: SchedulerSolverConfig):
        self.instance = instance
        self.config = config
        self.model = cp_model.CpModel()
        self._event_vars = _EventSchedulingVars(instance, self.model)
        self._build_model()
        self.solver = cp_model.CpSolver()

    def _add_default_constraints(self):
        # self._add_constraint_force_before_ship_date()
        self._add_constraint_machine_no_overlap()
        self._add_constraint_machine_contiguous_block()
        # self._add_constraint_pad_between_events()

    def _add_constraint_force_before_ship_date(self):
        for event in self.instance.events:
            self.model.add(self._event_vars.event_end[event.orderId] <= event.requestedShipDate)

    def _add_constraint_force_before_ship_date_ignore_lates(self):
        for event in self.instance.events:
            if event.requestedShipDate > 0:  # Only enforce for events that are not already late
                self.model.add(self._event_vars.event_end[event.orderId] <= event.requestedShipDate)

    def _add_constraint_force_before_ship_date_ignore_hinted(self, pre_solution: SchedulerSolution = None):
        hinted_event_ids = set()
        if pre_solution:
            hinted_event_ids = {e["orderId"] for e in pre_solution.schedule}
        
        for event in self.instance.events:
            if event.requestedShipDate > 0 and event.orderId not in hinted_event_ids:
                self.model.add(self._event_vars.event_end[event.orderId] <= event.requestedShipDate)

    def _add_constraint_machine_no_overlap(self):
        for machine in self.instance.machines:
            machine_id = machine["id"]
            intervals_on_machine = self._event_vars.machine_intervals[machine_id]
            if intervals_on_machine:
                self.model.add_no_overlap(intervals_on_machine)

    def _add_constraint_machine_contiguous_block(self):
        horizon = self._event_vars.horizon

        for machine in self.instance.machines:
            machine_id = machine["id"]
            machine_events = [
                event
                for event in self.instance.events
                if (event.orderId, machine_id) in self._event_vars.event_presence
            ]
            if not machine_events:
                continue

            presences = [
                self._event_vars.event_presence[event.orderId, machine_id]
                for event in machine_events
            ]

            machine_used = self.model.new_bool_var(f"machine_{machine_id}_used")
            self.model.add(sum(presences) >= 1).only_enforce_if(machine_used)
            self.model.add(sum(presences) == 0).only_enforce_if(machine_used.Not())

            adjusted_starts = []
            adjusted_ends = []

            for event in machine_events:
                presence = self._event_vars.event_presence[event.orderId, machine_id]
                start = self._event_vars.event_start[event.orderId]
                end = self._event_vars.event_end[event.orderId]

                adjusted_start = self.model.new_int_var(
                    0,
                    horizon,
                    f"event_{event.orderId}_adjusted_start_machine_{machine_id}",
                )
                adjusted_end = self.model.new_int_var(
                    0,
                    horizon,
                    f"event_{event.orderId}_adjusted_end_machine_{machine_id}",
                )

                self.model.add(adjusted_start == start).only_enforce_if(presence)
                self.model.add(adjusted_start == horizon).only_enforce_if(presence.Not())

                self.model.add(adjusted_end == end).only_enforce_if(presence)
                self.model.add(adjusted_end == 0).only_enforce_if(presence.Not())

                adjusted_starts.append(adjusted_start)
                adjusted_ends.append(adjusted_end)

            first_start = self.model.new_int_var(0, horizon, f"machine_{machine_id}_first_start")
            last_end = self.model.new_int_var(0, horizon, f"machine_{machine_id}_last_end")
            self.model.add_min_equality(first_start, adjusted_starts)
            self.model.add_max_equality(last_end, adjusted_ends)
            self.model.add(first_start == 0).only_enforce_if(machine_used)

            busy_span = self.model.new_int_var(0, horizon, f"machine_{machine_id}_busy_span")
            self.model.add(busy_span == last_end - first_start).only_enforce_if(machine_used)
            self.model.add(busy_span == 0).only_enforce_if(machine_used.Not())

            total_processing_time = sum(
                _event_duration(event) * self._event_vars.event_presence[event.orderId, machine_id]
                for event in machine_events
            )
            self.model.add(total_processing_time == busy_span)

    def _add_constraint_sequence_subevents(self):
        # force events with the same designId root to have its subevents scheduled in misc>Front Left Chest>Sleeve>Full Front>Full Back order
        root_to_subevents = collections.defaultdict(list)
        for event in self.instance.events:
            root_id = event.designId.split("_")[0]
            root_to_subevents[root_id].append(event)
        for root_id, subevents in root_to_subevents.items():
            if len(subevents) <= 1:
                continue
            # sort order Front Left Chest>Sleeve>Full Front>Full Back with any unspecified going first
            subevents.sort(key=lambda e: ["Front Left Chest", "Sleeve", "Full Front", "Full Back"].index(e.designId.split("_")[1]) if e.designId.split("_")[1] in ["Front Left Chest", "Sleeve", "Full Front", "Full Back"] else -1)
            for i in range(len(subevents) - 1):
                event_i = subevents[i]
                event_j = subevents[i + 1]
                self.model.add(self._event_vars.event_end[event_i.orderId] <= self._event_vars.event_start[event_j.orderId])

    # constraint that on the same machine there is a gap of 1 time unit between events
    def _add_constraint_pad_between_events(self):
        for machine in self.instance.machines:
            machine_id = machine["id"]
            intervals_on_machine = self._event_vars.machine_intervals[machine_id]
            if intervals_on_machine:
                for i in range(len(intervals_on_machine)):
                    for j in range(i + 1, len(intervals_on_machine)):
                        interval_i = intervals_on_machine[i]
                        interval_j = intervals_on_machine[j]
                        # Extract orderId from interval name: "event_{orderId}_interval_machine_{machineId}"
                        event_id_i = int(interval_i.Name().split("_")[1])
                        event_id_j = int(interval_j.Name().split("_")[1])
                        presence_i = self._event_vars.event_presence[event_id_i, machine_id]
                        presence_j = self._event_vars.event_presence[event_id_j, machine_id]
                        
                        # Add padding constraint only when both events are on this machine
                        self.model.add(interval_i.EndExpr() + 1 <= interval_j.StartExpr()).only_enforce_if(
                            [presence_i, presence_j]
                        )
                        self.model.add(interval_j.EndExpr() + 1 <= interval_i.StartExpr()).only_enforce_if(
                            [presence_i, presence_j]
                        )
                        
    def _add_soft_deadline_penalty(self):
        penalties = []
        for event in self.instance.events:
            tardiness = self.model.new_int_var(0, 10000, f"tardiness_{event.orderId}")
            self.model.add_max_equality(tardiness, [
                self._event_vars.event_end[event.orderId] - event.requestedShipDate,
                0
            ])
            penalties.append(tardiness)
        return penalties
    
    def _add_presolve_hint(self, pre_solution: SchedulerSolution):
        for scheduled_event in pre_solution.schedule:
            order_id = scheduled_event["orderId"]
            assigned_machine_id = scheduled_event["assignedMachineId"]
            scheduled_start = scheduled_event["scheduledStartDate"]
            self.model.add(self._event_vars.event_to_machine[order_id] == assigned_machine_id)
            self.model.add(self._event_vars.event_start[order_id] == scheduled_start)

    def _set_makespan_objective(self):
        makespan = self.model.new_int_var(0, self._event_vars.horizon, "makespan")
        self.model.add_max_equality(makespan, [self._event_vars.event_end[event.orderId] for event in self.instance.events])
        self.model.minimize(makespan)

    def _set_makespan_with_tardiness_penalty_objective(self):
        makespan = self.model.new_int_var(0, self._event_vars.horizon, "makespan")
        self.model.add_max_equality(makespan, [self._event_vars.event_end[event.orderId] for event in self.instance.events])
        penalties = None
        penalties = self._add_soft_deadline_penalty()
        self.model.minimize(makespan + 1000 * sum(penalties))

    def _set_balanced_objective(self):
        # Minimize makespan AND variance in machine load
        makespan = self.model.new_int_var(0, self._event_vars.horizon, "makespan")
        self.model.add_max_equality(makespan, [self._event_vars.event_end[e.orderId] for e in self.instance.events])
        
        machine_loads = []
        for machine in self.instance.machines:
            if machine["id"] == 6:  # ignores stryker when balancing loads
                continue
            load = sum(
                self._event_vars.event_presence[event.orderId, machine["id"]] * event.estTime
                for event in self.instance.events
                if machine["capability"] >= event.complexity
            )
            machine_loads.append(load)
        
        # Penalize maximum load difference
        max_load = self.model.new_int_var(0, self._event_vars.horizon, "max_load")
        min_load = self.model.new_int_var(0, self._event_vars.horizon, "min_load")
        self.model.add_max_equality(max_load, machine_loads)
        self.model.add_min_equality(min_load, machine_loads)
        
        self.model.minimize(makespan * 10 + (max_load - min_load))  # Weighted multi-objective

    def _build_model(self):
        self._add_default_constraints()

    def solve(self, time_limit: float | None = None) -> SchedulerSolution:
        self.solver.parameters.max_time_in_seconds = time_limit if time_limit is not None else self.config.time_limit_seconds
        self.solver.parameters.log_search_progress = self.config.log_search_progress
        self.solver.parameters.relative_gap_limit = self.config.optimization_tolerance
        self.solver.parameters.num_search_workers = self.config.num_search_workers
        status = self.solver.Solve(self.model)
        
        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            return SchedulerSolution(
                schedule=[
                    {
                        "orderId": event.orderId,
                        "designId": event.designId,
                        "assignedMachineId": self.solver.Value(self._event_vars.event_to_machine[event.orderId]),
                        "scheduledStartDate": self.solver.Value(self._event_vars.event_start[event.orderId]),
                        "scheduledEndDate": self.solver.Value(self._event_vars.event_end[event.orderId]),
                        "requestedShipDate": event.requestedShipDate,
                        "complexity": event.complexity
                    }
                    for event in self.instance.events
                ],
                objective_value=self.solver.ObjectiveValue(),
                status=self.solver.StatusName(status)
            )
        
        return SchedulerSolution(
            schedule=[],
            objective_value=0.0,
            status=self.solver.StatusName(status)
        )

def gtest():
    # events = [
    #     DummyEvent(1, 0, 5, 10, 0),
    #     DummyEvent(2, 0, 3, 8, 0),
    #     DummyEvent(3, 1, 4, 12, 1),
    #     DummyEvent(4, 1, 2, 9, 1),
    #     DummyEvent(5, 2, 6, 15, 2),
    #     DummyEvent(6, 3, 1, 7, 2),
    #     # DummyEvent(7, 4, 10, 15, 3),
    #     DummyEvent(8, 5, 2, 17, 0),
    #     DummyEvent(9, 4, 5, 20, 3),
    #     DummyEvent(10, 5, 5, 18, 0)
    # ]

    # from random import randint, seed
    # seed(12)
    # events = []
    # # dc = [0] * 18 + [1] * 24 + [2] * 34 + [3] * 14 # fake
    # normalizer = float(100 / (2466 + 430 + 1258 + 297))
    # dc = [0] * int(2466*normalizer) + [1] * int(430*normalizer) + [2] * int(1258*normalizer) + [3] * int(297*normalizer)
    # print(dc.count(0), dc.count(1), dc.count(2), dc.count(3))
    # for _ in range(1000):
    #     orderId = len(events) + 1
    #     designId = randint(0, 199)
    #     estTime = randint(1, 20)
    #     requestedShipDate = randint(1, 100)
    #     complexity = dc[designId % len(dc)]
    #     events.append(DummyEvent(orderId, designId, estTime, requestedShipDate, complexity))

    import pandas as pd
    # df = pd.read_csv('Inputs/unscheduled_03_06_26.csv')
    # df = pd.read_csv('Inputs/copy.csv')
    # input_df = pd.read_excel("C:/Users/Aston/Documents/Production Scheduler Demo.xlsx", sheet_name="Sheet3", header=2, engine="openpyxl", usecols="C,E,M,P,V")
    input_df = pd.read_excel("C:/Users/Aston/Documents/Production Scheduler Demo.xlsx", nrows=500, sheet_name="Sheet3", header=1, usecols="B,C,E,M,P,U,V")
    input_df = input_df[input_df["Week_Sch"] == 11]
    
    df = input_df.rename(columns={"Order No": "id_Order", "Design No": "id_Design", "Location": "Location", "DueDate": "date_OrderRequestedToShip", "Imp": "cn_QtyToProduce", "No_Colors": "ColorsTotal"})
    df = df.dropna(subset=["id_Order", "id_Design", "Location", "date_OrderRequestedToShip", "cn_QtyToProduce", "ColorsTotal"])
    
    df["estTime"] = df.apply(lambda row: DummyEvent.estTimeFromQuantityAndColorsInMinutes(row["cn_QtyToProduce"], row["ColorsTotal"]), axis=1)
    df["complexity"] = df.apply(lambda row: DummyEvent.complexityFromColorCount(row["ColorsTotal"]), axis=1)
    df["designId"] = df.apply(lambda row: f"{int(row['id_Design'])}_{row['Location']}", axis=1)
    df["date_OrderRequestedToShip"] = df["date_OrderRequestedToShip"].apply(lambda x: x.date() if isinstance(x, pd.Timestamp) else date.fromisoformat(x) if isinstance(x, str) else x)
    df["setupTime"] = df.apply(lambda row: row["ColorsTotal"] * 10, axis=1)

    events = [DummyEvent(row["id_Order"], row["designId"], row["estTime"], row["setupTime"], row["date_OrderRequestedToShip"], row["complexity"]) for _, row in df.iterrows()]
    
    events_grouped = collections.defaultdict(lambda: {"estTime": 0, "requestedShipDate": 0, "complexity": 0})
    for event in events:
        if event.designId not in events_grouped:
            events_grouped[event.designId] = {"estTime": event.estTime, "requestedShipDate": event.requestedShipDate, "complexity": event.complexity}
        else:
            events_grouped[event.designId]["estTime"] += event.estTime - event.setupTime
            events_grouped[event.designId]["requestedShipDate"] = min(events_grouped[event.designId]["requestedShipDate"], event.requestedShipDate)
            events_grouped[event.designId]["complexity"] = max(events_grouped[event.designId]["complexity"], event.complexity)
    
    
    max_presolve_window_days = 7
    for i in range(7, max_presolve_window_days + 1, 1):
        # late_events_grouped = {k: v for k, v in events_grouped.items() if date.fromisoformat(v["requestedShipDate"]) <= date.today() + pd.Timedelta(days=i)}
        print(f"Complexity distribution: {collections.Counter(e.complexity for e in events)}")

        machines = [
            {"id": 1, "capability": 2},
            {"id": 2, "capability": 1},
            {"id": 3, "capability": 2},
            {"id": 4, "capability": 2},
            {"id": 5, "capability": 0},
            {"id": 6, "capability": 3},
            {"id": 7, "capability": 0},
        ]

        # pre solve with tighter constraints on late events to use for a min solve hint for a full solve
        # pre_machines = [v for v in machines if v["capability"] <= max(event["complexity"] for event in late_events_grouped.values())]

        # pre_instance = SchedulerInstance(events=[DummyEvent(i, k, v["estTime"], v["requestedShipDate"], v["complexity"]) for i, (k, v) in enumerate(late_events_grouped.items())],
        #     machines=pre_machines
        # )

        config = SchedulerSolverConfig(time_limit_seconds=30, log_search_progress=False, optimization_tolerance=0.01, num_search_workers=16)
        # pre_solver = SchedulerSolver(pre_instance, config)
        # pre_solver._set_balanced_objective()
        # pre_solver._add_constraint_sequence_subevents()
        # pre_solution = pre_solver.solve(time_limit=30)
        # print(f"Pre-solve on late events - Solution status: {pre_solution.status}, Objective value: {pre_solution.objective_value}")

        # save solution for debugging
        # pre_solution_df = pd.DataFrame(pre_solution.schedule)
        # pre_solution_df.to_csv("Inputs/pre_solution.csv", index=False)

        instance = SchedulerInstance(events=[DummyEvent(i, k, v["estTime"], v["requestedShipDate"], v["complexity"]) for i, (k, v) in enumerate(events_grouped.items())],
            machines=machines
        )
        

        # turn instance.events into a dataframe and save to csv for debugging
        # events_df = pd.DataFrame([{
        #     "orderId": event.orderId,
        #     "designId": event.designId,
        #     "estTime": event.estTime,
        #     "requestedShipDate": event.requestedShipDate,
        #     "complexity": event.complexity
        # } for event in instance.events])
        # events_df.to_csv("Inputs/instance_events.csv", index=False)
        

        solver = SchedulerSolver(instance, config)
        # solver._add_presolve_hint(pre_solution)
        solver._set_balanced_objective()
        # solver._add_constraint_force_before_ship_date_ignore_hinted(pre_solution)
        solver._add_constraint_sequence_subevents()
        # solver._add_constraint_machine_contiguous_block()

        solution = solver.solve(time_limit=30)
        print(f"Solution status: {solution.status}, Objective value: {solution.objective_value}")

        if solution.status == "INFEASIBLE":
            print(f"Failed with a presolve range of {i} days. Trying again with larger presolve range.")
        else:
            print(f"Succeeded with a presolve range of {i} days.")
            break

    # save the final solution to csv for debugging organized by start date then machine
    # solution_df = pd.DataFrame(solution.schedule)
    # solution_df = solution_df.sort_values(by=["scheduledStartDate", "assignedMachineId"])
    # solution_df.to_csv("Inputs/final_solution.csv", index=False)

    workday_minutes = 8 * 60

    def _model_minutes_to_excel_date(model_minutes: int) -> date:
        clamped_minutes = max(0, int(model_minutes))
        day_offset = clamped_minutes // workday_minutes
        actual_date = date.today() + timedelta(days=day_offset)
        return actual_date

    # excel_solution_df = solution_df[["designId", "assignedMachineId", "scheduledStartDate"]].copy()
    # excel_solution_df = excel_solution_df.rename(columns={"assignedMachineId": "assignedMachine"})
    # excel_solution_df["scheduledStartDate"] = excel_solution_df["scheduledStartDate"].apply(_model_minutes_to_excel_date)
    
    # write to excel with formatting
    # import xlsxwriter
    # with pd.ExcelWriter("Inputs/final_solution.xlsx", engine="xlsxwriter") as writer:
    #     excel_solution_df.to_excel(writer, index=False, sheet_name="Schedule")
    #     workbook = writer.book
    #     worksheet = writer.sheets["Schedule"]

    #     date_format = workbook.add_format({"num_format": "yyyy-mm-dd"})
    #     for idx, col in enumerate(excel_solution_df.columns):
    #         if col == "scheduledStartDate":
    #             worksheet.set_column(idx, idx, 20, date_format)
    #         else:
    #             worksheet.set_column(idx, idx, 18)
        

    # Graph view
    if solution.status in ["OPTIMAL", "FEASIBLE"]:
        import matplotlib.pyplot as plt
        from matplotlib.ticker import FuncFormatter, MultipleLocator

        def _model_minutes_to_datetime_text(model_minutes: float, include_newline: bool = False) -> str:
            clamped_minutes = max(0, int(round(model_minutes)))
            day_offset, minute_of_day = divmod(clamped_minutes, workday_minutes)
            actual_date = date.today() + timedelta(days=day_offset)
            hour = 8 + (minute_of_day // 60)
            minute = minute_of_day % 60
            separator = "\n" if include_newline else " "
            return f"{actual_date.isoformat()}{separator}{hour:02d}:{minute:02d}"

        def _x_tick_formatter(value: float, _pos: int) -> str:
            clamped_minutes = max(0, int(round(value)))
            day_offset = clamped_minutes // workday_minutes
            return (date.today() + timedelta(days=day_offset)).isoformat()
        
        # Create color map for complexity levels
        complexity_levels = sorted(set(e.complexity for e in instance.events))
        colors = plt.cm.viridis(np.linspace(0, 1, len(complexity_levels)))
        complexity_to_color = {comp: colors[i] for i, comp in enumerate(complexity_levels)}
        
        fig, ax = plt.subplots(figsize=(14, 8))
        event_by_order_id = {event.orderId: event for event in instance.events}
        bars_with_details = []
        
        # Group by machine
        machine_schedules = collections.defaultdict(list)
        for job in solution.schedule:
            machine_schedules[job["assignedMachineId"]].append(job)
        
        yticks = []
        ylabels = []
        y_pos = 0
        
        for machine_id in sorted(machine_schedules.keys()):
            jobs = sorted(machine_schedules[machine_id], key=lambda x: x["scheduledStartDate"])
            yticks.append(y_pos)
            ylabels.append(f"Machine {machine_id}")
            
            for job in jobs:
                # Get event complexity for color
                event = event_by_order_id[job["orderId"]]
                color = complexity_to_color[event.complexity]
                
                bar_container = ax.barh(
                    y_pos,
                    job["scheduledEndDate"] - job["scheduledStartDate"],
                    left=job["scheduledStartDate"],
                    height=0.6,
                    color=color,
                    edgecolor='black',
                    linewidth=0.5,
                )
                bar = bar_container.patches[0]
                details_text = (
                    f"Order ID: {job['orderId']}\n"
                    f"Design ID: {event.designId}\n"
                    f"Machine ID: {job['assignedMachineId']}\n"
                    f"Start: {_model_minutes_to_datetime_text(job['scheduledStartDate'])}\n"
                    f"End: {_model_minutes_to_datetime_text(job['scheduledEndDate'])}\n"
                    f"Duration: {job['scheduledEndDate'] - job['scheduledStartDate']}\n"
                    f"Est Time: {event.estTime}\n"
                    f"Requested Ship: {_model_minutes_to_datetime_text(event.requestedShipDate)}\n"
                    f"Complexity: {event.complexity}"
                )
                bars_with_details.append((bar, details_text))
                ax.text(job["scheduledStartDate"] + (job["scheduledEndDate"] - job["scheduledStartDate"]) / 2, y_pos, 
                       f'{job["orderId"]}', ha='center', va='center', fontsize=8, color='white', weight='bold')
            
            y_pos += 1
        
        ax.set_yticks(yticks)
        ax.set_yticklabels(ylabels)
        ax.set_xlabel("Date")
        ax.xaxis.set_major_locator(MultipleLocator(workday_minutes))
        ax.xaxis.set_major_formatter(FuncFormatter(_x_tick_formatter))
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
        ax.set_ylabel("Machine")
        ax.set_title("Schedule by Machine (colored by complexity)")
        ax.grid(axis='x', alpha=0.3)
        
        # Add legend for complexity colors
        legend_elements = [plt.Rectangle((0,0),1,1, facecolor=complexity_to_color[comp], 
                                        edgecolor='black', label=f'Complexity {comp}')
                          for comp in complexity_levels]
        ax.legend(handles=legend_elements, loc='upper right')

        tooltip = ax.annotate(
            "",
            xy=(0, 0),
            xytext=(10, 10),
            textcoords="offset points",
            bbox={"boxstyle": "round,pad=0.4", "fc": "white", "ec": "black", "alpha": 0.9},
            arrowprops={"arrowstyle": "->", "color": "black"},
        )
        tooltip.set_visible(False)

        def _update_tooltip(selected_bar, details_text):
            tooltip.xy = (
                selected_bar.get_x() + selected_bar.get_width() / 2,
                selected_bar.get_y() + selected_bar.get_height() / 2,
            )
            tooltip.set_text(details_text)

        def _on_hover(mouse_event):
            if mouse_event.inaxes != ax:
                if tooltip.get_visible():
                    tooltip.set_visible(False)
                    fig.canvas.draw_idle()
                return

            for bar, details_text in bars_with_details:
                contains, _ = bar.contains(mouse_event)
                if contains:
                    _update_tooltip(bar, details_text)
                    if not tooltip.get_visible():
                        tooltip.set_visible(True)
                    fig.canvas.draw_idle()
                    return

            if tooltip.get_visible():
                tooltip.set_visible(False)
                fig.canvas.draw_idle()

        fig.canvas.mpl_connect("motion_notify_event", _on_hover)

        # Requested ship date vs actual scheduled end date view
        requested_ship_dates = []
        scheduled_end_dates = []
        comparison_colors = []
        comparison_point_details = []

        for job in solution.schedule:
            event = event_by_order_id[job["orderId"]]
            requested_time = event.requestedShipDate
            end_time = job["scheduledEndDate"]
            delta_minutes = end_time - requested_time
            status_text = "Late" if delta_minutes > 0 else "On-time / Early"
            delta_text = f"{delta_minutes} min" if delta_minutes != 0 else "0 min"

            requested_ship_dates.append(requested_time)
            scheduled_end_dates.append(end_time)
            comparison_colors.append("tab:red" if end_time > requested_time else "tab:green")
            comparison_point_details.append(
                f"Order ID: {job['orderId']}\n"
                f"Design ID: {event.designId}\n"
                f"Machine ID: {job['assignedMachineId']}\n"
                f"Requested Ship: {_model_minutes_to_datetime_text(requested_time)}\n"
                f"Scheduled End: {_model_minutes_to_datetime_text(end_time)}\n"
                f"Status: {status_text}\n"
                f"Delta: {delta_text}"
            )

        fig2, ax2 = plt.subplots(figsize=(10, 8))
        comparison_scatter = ax2.scatter(
            requested_ship_dates,
            scheduled_end_dates,
            c=comparison_colors,
            alpha=0.8,
            edgecolors="black",
            linewidths=0.4,
        )

        min_time = min(min(requested_ship_dates), min(scheduled_end_dates))
        max_time = max(max(requested_ship_dates), max(scheduled_end_dates))
        ax2.plot(
            [min_time, max_time],
            [min_time, max_time],
            linestyle="--",
            color="black",
            linewidth=1,
            label="Scheduled End = Requested Ship",
        )

        late_orders = sum(1 for i in range(len(requested_ship_dates)) if scheduled_end_dates[i] > requested_ship_dates[i])
        on_time_orders = len(requested_ship_dates) - late_orders

        ax2.set_xlabel("Requested Ship Date")
        ax2.set_ylabel("Scheduled End Date")
        ax2.set_title(f"Requested vs Scheduled End ({on_time_orders} on-time, {late_orders} late)")
        ax2.xaxis.set_major_locator(MultipleLocator(workday_minutes))
        ax2.yaxis.set_major_locator(MultipleLocator(workday_minutes))
        ax2.xaxis.set_major_formatter(FuncFormatter(_x_tick_formatter))
        ax2.yaxis.set_major_formatter(FuncFormatter(_x_tick_formatter))
        plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')
        ax2.grid(alpha=0.3)

        comparison_legend = [
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='tab:green', markeredgecolor='black', markersize=7, label='On-time or early'),
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='tab:red', markeredgecolor='black', markersize=7, label='Late'),
        ]
        ax2.legend(handles=comparison_legend + [ax2.lines[0]], loc='upper left')

        comparison_tooltip = ax2.annotate(
            "",
            xy=(0, 0),
            xytext=(10, 10),
            textcoords="offset points",
            bbox={"boxstyle": "round,pad=0.4", "fc": "white", "ec": "black", "alpha": 0.9},
            arrowprops={"arrowstyle": "->", "color": "black"},
        )
        comparison_tooltip.set_visible(False)

        def _update_comparison_tooltip(point_index: int):
            comparison_tooltip.xy = (requested_ship_dates[point_index], scheduled_end_dates[point_index])
            comparison_tooltip.set_text(comparison_point_details[point_index])

        def _on_comparison_hover(mouse_event):
            if mouse_event.inaxes != ax2:
                if comparison_tooltip.get_visible():
                    comparison_tooltip.set_visible(False)
                    fig2.canvas.draw_idle()
                return

            contains, ind = comparison_scatter.contains(mouse_event)
            if contains and ind.get("ind"):
                point_index = ind["ind"][0]
                _update_comparison_tooltip(point_index)
                if not comparison_tooltip.get_visible():
                    comparison_tooltip.set_visible(True)
                fig2.canvas.draw_idle()
                return

            if comparison_tooltip.get_visible():
                comparison_tooltip.set_visible(False)
                fig2.canvas.draw_idle()

        fig2.canvas.mpl_connect("motion_notify_event", _on_comparison_hover)
        
        fig.tight_layout()
        fig2.tight_layout()
        plt.show()

gtest()

# refresh()