from asyncio import events
import collections
from pyexpat import model
import re
import stat
from matplotlib.pylab import f
import matplotlib.pyplot as plt
from datetime import date
import numpy as np
from ortools.sat.python import cp_model
from pydantic import BaseModel, Field, PositiveInt, NonNegativeFloat, model_validator

from Types import DummyEvent

# Util func to convert date to int for cpsat model relative to a fixed start date
# needed for compatibility with cp models constraints
def _date_to_int(this_date: date | str, start_date: date = date(2025, 1, 1)) -> int:
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
                event.requestedShipDate = _date_to_int(event.requestedShipDate)
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
        # self._add_constraint_pad_between_events()

    def _add_constraint_force_before_ship_date(self):
        for event in self.instance.events:
            self.model.add(self._event_vars.event_end[event.orderId] <= event.requestedShipDate)

    def _add_constraint_machine_no_overlap(self):
        for machine in self.instance.machines:
            machine_id = machine["id"]
            intervals_on_machine = self._event_vars.machine_intervals[machine_id]
            if intervals_on_machine:
                self.model.add_no_overlap(intervals_on_machine)

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

    def _add_constraint_no_gap_between_events(self):
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
                        
                        # Create bool vars for ordering when both events are on this machine
                        i_before_j = self.model.new_bool_var(f"i_before_j_{event_id_i}_{event_id_j}_m{machine_id}")
                        j_before_i = self.model.new_bool_var(f"j_before_i_{event_id_i}_{event_id_j}_m{machine_id}")
                        
                        # Enforce no-gap constraint in either order
                        self.model.add(interval_i.EndExpr() == interval_j.StartExpr()).only_enforce_if(
                            [presence_i, presence_j, i_before_j]
                        )
                        self.model.add(interval_j.EndExpr() == interval_i.StartExpr()).only_enforce_if(
                            [presence_i, presence_j, j_before_i]
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
                        "assignedMachineId": self.solver.Value(self._event_vars.event_to_machine[event.orderId]),
                        "scheduledStartDate": self.solver.Value(self._event_vars.event_start[event.orderId]),
                        "scheduledEndDate": self.solver.Value(self._event_vars.event_end[event.orderId]),
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
    df = pd.read_csv('filtered_orders.csv')

    df["estTime"] = df.apply(lambda row: DummyEvent.estTimeFromQuantityAndColorsInMinutes(row["cn_QtyToProduce"], row["ColorsTotal"]), axis=1)
    df["complexity"] = df.apply(lambda row: DummyEvent.complexityFromColorCount(row["ColorsTotal"]), axis=1)
    df["designId"] = df["id_Design"].apply(lambda x: int(float(x)))
    
    events = [DummyEvent(row["id_Order"], row["designId"], row["estTime"], row["date_OrderRequestedToShip"], row["complexity"]) for _, row in df.iterrows()]

    # quit()
    events_grouped = collections.defaultdict(lambda: {"estTime": 0, "requestedShipDate": 0, "complexity": 0})
    for event in events:
        if event.designId not in events_grouped:
            events_grouped[event.designId] = {"estTime": event.estTime, "requestedShipDate": event.requestedShipDate, "complexity": event.complexity}
        else:
            events_grouped[event.designId]["estTime"] += event.estTime
            events_grouped[event.designId]["requestedShipDate"] = min(events_grouped[event.designId]["requestedShipDate"], event.requestedShipDate)
            events_grouped[event.designId]["complexity"] = max(events_grouped[event.designId]["complexity"], event.complexity)

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

    instance = SchedulerInstance(events=[DummyEvent(i, k, v["estTime"], v["requestedShipDate"], v["complexity"]) for i, (k, v) in enumerate(events_grouped.items())],
        machines=machines
    )
    config = SchedulerSolverConfig(time_limit_seconds=30, log_search_progress=False, optimization_tolerance=0.01, num_search_workers=16)
    
    solver = SchedulerSolver(instance, config)
    solver._set_balanced_objective()
    # solver._add_constraint_no_gap_between_events()
    for event in instance.events:
        print(f"Event {event.orderId}: estTime={event.estTime}, requestedShipDate={event.requestedShipDate}, complexity={event.complexity}")
        break
    solution = solver.solve(time_limit=30)
    print(f"Solution status: {solution.status}")
    # print(f"Solution status: {solution.status}, Objective value: {solution.objective_value}, Schedule: {solution.schedule}")

    # Graph view
    if solution.status in ["OPTIMAL", "FEASIBLE"]:
        import matplotlib.pyplot as plt
        import matplotlib.colors as mcolors
        
        # Create color map for complexity levels
        complexity_levels = sorted(set(e.complexity for e in instance.events))
        colors = plt.cm.viridis(np.linspace(0, 1, len(complexity_levels)))
        complexity_to_color = {comp: colors[i] for i, comp in enumerate(complexity_levels)}
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
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
                event = next(e for e in instance.events if e.orderId == job["orderId"])
                color = complexity_to_color[event.complexity]
                
                ax.barh(y_pos, job["scheduledEndDate"] - job["scheduledStartDate"], left=job["scheduledStartDate"], 
                       height=0.6, color=color, edgecolor='black', linewidth=0.5)
                ax.text(job["scheduledStartDate"] + (job["scheduledEndDate"] - job["scheduledStartDate"]) / 2, y_pos, 
                       f'{job["orderId"]}', ha='center', va='center', fontsize=8, color='white', weight='bold')
            
            y_pos += 1
        
        ax.set_yticks(yticks)
        ax.set_yticklabels(ylabels)
        ax.set_xlabel("Time")
        ax.set_ylabel("Machine")
        ax.set_title("Schedule by Machine (colored by complexity)")
        ax.grid(axis='x', alpha=0.3)
        
        # Add legend for complexity colors
        legend_elements = [plt.Rectangle((0,0),1,1, facecolor=complexity_to_color[comp], 
                                        edgecolor='black', label=f'Complexity {comp}')
                          for comp in complexity_levels]
        ax.legend(handles=legend_elements, loc='upper right')
        
        plt.tight_layout()
        plt.show()

gtest()