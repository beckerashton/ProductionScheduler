from asyncio import events
import collections
from pyexpat import model
import re
import stat
from matplotlib.pylab import f
import matplotlib.pyplot as plt
import numpy as np
from ortools.sat.python import cp_model
from pydantic import BaseModel, Field, PositiveInt, NonNegativeFloat, model_validator

from Types import DummyEvent

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

    from random import randint, seed, normalvariate
    seed(12)
    events = []
    dc = [0] * 15 + [1] * 21 + [2] * 31 + [3] * 11 + [4] * 12
    for _ in range(1000):
        orderId = len(events) + 1
        designId = randint(0, 199)
        estTime = randint(1, 20)
        requestedShipDate = randint(1, 100)
        complexity = dc[designId % len(dc)]
        events.append(DummyEvent(orderId, designId, estTime, requestedShipDate, complexity))

    events_grouped = collections.defaultdict(lambda: {"estTime": 0, "requestedShipDate": 0, "complexity": 0})
    for event in events:
        if event.designId not in events_grouped:
            events_grouped[event.designId] = {"estTime": event.estTime, "requestedShipDate": event.requestedShipDate, "complexity": event.complexity}
        else:
            events_grouped[event.designId]["estTime"] += event.estTime
            events_grouped[event.designId]["requestedShipDate"] = max(events_grouped[event.designId]["requestedShipDate"], event.requestedShipDate)
    
    print(f"Complexity distribution: {collections.Counter(e.complexity for e in events)}")

    machines = [
        {"id": 1, "capability": 0},
        {"id": 2, "capability": 1},
        {"id": 3, "capability": 1},
        {"id": 4, "capability": 2},
        {"id": 5, "capability": 2},
        {"id": 6, "capability": 3},
        {"id": 7, "capability": 4},
    ]

    instance = SchedulerInstance(events=[DummyEvent(i, k, v["estTime"], v["requestedShipDate"], v["complexity"]) for i, (k, v) in enumerate(events_grouped.items())],
        machines=machines
    )
    config = SchedulerSolverConfig(time_limit_seconds=30, log_search_progress=False, optimization_tolerance=0.01, num_search_workers=16)
    
    solver = SchedulerSolver(instance, config)
    solver._set_balanced_objective()
    solver._add_constraint_no_gap_between_events()

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
# ==============================================
# Everything beneath here is just reference code
# ==============================================

def knapsack():
    weights = [395, 658, 113, 185, 336, 494, 294, 295, 256, 530, 311, 321, 602, 855, 209, 647, 520, 387, 743, 26, 54, 420, 667, 971, 171, 354, 962, 454, 589, 131, 342, 449, 648, 14, 201, 150, 602, 831, 941, 747, 444, 982, 732, 350, 683, 279, 667, 400, 441, 786, 309, 887, 189, 119, 209, 532, 461, 420, 14, 788, 691, 510, 961, 528, 538, 476, 49, 404, 761, 435, 729, 245, 204, 401, 347, 674, 75, 40, 882, 520, 692, 104, 512, 97, 713, 779, 224, 357, 193, 431, 442, 816, 920, 28, 143, 388, 23, 374, 905, 942]
    values = [71, 15, 100, 37, 77, 28, 71, 30, 40, 22, 28, 39, 43, 61, 57, 100, 28, 47, 32, 66, 79, 70, 86, 86, 22, 57, 29, 38, 83, 73, 91, 54, 61, 63, 45, 30, 51, 5, 83, 18, 72, 89, 27, 66, 43, 64, 22, 23, 22, 72, 10, 29, 59, 45, 65, 38, 22, 68, 23, 13, 45, 34, 63, 34, 38, 30, 82, 33, 64, 100, 26, 50, 66, 40, 85, 71, 54, 25, 100, 74, 96, 62, 58, 21, 35, 36, 91, 7, 19, 32, 77, 70, 23, 43, 78, 98, 30, 12, 76, 38]
    capacity = 2000

    model = cp_model.CpModel()

    toggles = [model.new_bool_var(f"toggle_{i}") for i in range(len(weights))]

    accumulated_weight = sum(x * w for x, w in zip(toggles, weights))
    model.add(accumulated_weight <= capacity)
    accumulated_value = sum(x * v for x, v in zip(toggles, values))
    model.maximize(accumulated_value)

    solver = cp_model.CpSolver()
    solver.parameters.log_search_progress = True
    solver.solve(model)

    print(f"Values claimed: {[values[i] for i, v in enumerate(toggles) if solver.value(v)]}")
    print(f"Totaling: {solver.ObjectiveValue()}")

def tier1():
    # fake data
    class task:
        __slots__ = ['mid', 'duration']
        def __init__(self, mid, duration):
            self.mid = mid
            self.duration = duration
    class job:
        __slots__ = ['tasks']
        def __init__(self, tasks):
            self.tasks = tasks

    jobsData = [
        job([task(0, 3), task(1, 2), task(2, 2)]),
        job([task(0, 2), task(2, 1), task(1, 4)]),
        job([task(1, 4), task(2, 3)])
    ]

    mCnt = 1 + max(task.mid for job in jobsData for task in job.tasks)
    allMachines = range(mCnt)
    horizon = sum(task.duration for job in jobsData for task in job.tasks)

    # (0) instance model
    model = cp_model.CpModel()

    # (1) create variables
    taskType = collections.namedtuple("taskType", "start end interval")
    assignedTaskType = collections.namedtuple("assignedTaskType", "start job index duration")

    allTasks = {}
    machineToIntervals = collections.defaultdict(list)

    for jid, job in enumerate(jobsData):
        for tid, task in enumerate(job.tasks):
            suffix = f"j{jid}t{tid}"
            startVar = model.new_int_var(0, horizon, "start_" + suffix)
            endVar = model.new_int_var(0, horizon, "end_" + suffix)
            intervalVar = model.new_interval_var(startVar, task.duration, endVar, "interval_" + suffix)
            allTasks[jid, tid] = taskType(start=startVar, end=endVar, interval=intervalVar)
            machineToIntervals[task.mid].append(intervalVar)

    # (2) add constraints
    # each machine can only process one task at a time
    for machine in allMachines:
        model.add_no_overlap(machineToIntervals[machine])
    
    # a task must start after its predecessor finishes
    for jid, job in enumerate(jobsData):
        for tid in range(len(job.tasks) - 1):
            model.add(allTasks[jid, tid + 1].start >= allTasks[jid, tid].end)

    # (3) define objective
    # minimize makespan objective
    objVar = model.new_int_var(0, horizon, "makespan")
    model.add_max_equality(objVar, [allTasks[jid, len(job.tasks) - 1].end for jid, job in enumerate(jobsData)])
    model.minimize(objVar)

    # (4) solve
    solver = cp_model.CpSolver()
    status = solver.Solve(model)

    # (5) show results
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        print(f"Solution with makespan: {solver.ObjectiveValue()}. Rated as {solver.StatusName(status)}.")
        assignedTasks = collections.defaultdict(list)
        for jid, job in enumerate(jobsData):
            for tid, task in enumerate(job.tasks):
                assignedTasks[task.mid].append(
                    assignedTaskType(start=solver.Value(allTasks[jid, tid].start), job=jid, index=tid, duration=task.duration)
                )
        for machine in allMachines:
            assignedTasks[machine].sort()
            print(f"Machine {machine}:")
            for assignedTask in assignedTasks[machine]:
                print(f"  Job {assignedTask.job} Task {assignedTask.index} (start: {assignedTask.start}, duration: {assignedTask.duration})")
    else:
        print("No solution found.")
    print("Time: %.4f seconds" % solver.WallTime())


def tier2():
    # fake data
    class task:
        __slots__ = ['mid', 'duration', 'type']
        def __init__(self, mid, duration, type):
            self.mid = mid
            self.duration = duration
            self.type = type

    class job:
        __slots__ = ['tasks']
        def __init__(self, tasks):
            self.tasks = tasks

    jobsData = [
        job([task(0, 3, 0), task(1, 2, 1), task(2, 2, 1)]),
        job([task(0, 2, 0), task(2, 1, 1), task(1, 4, 1)]),
        job([task(1, 4, 0), task(2, 3, 1)])
    ]

    mCnt = 1 + max(task.mid for job in jobsData for task in job.tasks)
    allMachines = range(mCnt)
    horizon = sum(task.duration for job in jobsData for task in job.tasks)

    # (0) instance model
    model = cp_model.CpModel()

    # (1) create variables
    taskType = collections.namedtuple("taskType", "start end interval type machine")
    assignedTaskType = collections.namedtuple("assignedTaskType", "start job index duration")

    allTasks = {}
    machineToIntervals = collections.defaultdict(list)

    for jid, job in enumerate(jobsData):
        for tid, task in enumerate(job.tasks):
            suffix = f"j{jid}t{tid}"
            startVar = model.new_int_var(0, horizon, "start_" + suffix)
            endVar = model.new_int_var(0, horizon, "end_" + suffix)
            intervalVar = model.new_interval_var(startVar, task.duration, endVar, "interval_" + suffix)
            allTasks[jid, tid] = taskType(start=startVar, end=endVar, interval=intervalVar, type=task.type, machine=task.mid)
            machineToIntervals[task.mid].append(intervalVar)

    # (2) add constraints
    # each machine can only process one task at a time
    for machine in allMachines:
        model.add_no_overlap(machineToIntervals[machine])
    
    # conditionals
    # unimplemented

    for jid, job in enumerate(jobsData):
        for tid in range(len(job.tasks) - 1):
            model.add(allTasks[jid, tid + 1].start >= allTasks[jid, tid].end).only_enforce_if([])
                

    # (3) define objective
    # minimize makespan objective
    objVar = model.new_int_var(0, horizon, "makespan")
    model.add_max_equality(objVar, [allTasks[jid, len(job.tasks) - 1].end for jid, job in enumerate(jobsData)])
    model.minimize(objVar)

    # (4) solve
    solver = cp_model.CpSolver()
    status = solver.Solve(model)

    # (5) show results
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        print(f"Solution with makespan: {solver.ObjectiveValue()}. Rated as {solver.StatusName(status)}.")
        assignedTasks = collections.defaultdict(list)
        for jid, job in enumerate(jobsData):
            for tid, task in enumerate(job.tasks):
                assignedTasks[task.mid].append(
                    assignedTaskType(start=solver.Value(allTasks[jid, tid].start), job=jid, index=tid, duration=task.duration)
                )
        for machine in allMachines:
            assignedTasks[machine].sort()
            print(f"Machine {machine}:")
            for assignedTask in assignedTasks[machine]:
                print(f"  Job {assignedTask.job} Task {assignedTask.index} Type {jobsData[assignedTask.job].tasks[assignedTask.index].type} (start: {assignedTask.start}, duration: {assignedTask.duration})")
    else:
        print("No solution found.")
    print("Time: %.4f seconds" % solver.WallTime())

def tier3():
    # psuedo event data that short cuts a lot of stuff and only includes inputs to scheduling
    class event:
        __slots__ = ['orderId', 'designId', 'estTime', 'requestedShipDate', 'complexity']
        def __init__(self, orderId, designId, estTime, requestedShipDate, complexity):
            self.orderId = orderId
            self.designId = designId
            self.estTime = estTime
            self.requestedShipDate = requestedShipDate
            self.complexity = complexity
    
    eventsData = [
        event(1, 0, 5, 10, 0),
        event(2, 0, 3, 8, 0),
        event(3, 1, 4, 12, 1),
        event(4, 1, 2, 9, 1),
        event(5, 2, 6, 15, 2),
        event(6, 3, 1, 7, 2),
        event(7, 4, 10, 15, 3),
        event(8, 5, 2, 17, 0),
        event(9, 4, 5, 20, 3),
        event(10, 5, 10, 18, 0)
    ]

    horizon = sum(event.estTime for event in eventsData) + max(event.requestedShipDate for event in eventsData)
    model = cp_model.CpModel()

    taskType = collections.namedtuple("taskType", "start end interval event")

    allTasks = {}
    machineToIntervals = collections.defaultdict(list)
    presence = {}

    for event in eventsData:
        tag = f"e{event.orderId}"
        startVar = model.new_int_var(0, horizon, "start_" + tag)
        endVar = model.new_int_var(0, horizon, "end_" + tag)

        presenceList = []
        for machine in range(event.complexity, 4):
            m_tag = f"{tag}m{machine}"
            presenceVar = model.new_bool_var("presence_" + m_tag)
            intervalVar = model.new_optional_interval_var(startVar, event.estTime, endVar, presenceVar, "interval_" + m_tag)
            machineToIntervals[machine].append(intervalVar)
            presence[event.orderId, machine] = presenceVar
            presenceList.append(presenceVar)

        model.add_exactly_one(presenceList)
        allTasks[event.orderId] = taskType(start=startVar, end=endVar, interval=intervalVar, event=event)

    for machine in range(4):
        model.add_no_overlap(machineToIntervals[machine])

    # add an incentive to schedule the same designIds together (to simulate setup time reduction) by adding a constraint that if two events of the same designId are scheduled on the same machine, they must be scheduled back to back
    grouping_vars = []
    for i in range(len(eventsData)):
        for j in range(i + 1, len(eventsData)):
            if eventsData[i].designId == eventsData[j].designId:
                for machine in range(max(eventsData[i].complexity, eventsData[j].complexity), 4):
                    if (eventsData[i].orderId, machine) not in presence or (eventsData[j].orderId, machine) not in presence:
                        continue
                    
                    both_on_machine = model.new_bool_var(f"both_e{eventsData[i].orderId}_e{eventsData[j].orderId}_m{machine}")
                    # both_on_machine is true if and only if both events are on this machine
                    model.add(presence[eventsData[i].orderId, machine] == 1).only_enforce_if(both_on_machine)
                    model.add(presence[eventsData[j].orderId, machine] == 1).only_enforce_if(both_on_machine)
                    model.add(both_on_machine == 1).only_enforce_if([presence[eventsData[i].orderId, machine], presence[eventsData[j].orderId, machine]])
                    
                    grouping_vars.append(both_on_machine)
                    
                    i_before_j = model.new_bool_var(f"i_before_j_e{eventsData[i].orderId}_e{eventsData[j].orderId}_m{machine}")
                    model.add(allTasks[eventsData[i].orderId].end == allTasks[eventsData[j].orderId].start).only_enforce_if([both_on_machine, i_before_j])
                    
                    j_before_i = model.new_bool_var(f"j_before_i_e{eventsData[i].orderId}_e{eventsData[j].orderId}_m{machine}")
                    model.add(allTasks[eventsData[j].orderId].end == allTasks[eventsData[i].orderId].start).only_enforce_if([both_on_machine, j_before_i])
                    
                    model.add_bool_or([i_before_j, j_before_i]).only_enforce_if(both_on_machine)

    objVar = model.new_int_var(0, horizon, "makespan")
    model.add_max_equality(objVar, [allTasks[event.orderId].end for event in eventsData])
    
    # Multi-objective: minimize makespan, maximize grouping (weighted heavily to encourage grouping)
    model.minimize(objVar - 100 * sum(grouping_vars))

    solver = cp_model.CpSolver()
    status = solver.Solve(model)

    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        print(f"Solution with makespan: {solver.ObjectiveValue()}. Rated as {solver.StatusName(status)}.")
        assignedTasks = collections.defaultdict(list)
        for event in eventsData:
            assignedMachine = None
            for machine in range(event.complexity, 4):
                if solver.Value(presence[event.orderId, machine]) == 1:
                    assignedMachine = machine
                    break
            if assignedMachine is not None:
                assignedTasks[assignedMachine].append(
                    (solver.Value(allTasks[event.orderId].start), event)
                )
        for machine in range(4):
            assignedTasks[machine].sort()
            print(f"Machine {machine}:")
            for start, event in assignedTasks[machine]:
                print(f"  Event {event.orderId} Design {event.designId} (start: {start}, estTime: {event.estTime}, requestedShipDate: {event.requestedShipDate}, complexity: {event.complexity})")
    else:
        print("No solution found.")
    print("Time: %.4f seconds" % solver.WallTime())

def tier1_5():
    # fake data
    class task:
        __slots__ = ['duration']
        def __init__(self, duration):
            self.duration = duration
    class job:
        __slots__ = ['tasks']
        def __init__(self, tasks):
            self.tasks = tasks

    jobsData = [
        job([task(3), task(2), task(2)]),
        job([task(2), task(1), task(4)]),
        job([task(4), task(3)])
    ]

    mCnt = 3
    allMachines = range(mCnt)
    horizon = sum(task.duration for job in jobsData for task in job.tasks)

    # (0) instance model
    model = cp_model.CpModel()

    # (1) create variables
    taskType = collections.namedtuple("taskType", "start end")
    assignedTaskType = collections.namedtuple("assignedTaskType", "start job index duration")

    allTasks = {}
    machineToIntervals = collections.defaultdict(list)
    presenceVars = {}

    for jid, job in enumerate(jobsData):
        for tid, task in enumerate(job.tasks):
            suffix = f"j{jid}t{tid}"
            startVar = model.new_int_var(0, horizon, "start_" + suffix)
            endVar = model.new_int_var(0, horizon, "end_" + suffix)
            allTasks[jid, tid] = taskType(start=startVar, end=endVar)

            presence_list = []
            for machine in allMachines:
                m_suffix = f"{suffix}m{machine}"
                presence = model.new_bool_var("present_" + m_suffix)
                s = model.new_int_var(0, horizon, "s_" + m_suffix)
                e = model.new_int_var(0, horizon, "e_" + m_suffix)
                intervalVar = model.new_optional_interval_var(s, task.duration, e, presence, "interval_" + m_suffix)
                machineToIntervals[machine].append(intervalVar)
                presenceVars[jid, tid, machine] = presence
                presence_list.append(presence)
                model.add(startVar == s).only_enforce_if(presence)
                model.add(endVar == e).only_enforce_if(presence)

            model.add_exactly_one(presence_list)

    # (2) add constraints
    # each machine can only process one task at a time
    for machine in allMachines:
        model.add_no_overlap(machineToIntervals[machine])

    # a task must start after its predecessor finishes
    for jid, job in enumerate(jobsData):
        for tid in range(len(job.tasks) - 1):
            model.add(allTasks[jid, tid + 1].start >= allTasks[jid, tid].end)

    # (3) define objective
    # minimize makespan objective
    objVar = model.new_int_var(0, horizon, "makespan")
    model.add_max_equality(objVar, [allTasks[jid, len(job.tasks) - 1].end for jid, job in enumerate(jobsData)])
    model.minimize(objVar)

    # (4) solve
    solver = cp_model.CpSolver()
    status = solver.Solve(model)

    # (5) show results
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        print(f"Solution with makespan: {solver.ObjectiveValue()}. Rated as {solver.StatusName(status)}.")
        assignedTasks = collections.defaultdict(list)
        for jid, job in enumerate(jobsData):
            for tid, task in enumerate(job.tasks):
                assigned_machine = None
                for machine in allMachines:
                    if solver.Value(presenceVars[jid, tid, machine]) == 1:
                        assigned_machine = machine
                        break
                if assigned_machine is None:
                    continue
                assignedTasks[assigned_machine].append(
                    assignedTaskType(start=solver.Value(allTasks[jid, tid].start), job=jid, index=tid, duration=task.duration)
                )
        for machine in allMachines:
            assignedTasks[machine].sort()
            print(f"Machine {machine}:")
            for assignedTask in assignedTasks[machine]:
                print(f"  Job {assignedTask.job} Task {assignedTask.index} (start: {assignedTask.start}, duration: {assignedTask.duration})")
    else:
        print("No solution found.")
    print("Time: %.4f seconds" % solver.WallTime())

def test():
    from collections import namedtuple
    def t_to_idx(hour, minute):
        return (hour - 8) * 12 + minute // 5

    def idx_to_t(time_idx):
        hour = 8 + time_idx // 12
        minute = (time_idx % 12) * 5
        return f"{hour}:{minute:02d}"

    # Define meeting information using namedtuples
    MeetingInfo = namedtuple("MeetingInfo", ["start_times", "duration"])

    # Meeting definitions
    meetings = {
        "meeting_a": MeetingInfo(
            start_times=[
                [t_to_idx(8, 0), t_to_idx(12, 0)],
                [t_to_idx(16, 0), t_to_idx(17, 0)],
            ],
            duration=120 // 5,  # 2 hours
        ),
        "meeting_b": MeetingInfo(
            start_times=[
                [t_to_idx(10, 0), t_to_idx(12, 0)],
            ],
            duration=30 // 5,  # 30 minutes
        ),
        "meeting_c": MeetingInfo(
            start_times=[
                [t_to_idx(16, 0), t_to_idx(17, 0)],
            ],
            duration=15 // 5,  # 15 minutes
        ),
        "meeting_d": MeetingInfo(
            start_times=[
                [t_to_idx(8, 0), t_to_idx(10, 0)],
                [t_to_idx(12, 0), t_to_idx(14, 0)],
            ],
            duration=60 // 5,  # 1 hour
        ),
    }

    # Create a new CP-SAT model
    model = cp_model.CpModel()

    # Create start time variables for each meeting
    start_time_vars = {
        meeting_name: model.new_int_var_from_domain(
            cp_model.Domain.from_intervals(meeting_info.start_times),
            f"start_{meeting_name}",
        )
        for meeting_name, meeting_info in meetings.items()
    }

    # Create interval variables for each meeting
    interval_vars = {
        meeting_name: model.new_fixed_size_interval_var(
            start=start_time_vars[meeting_name],
            size=meeting_info.duration,
            name=f"interval_{meeting_name}",
        )
        for meeting_name, meeting_info in meetings.items()
    }

    # Ensure that now two meetings overlap
    model.add_no_overlap(list(interval_vars.values()))

    # if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
    #     print(f"Solution found: a={solver.Value(a)}, b={solver.Value(b)}, c={solver.Value(c)}. Rated as {solver.StatusName(status)}.")
    # else:
    #     print("No solution found.")
