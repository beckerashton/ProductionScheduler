from asyncio import events
import collections
import stat
from ortools.sat.python import cp_model

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

tier3()