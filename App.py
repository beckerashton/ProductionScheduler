import collections
from logging import config
import os
import traceback
from datetime import date
from pathlib import Path
from typing import TypedDict

from matplotlib import table
from numpy.random import f
import pandas as pd
import plotly.graph_objects as go
from nicegui.events import GenericEventArguments
from nicegui import app, run, ui

from Types import Event, EventGroup

from cpsat import SchedulerInstance, SchedulerSolver, SchedulerSolverConfig

DEFAULT_EXCEL_PATH = os.getenv("EXCEL_PATH", "")
WORKDAY_MINUTES = 8 * 60
MAX_UI_SOLUTIONS = 25
DEFAULT_MACHINES = [
	{"id": 1, "colors": 12, "flashes": 3},
	{"id": 2, "colors": 8, "flashes": 3},
	{"id": 4, "colors": 12, "flashes": 3},
	{"id": 5, "colors": 6, "flashes": 2},
	{"id": 6, "colors": 50, "flashes": 10},
	{"id": 7, "colors": 6, "flashes": 3},
]

class GroupedEvent(TypedDict):
	designName: str
	estTime: int
	requestedShipDate: date
	colors: int
	flashes: int


def _load_instance_from_excel(
	excel_path: str,
	excel_filter: str,
	machine_config: dict[int, dict[str, str | int | bool]],
	global_hours_per_week: int,
	manual_events: list[dict] | None = None,
	ignored_group_ids: set[int] | None = None,
) -> SchedulerInstance:
	input_df = pd.read_excel(
		excel_path,
		sheet_name="AssignEvents",
		header=3,
		usecols="B,C,D,E,J,M,R,S,T",
	)
	input_df = input_df.dropna(
		subset=["Order No", "Design No", "Location", "DueDate", "Imp", "No_Colors", "No_Flashes"]
	)
	if excel_filter:
		input_df = input_df.query(excel_filter)

	df = input_df.rename(
		columns={
			"Order No": "id_Order",
			"Design No": "id_Design",
			"Design Name": "design_Name",
			"Location": "Location",
			"DueDate": "date_OrderRequestedToShip",
			"Imp": "cn_QtyToProduce",
			"No_Colors": "ColorsTotal",
			"No_Flashes": "flashes",
		}
	)
	df = df.dropna(
		subset=["id_Order", "id_Design", "design_Name", "Location", "date_OrderRequestedToShip", "cn_QtyToProduce", "ColorsTotal", "flashes"]
	)

	df["runTime"] = df.apply(lambda row: row["cn_QtyToProduce"] / 300 * 60, axis=1)
	df["setupTime"] = df.apply(lambda row: row["ColorsTotal"] * 10, axis=1)
	df["colors"] = df["ColorsTotal"]
	df["designId"] = df.apply(lambda row: f"{int(row['id_Design'])}_{row['Location']}", axis=1)
	df["date_OrderRequestedToShip"] = df["date_OrderRequestedToShip"].apply(
		lambda x: x.date() if isinstance(x, pd.Timestamp) else date.fromisoformat(x) if isinstance(x, str) else x
	)

	events = [
		Event(
			row["id_Order"],
			row["designId"],
			row["runTime"],
			row["setupTime"],
			row["date_OrderRequestedToShip"],
			row["colors"],
			row["flashes"],
		)
		for _, row in df.iterrows()
	]

	design_name_by_id = dict(zip(df["designId"], df["design_Name"]))

	events_grouped: dict[str, GroupedEvent] = {}
	for event in events:
		if event.designId not in events_grouped:
			events_grouped[event.designId] = {
				"designName": str(design_name_by_id.get(event.designId, event.designId)),
				"estTime": int(event.setupTime + event.runTime),
				"requestedShipDate": event.requestedShipDate,
				"colors": int(event.colors),
				"flashes": int(event.flashes),
			}
		else:
			grouped = events_grouped[event.designId]
			grouped["estTime"] += int(event.runTime)
			grouped["requestedShipDate"] = min(grouped["requestedShipDate"], event.requestedShipDate)
			grouped["colors"] = max(grouped["colors"], int(event.colors))
			grouped["flashes"] = max(grouped["flashes"], int(event.flashes))

	grouped_events = [
		EventGroup(
			i,
			k,
			v["designName"],
			v["estTime"],
			v["colors"],
			v["flashes"],
			(v["requestedShipDate"] - date.today()).days * WORKDAY_MINUTES,
		)
		for i, (k, v) in enumerate(events_grouped.items())
	]

	# Append manually added events, assigning IDs after the Excel-sourced ones
	if manual_events:
		next_id = len(grouped_events)
		for m in manual_events:
			ship_date = m.get("requestedShipDate")
			if isinstance(ship_date, date):
				ship_days = (ship_date - date.today()).days * WORKDAY_MINUTES
			else:
				ship_days = int(ship_date or 0)
			grouped_events.append(EventGroup(
				next_id,
				m.get("designId", f"manual_{next_id}"),
				m.get("designName", f"Manual Event {next_id}"),
				int(m.get("estTime", 60)),
				int(m.get("colors", 1)),
				int(m.get("flashes", 0)),
				ship_days,
			))
			next_id += 1

	# Filter out ignored events
	if ignored_group_ids:
		grouped_events = [e for e in grouped_events if e.groupId not in ignored_group_ids]

	enabled_machines = [
		{
			"id": int(machine_id),
			"colors": int(machine["colors"]),
			"flashes": int(machine["flashes"]),
			"hours_per_week": max(0, int(global_hours_per_week)),
		}
		for machine_id, machine in machine_config.items()
		if bool(machine.get("enabled", True))
	]

	if not enabled_machines:
		raise ValueError("At least one machine must be enabled.")

	return SchedulerInstance(events=grouped_events, machines=enabled_machines)


def _minutes_to_datetime_text(model_minutes: int) -> str:
	day_offset, minute_of_day = divmod(max(0, int(model_minutes)), WORKDAY_MINUTES)
	actual_date = date.today() + pd.Timedelta(days=day_offset)
	hour = 8 + minute_of_day // 60
	minute = minute_of_day % 60
	return f"{actual_date.isoformat()} {hour:02d}:{minute:02d}"

def _location_from_design_id(design_id: str) -> str:
	if "_" not in design_id:
		return "Unknown"
	_, location = design_id.split("_", 1)
	normalized = location.strip()
	return normalized if normalized else "Unknown"


def _build_schedule_plotly_figure(schedule: list[dict], instance: SchedulerInstance, objective_value: float) -> go.Figure:
	fig = go.Figure()

	if not schedule:
		fig.update_layout(
			title="Schedule by Machine",
			annotations=[
				{
					"text": "No scheduled jobs to display",
					"showarrow": False,
					"xref": "paper",
					"yref": "paper",
					"x": 0.5,
					"y": 0.5,
				}
			],
		)
		return fig

	event_by_group_id = {event.groupId: event for event in instance.events}
	location_palette = [
		"#1b9e77",
		"#d95f02",
		"#7570b3",
		"#e7298a",
		"#66a61e",
		"#e6ab02",
		"#a6761d",
		"#666666",
		"#0b84a5",
		"#f6c85f",
		"#ca472f",
		"#8dddd0",
		"#b30000",
	]

	locations = sorted(
		{
			_location_from_design_id(event_by_group_id[job["groupId"]].designId)
			for job in schedule
			if job["groupId"] in event_by_group_id
		}
	)
	location_to_color = {
		location: location_palette[index % len(location_palette)]
		for index, location in enumerate(locations)
	}

	legend_seen: set[str] = set()
	for job in schedule:
		event = event_by_group_id[job["groupId"]]
		location = _location_from_design_id(event.designId)
		machine_label = f"Machine {job['assignedMachineId']}"
		start = int(job["scheduledStartDate"])
		end = int(job["scheduledEndDate"])
		duration = end - start
		show_legend = location not in legend_seen
		legend_seen.add(location)

		fig.add_trace(
			go.Bar(
				x=[duration],
				y=[machine_label],
				base=[start],
				orientation="h",
				marker={
					"color": location_to_color.get(location, "#888888"),
					"line": {"color": "#111111", "width": 0.6},
				},
				text=[event.designName],
				textposition="inside",
				name=location,
				legendgroup=f"location_{location}",
				showlegend=show_legend,
				customdata=[[
					int(job["groupId"]),
					str(event.designId),
					str(event.colors),
					str(event.flashes),
					int(job["assignedMachineId"]),
					int(start),
					int(end),
					int(duration),
					_minutes_to_datetime_text(start),
					_minutes_to_datetime_text(end),
					int(event.estTime),
					int(job["requestedShipDate"]),
					_minutes_to_datetime_text(int(job["requestedShipDate"])),
					location,
					str(event.designName),
				]],
				hovertemplate=(
					f"Group ID: {job['groupId']}<br>"
					f"Design Name: {event.designName}<br>"
					f"Design ID: {event.designId}<br>"
					f"Location: {location}<br>"
					f"Colors: {event.colors}<br>"
					f"Flashes: {event.flashes}<br>"
					f"Machine: {machine_label}<br>"
					f"Start: %{{customdata[8]}}<br>"
					f"End: %{{customdata[9]}}<br>"
					f"Duration: %{{customdata[7]}} min<br>"
					f"Estimated Time: {event.estTime} min<br>"
					f"Requested Ship: %{{customdata[12]}}<br>"
				),
			)
		)

	max_end = max(int(job["scheduledEndDate"]) for job in schedule)
	tick_limit = ((max_end + WORKDAY_MINUTES - 1) // WORKDAY_MINUTES + 1) * WORKDAY_MINUTES
	tick_values = list(range(0, tick_limit + 1, WORKDAY_MINUTES))
	tick_labels = [f"Day {value // WORKDAY_MINUTES}" for value in tick_values]
	machine_labels = sorted({f"Machine {job['assignedMachineId']}" for job in schedule}, key=lambda label: int(label.split()[-1]))

	fig.update_layout(
		title=f"Schedule by Machine (objective {objective_value:g})",
		hovermode="closest",
		barmode="overlay",
		template="plotly_white",
		height=640,
		xaxis={
			"title": "Schedule Timeline",
			"tickmode": "array",
			"tickvals": tick_values,
			"ticktext": tick_labels,
			"showgrid": True,
			"gridcolor": "rgba(0, 0, 0, 0.15)",
		},
		yaxis={
			"title": "Machine",
			"categoryorder": "array",
			"categoryarray": machine_labels,
		},
		legend={"title": {"text": "Location"}},
	)

	return fig


def _run_scheduler(
	excel_path: str,
	excel_filter: str,
	objective: str,
	time_limit_seconds: int,
	worker_count: int,
	optimization_tolerance: float,
	enumerate_all_solutions: bool,
	add_force_before_ship_date: bool,
	add_force_before_ship_date_ignore_lates: bool,
	add_sequence_constraint: bool,
	add_pad_between_events_constraint: bool,
	multi_makespan_checks: int,
	global_hours_per_week: int,
	machine_config: dict[int, dict[str, str | int | bool]],
	locked_events: list[dict] | None = None,
	manual_events: list[dict] | None = None,
	ignored_group_ids: set[int] | None = None,
) -> dict:
	if locked_events is None:
		locked_events = []
	instance = _load_instance_from_excel(
		excel_path,
		excel_filter,
		machine_config,
		global_hours_per_week,
		manual_events,
		ignored_group_ids,
	)
	config = SchedulerSolverConfig(
		time_limit_seconds=max(1, int(time_limit_seconds)),
		log_search_progress=False,
		optimization_tolerance=max(0.0, float(optimization_tolerance)),
		num_search_workers=max(1, int(worker_count)),
		enumerate_all_solutions=enumerate_all_solutions,
	)

	solver = SchedulerSolver(instance, config, locked_events)

	if add_force_before_ship_date:
		solver._add_constraint_force_before_ship_date()
	if add_force_before_ship_date_ignore_lates:
		solver._add_constraint_force_before_ship_date_ignore_lates()
	if add_sequence_constraint:
		solver._add_constraint_sequence_subevents()
	if add_pad_between_events_constraint:
		solver._add_constraint_pad_between_events()
	if locked_events:
		solver._add_constraint_locked_events(locked_events)

	if objective == "makespan":
		solver._set_makespan_objective()
	elif objective == "multi_makespan":
		solver._set_multi_makespan_objective(max(1, int(multi_makespan_checks)))
	elif objective == "makespan_with_tardiness":
		solver._set_makespan_with_tardiness_penalty_objective()
	elif objective == "balanced":
		solver._set_balanced_objective()
	else:
		raise ValueError(f"Unknown objective: {objective}")

	solution = solver.solve(time_limit=float(time_limit_seconds))
	all_solver_schedules = [solution.schedule, *solution.equally_optimal_schedules]
	truncated_solution_count = max(0, len(all_solver_schedules) - MAX_UI_SOLUTIONS)
	display_schedules = all_solver_schedules[:MAX_UI_SOLUTIONS]
	all_schedule_rows = [
		sorted(
			schedule,
			key=lambda row: (row["assignedMachineId"], row["scheduledStartDate"], row["groupId"]),
		)
		for schedule in display_schedules
	]
	all_graph_figures = [
		_build_schedule_plotly_figure(rows, instance, solution.objective_value).to_dict()
		for rows in all_schedule_rows
	]
	schedule_rows = all_schedule_rows[0] if all_schedule_rows else []
	graph_figure = all_graph_figures[0] if all_graph_figures else go.Figure().to_dict()

	return {
		"status": solution.status,
		"objective_value": solution.objective_value,
		"rows": schedule_rows,
		"row_count": len(schedule_rows),
		"equally_optimal_count": len(solution.equally_optimal_schedules),
		"graph_figure": graph_figure,
		"all_rows": all_schedule_rows,
		"all_graph_figures": all_graph_figures,
		"solution_count": len(all_schedule_rows),
		"truncated_solution_count": truncated_solution_count,
	}

def _style() -> None:
	ui.add_css('''
		.configuration {
			width: 100%;
		}
		.configuration .q-expansion-item__container > .q-item {
			background-color: #f0f0f0;
			border-radius: 8px;
		}
	''')

app.native.window_args['resizable'] = True

@ui.page('/')
async def main_page():
	await ui.context.client.connected()

	_style()

	config = {
		"excel_path": DEFAULT_EXCEL_PATH,
		"excel_filter": "Week_Sch == 13",
		"objective": "multi_makespan",
		"time_limit_seconds": 30,
		"worker_count": 16,
		"optimization_tolerance": 0.01,
		"enumerate_all_solutions": False,
		"add_force_before_ship_date": False,
		"add_force_before_ship_date_ignore_lates": False,
		"add_sequence_constraint": True,
		"add_pad_between_events_constraint": False,
		"multi_makespan_checks": 4,
	}
	state = {
		"active_tab": "Schedule Table",
		"schedule_result": None,
		"locked_events": [],
		"manual_events": [],
		"ignored_group_ids": set(),
		"selected_solution_index": 0,
	}

	default_machine_hours_per_week = 40
	machine_config = {
		1: {"name": "Press #1 - Gauntlet III", 
	  		"colors": 12, "flashes": 3},
		
		2: {"name": "Press #2 - Gauntlet III", 
	  		"colors": 8, "flashes": 3},
		
		3: {"name": "Press #3 - Gauntlet III", 
	  		"colors": 12, "flashes": 3},  # manually toggle off because of sample prod, later samples will be integrated into scheduler as reservations
		
		4: {"name": "Press #4 - Gauntlet III", 
	  		"colors": 12, "flashes": 3},  # override for 9/4 designs found deeper in cpsat in the class def of _EventSchedulingVars
		
		5: {"name": "Press #5 - Sportsman", 
	  		"colors": 6, "flashes": 2},
		
		6: {"name": "Press #6 - Stryker", 
	  		"colors": 50, "flashes": 10}, # arbitrarily high-capacity, not proper numbers
		
		7: {"name": "Press #7 - Gauntlet III", 
	  		"colors": 6, "flashes": 3}
	}
	for machine_id, machine in machine_config.items():
		machine["enabled"] = True
	global_hours_per_week = default_machine_hours_per_week
	del default_machine_hours_per_week

	ui.colors(primary="#005f73", secondary="#0a9396", accent="#ee9b00")

	ui.label("CP-SAT Scheduler Console").classes("text-2xl font-bold")
	ui.label("Run your scheduler, toggle constraints, choose an objective, and view the resulting matplotlib graph.").classes(
		"text-gray-700"
	)

	# with ui.card().classes("w-full"):
	with ui.expansion("Scheduler Configuration", value=False).classes("configuration"):
		with ui.row().classes("w-full items-end gap-4"):
			excel_path_input = ui.input("Excel path", value=config["excel_path"]).classes("w-[42rem]")
			excel_filter_input = ui.input("Excel filter (pandas query syntax)", value=config["excel_filter"]).classes("w-[24rem]")
			time_limit_input = ui.number("Time limit (seconds)", value=config["time_limit_seconds"], min=1, precision=0)
			worker_count_input = ui.number("Search workers", value=config["worker_count"], min=1, precision=0)
			tolerance_input = ui.number("Optimization tolerance", value=config["optimization_tolerance"], min=0, step=0.001)

		with ui.row().classes("w-full items-end gap-4"):
			objective_select = ui.select(
				{
					"makespan": "Makespan",
					"multi_makespan": "Multi-layer Makespan",
					"makespan_with_tardiness": "Makespan + Tardiness Penalty",
					"balanced": "Balanced Load",
				},
				value=config["objective"],
				label="Objective",
			)
			multi_makespan_checks_input = ui.number("Multi-makespan checks", value=config["multi_makespan_checks"], min=1, precision=0)
			enumerate_switch = ui.switch("Enumerate equally optimal solutions", value=config["enumerate_all_solutions"])

		with ui.row().classes("w-full gap-8"):
			with ui.column().classes("gap-1"):
				ui.label("Optional constraints").classes("text-base font-semibold")
				force_ship_date_switch = ui.switch("Force all events before ship date", value=config["add_force_before_ship_date"])
				force_ship_date_ignore_lates_switch = ui.switch("Force before ship date (ignore already-late)", value=config["add_force_before_ship_date_ignore_lates"])
				sequence_switch = ui.switch("Force sequence of subevents by location", value=config["add_sequence_constraint"])
				pad_switch = ui.switch("Add 1-minute pad between machine events", value=config["add_pad_between_events_constraint"])

	with ui.expansion("Machine Configuration", value=True).classes("configuration"):
		global_hours_per_week_input = ui.number(
			"Global Hours/week per enabled machine",
			value=global_hours_per_week,
			min=0,
			max=168,
			precision=0,
		).classes("w-64").props("dense")
		with ui.row().classes("w-full flex-wrap gap-4"):
			for machine_id, machine in machine_config.items():
				with ui.card().classes("p-3 gap-2"):
					with ui.row().classes("items-center gap-2"):
						ui.label(f"{machine['name']}").classes("text-sm font-semibold")
						ui.checkbox(value=True).bind_value(machine, "enabled").classes("q-mt-xs")
					ui.label(f"ID {machine_id} · {machine['colors']} colors · {machine['flashes']} flashes").classes("text-xs text-gray-500")
	
	with ui.expansion("Locked Events", value=False).classes("configuration"):
		with ui.row().classes("w-full items-end gap-2"):
			locked_group_id_input = ui.input("Event Group ID", value="", placeholder="Enter group ID").classes("w-32")
			locked_machine_select = ui.select(
				{str(mid): f"Machine {mid} - {machine_config[mid]['name']}" for mid in machine_config.keys()},
				label="Machine",
				value=str(next(iter(machine_config.keys()))),
			).classes("w-64")
			
			locked_use_time = ui.checkbox("Lock to time", value=False).classes("q-mt-xs")
			locked_time_input = ui.input("Start time (model min)", value="0", placeholder="0").classes("w-32")
			locked_time_input.enabled = locked_use_time.value
			
			def toggle_time_input():
				locked_time_input.enabled = locked_use_time.value
				locked_time_input.update()
			
			locked_use_time.on_value_change(toggle_time_input)
			
			def add_locked_event():
				try:
					group_id = int(locked_group_id_input.value or 0)
					machine_id = int(locked_machine_select.value or str(next(iter(machine_config.keys()))))
					start_time = None
					if locked_use_time.value:
						start_time = int(locked_time_input.value)
					
					new_lock = {
						"groupId": group_id,
						"machineId": machine_id,
					}
					if start_time is not None:
						new_lock["startTime"] = start_time
					
					state["locked_events"].append(new_lock)
					refresh_locked_table()
					
					locked_group_id_input.value = ""
					locked_time_input.value = "0"
					ui.notify(f"Locked event {group_id} to machine {machine_id}", color="positive")
				except ValueError:
					ui.notify("Invalid input. Group ID must be an integer.", color="negative")
			
			ui.button("Add Lock", on_click=add_locked_event).classes("px-4")
		
		locked_events_list = ui.column().classes("w-full gap-1 q-mt-sm")

		def refresh_locked_table():
			locked_events_list.clear()
			with locked_events_list:
				if not state["locked_events"]:
					ui.label("No locked events.").classes("text-sm text-gray-400")
					return
				for lock in state["locked_events"]:
					group_id = lock["groupId"]
					machine_id = lock["machineId"]
					machine_name = machine_config.get(machine_id, {}).get("name", f"Machine {machine_id}")
					start_label = f"  ·  Start: {lock['startTime']} min" if "startTime" in lock else ""

					def make_delete(gid: int):
						def do_delete():
							state["locked_events"] = [l for l in state["locked_events"] if l["groupId"] != gid]
							refresh_locked_table()
							ui.notify(f"Removed lock for event {gid}", color="info")
						return do_delete

					with ui.row().classes("items-center gap-4 px-3 py-1 rounded border border-gray-200 w-full"):
						ui.label(f"Event {group_id}").classes("text-sm font-mono w-20")
						ui.label(f"{machine_name}").classes("text-sm flex-1")
						if start_label:
							ui.label(start_label).classes("text-sm text-gray-500")
						ui.button(icon="delete", on_click=make_delete(group_id)).props("flat round dense color=negative")

		refresh_locked_table()

	with ui.expansion("Manual Events", value=False).classes("configuration"):
		with ui.row().classes("w-full items-end gap-2"):
			manual_name_input = ui.input("Design Name", placeholder="e.g. My Logo").classes("w-48")
			manual_colors_input = ui.number("Colors", value=1, min=1, max=50, precision=0).classes("w-20")
			manual_flashes_input = ui.number("Flashes", value=0, min=0, max=10, precision=0).classes("w-20")
			manual_esttime_input = ui.number("Est. Time (min)", value=60, min=1, precision=0).classes("w-32")
			manual_shipdate_input = ui.input("Ship Date (YYYY-MM-DD)", placeholder=str(date.today())).classes("w-40")

			def add_manual_event():
				try:
					ship_date = date.fromisoformat(str(manual_shipdate_input.value).strip()) if str(manual_shipdate_input.value).strip() else date.today()
					design_name = str(manual_name_input.value or "Manual Event").strip()
					design_id = f"manual_{len(state['manual_events'])}_{design_name.lower().replace(' ', '_')}"
					new_event = {
						"designId": design_id,
						"designName": design_name,
						"colors": int(manual_colors_input.value or 1),
						"flashes": int(manual_flashes_input.value or 0),
						"estTime": int(manual_esttime_input.value or 60),
						"requestedShipDate": ship_date,
					}
					state["manual_events"].append(new_event)
					refresh_manual_events_list()
					manual_name_input.value = ""
					ui.notify(f"Added manual event: {design_name}", color="positive")
				except ValueError as exc:
					ui.notify(f"Invalid input: {exc}", color="negative")

			ui.button("Add Event", on_click=add_manual_event).classes("px-4")

		manual_events_list = ui.column().classes("w-full gap-1 q-mt-sm")

		def refresh_manual_events_list():
			manual_events_list.clear()
			with manual_events_list:
				if not state["manual_events"]:
					ui.label("No manual events added.").classes("text-sm text-gray-400")
					return
				for idx, m in enumerate(state["manual_events"]):
					def make_delete_manual(i: int):
						def do_delete():
							state["manual_events"].pop(i)
							refresh_manual_events_list()
							ui.notify("Removed manual event", color="info")
						return do_delete
					with ui.row().classes("items-center gap-4 px-3 py-1 rounded border border-gray-200 w-full"):
						ui.label(m["designName"]).classes("text-sm font-semibold flex-1")
						ui.label(f"{m['colors']} colors · {m['flashes']} flashes · {m['estTime']} min · ship {m['requestedShipDate']}").classes("text-xs text-gray-500")
						ui.button(icon="delete", on_click=make_delete_manual(idx)).props("flat round dense color=negative")

		refresh_manual_events_list()

	with ui.expansion("Ignored Events", value=False).classes("configuration"):
		with ui.row().classes("w-full items-end gap-2"):
			ignore_group_id_input = ui.input("Event Group ID", placeholder="Enter group ID").classes("w-32")

			def add_ignored_event():
				try:
					group_id = int(ignore_group_id_input.value or 0)
					state["ignored_group_ids"].add(group_id)
					refresh_ignored_list()
					ignore_group_id_input.value = ""
					ui.notify(f"Event {group_id} will be ignored", color="positive")
				except ValueError:
					ui.notify("Group ID must be an integer.", color="negative")

			ui.button("Ignore Event", on_click=add_ignored_event).classes("px-4")

		ignored_events_list = ui.column().classes("w-full gap-1 q-mt-sm")

		def refresh_ignored_list():
			ignored_events_list.clear()
			with ignored_events_list:
				if not state["ignored_group_ids"]:
					ui.label("No events ignored.").classes("text-sm text-gray-400")
					return
				for gid in sorted(state["ignored_group_ids"]):
					def make_unignore(g: int):
						def do_unignore():
							state["ignored_group_ids"].discard(g)
							refresh_ignored_list()
							ui.notify(f"Event {g} restored", color="info")
						return do_unignore
					with ui.row().classes("items-center gap-4 px-3 py-1 rounded border border-gray-200 w-full"):
						ui.label(f"Event {gid}").classes("text-sm font-mono flex-1")
						ui.button(icon="delete", on_click=make_unignore(gid)).props("flat round dense color=negative")

		refresh_ignored_list()

	status_label = ui.label("Ready.").classes("text-sm")
	summary_label = ui.label("").classes("text-sm")

	run_button = ui.button("Run Scheduler")
	with ui.row().classes("items-center gap-2"):
		prev_solution_button = ui.button("Prev Solution").props("outline dense")
		next_solution_button = ui.button("Next Solution").props("outline dense")
		solution_pick = ui.select(options={"0": "Solution 1 (primary)"}, value="0", label="Solution").classes("w-56")
		solution_status_label = ui.label("Solution 1/1").classes("text-sm text-gray-700")

	with ui.tabs().classes("w-full") as tabs:
		table_tab = ui.tab("Schedule Table")
		graph_tab = ui.tab("Schedule Graph")

	with ui.tab_panels(tabs= tabs, value= state["active_tab"], on_change= lambda e: state.update({"active_tab": e.value})).classes("w-full"):
		with ui.tab_panel(table_tab):
			result_table = ui.table(
				columns=[
					{"name": "groupId", "label": "Group ID", "field": "groupId", "sortable": True},
					{"name": "designId", "label": "Design ID", "field": "designId", "sortable": True},
					{"name": "designName", "label": "Design Name", "field": "designName", "sortable": True},
					{"name": "assignedMachineId", "label": "Machine", "field": "assignedMachineId", "sortable": True},
					{
						"name": "scheduledStartDate",
						"label": "Start (model min)",
						"field": "scheduledStartDate",
						"sortable": True,
					},
					{
						"name": "scheduledEndDate",
						"label": "End (model min)",
						"field": "scheduledEndDate",
						"sortable": True,
					},
					{"name": "requestedShipDate", "label": "Requested Ship", "field": "requestedShipDate", "sortable": True},
				],
				rows=[],
				row_key="groupId",
			).classes("w-full")
			
		with ui.tab_panel(graph_tab):
			graph_plot = ui.plotly(go.Figure()).classes("w-full")
			selected_job_label = ui.label("Click a bar to inspect a scheduled job.").classes("text-sm text-gray-700")

	def _render_selected_solution() -> None:
		result = state.get("schedule_result")
		if not result:
			result_table.rows = []
			result_table.update()
			graph_plot.figure = go.Figure()
			graph_plot.update()
			solution_pick.options = {"0": "Solution 1 (primary)"}
			solution_pick.value = "0"
			solution_pick.update()
			solution_status_label.text = "Solution 1/1"
			prev_solution_button.disable()
			next_solution_button.disable()
			return

		count = max(1, int(result.get("solution_count", 1)))
		idx = max(0, min(int(state.get("selected_solution_index", 0)), count - 1))
		state["selected_solution_index"] = idx

		result_table.rows = result["all_rows"][idx]
		result_table.update()
		graph_plot.figure = go.Figure(result["all_graph_figures"][idx])
		graph_plot.update()

		solution_pick.options = {
			str(i): f"Solution {i + 1}{' (primary)' if i == 0 else ''}"
			for i in range(count)
		}
		solution_pick.value = str(idx)
		solution_pick.update()
		solution_status_label.text = f"Solution {idx + 1}/{count}"

		if idx > 0:
			prev_solution_button.enable()
		else:
			prev_solution_button.disable()
		if idx < count - 1:
			next_solution_button.enable()
		else:
			next_solution_button.disable()

	def _on_solution_pick_change(e: GenericEventArguments) -> None:
		raw_value = e.args.get("value", "0") if isinstance(e.args, dict) else e.args
		state["selected_solution_index"] = int(str(raw_value or "0"))
		_render_selected_solution()

	def _on_prev_solution() -> None:
		state["selected_solution_index"] = max(0, int(state.get("selected_solution_index", 0)) - 1)
		_render_selected_solution()

	def _on_next_solution() -> None:
		result = state.get("schedule_result") or {}
		count = max(1, int(result.get("solution_count", 1)))
		state["selected_solution_index"] = min(count - 1, int(state.get("selected_solution_index", 0)) + 1)
		_render_selected_solution()

	prev_solution_button.on_click(_on_prev_solution)
	next_solution_button.on_click(_on_next_solution)
	solution_pick.on("update:model-value", _on_solution_pick_change)
	
	if state["schedule_result"]:
		_render_selected_solution()
		summary_label.text = (
				f"Status: {state['schedule_result']['status']} | Objective: {state['schedule_result']['objective_value']:.3f} | "
				f"Rows: {state['schedule_result']['row_count']} | Equally optimal alternates: {state['schedule_result']['equally_optimal_count']}"
			)
	else:
		_render_selected_solution()


	def on_graph_click(event: GenericEventArguments) -> None:
		points = event.args.get("points", []) if isinstance(event.args, dict) else []
		if not points:
			return

		custom = points[0].get("customdata")
		if not custom or len(custom) < 12:
			selected_job_label.text = "Clicked chart element has no job metadata."
			return

		selected_job_label.text = (
			f"Selected Group {custom[0]} | Design {custom[1]} | Machine {custom[2]} | "
			f"Start {custom[6]} | End {custom[7]}"
		)


	graph_plot.on("plotly_click", on_graph_click)


	async def on_run_click() -> None:
		excel_path = str(excel_path_input.value or "").strip()
		excel_filter = str(excel_filter_input.value or "")
		objective = str(objective_select.value or "")
		time_limit_seconds = int(time_limit_input.value or 30)
		worker_count = int(worker_count_input.value or 1)
		optimization_tolerance = float(tolerance_input.value or 0)
		enumerate_all_solutions = bool(enumerate_switch.value)
		add_force_before_ship_date = bool(force_ship_date_switch.value)
		add_force_before_ship_date_ignore_lates = bool(force_ship_date_ignore_lates_switch.value)
		add_sequence_constraint = bool(sequence_switch.value)
		add_pad_between_events_constraint = bool(pad_switch.value)
		multi_makespan_checks = int(multi_makespan_checks_input.value or 1)
		global_hours_per_week = int(global_hours_per_week_input.value or 0)
		machine_runtime_config = {
			machine_id: {
				"name": str(machine["name"]),
				"colors": int(machine["colors"]),
				"flashes": int(machine["flashes"]),
				"enabled": bool(machine.get("enabled", True)),
			}
			for machine_id, machine in machine_config.items()
		}
		if not excel_path:
			ui.notify("Excel path is required.", color="negative")
			return

		if not Path(excel_path).exists():
			ui.notify(f"Excel file was not found: {excel_path}", color="negative")
			return

		if force_ship_date_switch.value and force_ship_date_ignore_lates_switch.value:
			ui.notify("Choose only one ship-date constraint mode.", color="warning")
			return

		run_button.disable()
		status_label.text = "Running scheduler..."
		summary_label.text = ""

		try:
			state["schedule_result"] = await run.io_bound(
				_run_scheduler,
				excel_path,
				excel_filter,
				objective,
				time_limit_seconds,
				worker_count,
				optimization_tolerance,
				enumerate_all_solutions,
				add_force_before_ship_date,
				add_force_before_ship_date_ignore_lates,
				add_sequence_constraint,
				add_pad_between_events_constraint,
				multi_makespan_checks,
				global_hours_per_week,
				machine_runtime_config,
				state["locked_events"],
				state["manual_events"],
				state["ignored_group_ids"],
			)
			state["selected_solution_index"] = 0
			_render_selected_solution()
			selected_job_label.text = "Click a bar to inspect a scheduled job."

			summary_label.text = (
				f"Status: {state['schedule_result']['status']} | Objective: {state['schedule_result']['objective_value']:.3f} | "
				f"Rows: {state['schedule_result']['row_count']} | Equally optimal alternates: {state['schedule_result']['equally_optimal_count']}"
			)
			if int(state["schedule_result"].get("truncated_solution_count", 0)) > 0:
				summary_label.text += (
					f" | Showing first {state['schedule_result']['solution_count']} solutions in UI "
					f"({state['schedule_result']['truncated_solution_count']} more not rendered)"
				)
			status_label.text = "Run completed."
			ui.notify("Scheduler run complete.", color="positive")
		except Exception as exc:
			status_label.text = "Run failed."
			summary_label.text = ""
			ui.notify(f"Error: {exc}", color="negative", multi_line=True)
			print(traceback.format_exc())
		finally:
			run_button.enable()


	run_button.on_click(on_run_click)

ui.run(title="Scheduler Frontend", native= True, window_size=(400, 300), fullscreen=False)
