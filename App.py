import collections
from logging import config
import os
import traceback
from datetime import date
from pathlib import Path
from typing import TypedDict

from matplotlib import table
import pandas as pd
import plotly.graph_objects as go
from nicegui.events import GenericEventArguments
from nicegui import app, run, ui

from Types import Event, EventGroup

from cpsat import SchedulerInstance, SchedulerSolver, SchedulerSolverConfig

DEFAULT_EXCEL_PATH = os.getenv("EXCEL_PATH", "")
WORKDAY_MINUTES = 8 * 60
DEFAULT_MACHINES = [
	{"id": 1, "colors": 12, "flashes": 3},
	{"id": 2, "colors": 8, "flashes": 3},
	{"id": 4, "colors": 12, "flashes": 3},
	{"id": 5, "colors": 6, "flashes": 2},
	{"id": 6, "colors": 50, "flashes": 10},
	{"id": 7, "colors": 6, "flashes": 3},
]

class GroupedEvent(TypedDict):
	estTime: int
	requestedShipDate: date
	colors: int
	flashes: int


def _load_instance_from_excel(excel_path: str, excel_filter: str) -> SchedulerInstance:
	input_df = pd.read_excel(
		excel_path,
		sheet_name="AssignEvents",
		header=3,
		usecols="B,C,E,J,M,R,S,T",
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
			"Location": "Location",
			"DueDate": "date_OrderRequestedToShip",
			"Imp": "cn_QtyToProduce",
			"No_Colors": "ColorsTotal",
			"No_Flashes": "flashes",
		}
	)
	df = df.dropna(
		subset=["id_Order", "id_Design", "Location", "date_OrderRequestedToShip", "cn_QtyToProduce", "ColorsTotal", "flashes"]
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

	events_grouped: dict[str, GroupedEvent] = {}
	for event in events:
		if event.designId not in events_grouped:
			events_grouped[event.designId] = {
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
			v["estTime"],
			v["colors"],
			v["flashes"],
			(v["requestedShipDate"] - date.today()).days * WORKDAY_MINUTES,
		)
		for i, (k, v) in enumerate(events_grouped.items())
	]

	return SchedulerInstance(events=grouped_events, machines=DEFAULT_MACHINES)


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
				text=[event.designId],
				textposition="inside",
				name=location,
				legendgroup=f"location_{location}",
				showlegend=show_legend,
				customdata=[[
					int(job["groupId"]),
					str(event.designId),
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
				]],
				hovertemplate=(
					"Order ID: %{customdata[0]}<br>"
					"Design ID: %{customdata[1]}<br>"
					"Location: %{customdata[11]}<br>"
					"Machine ID: %{customdata[2]}<br>"
					"Start: %{customdata[6]} (%{customdata[3]} min)<br>"
					"End: %{customdata[7]} (%{customdata[4]} min)<br>"
					"Duration: %{customdata[5]} min<br>"
					"Est Time: %{customdata[8]} min<br>"
					"Requested Ship: %{customdata[10]}"
					"<extra></extra>"
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
) -> dict:
	instance = _load_instance_from_excel(excel_path, excel_filter)
	config = SchedulerSolverConfig(
		time_limit_seconds=max(1, int(time_limit_seconds)),
		log_search_progress=False,
		optimization_tolerance=max(0.0, float(optimization_tolerance)),
		num_search_workers=max(1, int(worker_count)),
		enumerate_all_solutions=enumerate_all_solutions,
	)

	solver = SchedulerSolver(instance, config)

	if add_force_before_ship_date:
		solver._add_constraint_force_before_ship_date()
	if add_force_before_ship_date_ignore_lates:
		solver._add_constraint_force_before_ship_date_ignore_lates()
	if add_sequence_constraint:
		solver._add_constraint_sequence_subevents()
	if add_pad_between_events_constraint:
		solver._add_constraint_pad_between_events()

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
	schedule_rows = sorted(
		solution.schedule,
		key=lambda row: (row["assignedMachineId"], row["scheduledStartDate"], row["groupId"]),
	)

	figure = _build_schedule_plotly_figure(schedule_rows, instance, solution.objective_value)

	return {
		"status": solution.status,
		"objective_value": solution.objective_value,
		"rows": schedule_rows,
		"row_count": len(schedule_rows),
		"equally_optimal_count": len(solution.equally_optimal_schedules),
		"graph_figure": figure.to_dict(),
	}

app.native.window_args['resizable'] = True

@ui.page('/')
async def main_page():
	await ui.context.client.connected()
	if not app.storage.tab.get('scheduler_config'):
		app.storage.tab['scheduler_config'] = {
			"excel_path": DEFAULT_EXCEL_PATH,
			"excel_filter": "",
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
	if not app.storage.tab.get('scheduler_state'):
		app.storage.tab['scheduler_state'] = {
			"active_tab": "Schedule Table",
			"schedule_result": None,
		}
	config = app.storage.tab['scheduler_config']
	state = app.storage.tab['scheduler_state']

	ui.colors(primary="#005f73", secondary="#0a9396", accent="#ee9b00")

	ui.label("CP-SAT Scheduler Console").classes("text-2xl font-bold")
	ui.label("Run your scheduler, toggle constraints, choose an objective, and view the resulting matplotlib graph.").classes(
		"text-gray-700"
	)

	with ui.card().classes("w-full"):
		with ui.row().classes("w-full items-end gap-4"):
			excel_path_input = ui.input("Excel path", value=config["excel_path"]).classes("w-[42rem]").bind_value(config, "excel_path")
			excel_filter_input = ui.input("Excel filter (pandas query syntax)", value=config["excel_filter"]).classes("w-[24rem]").bind_value(config, "excel_filter")
			time_limit_input = ui.number("Time limit (seconds)", value=config["time_limit_seconds"], min=1, precision=0).bind_value(config, "time_limit_seconds")
			worker_count_input = ui.number("Search workers", value=config["worker_count"], min=1, precision=0).bind_value(config, "worker_count")
			tolerance_input = ui.number("Optimization tolerance", value=config["optimization_tolerance"], min=0, step=0.001).bind_value(config, "optimization_tolerance")

		with ui.row().classes("w-full items-end gap-4"):
			objective_select = ui.select(
				{
					"makespan": "Makespan",
					"multi_makespan": "Multi-layer Makespan",
					"makespan_with_tardiness": "Makespan + Tardiness Penalty",
					"balanced": "Balanced Load",
				},
				value="multi_makespan",
				label="Objective",
			).bind_value(config, "objective")
			multi_makespan_checks_input = ui.number("Multi-makespan checks", value=4, min=1, precision=0).bind_value(config, "multi_makespan_checks")
			enumerate_switch = ui.switch("Enumerate equally optimal solutions", value=False).bind_value(config, "enumerate_all_solutions")

		with ui.row().classes("w-full gap-8"):
			with ui.column().classes("gap-1"):
				ui.label("Optional constraints").classes("text-base font-semibold")
				force_ship_date_switch = ui.switch("Force all events before ship date", value=False).bind_value(config, "add_force_before_ship_date")
				force_ship_date_ignore_lates_switch = ui.switch("Force before ship date (ignore already-late)", value=False).bind_value(config, "add_force_before_ship_date_ignore_lates")
				sequence_switch = ui.switch("Force sequence of subevents by location", value=True).bind_value(config, "add_sequence_constraint")
				pad_switch = ui.switch("Add 1-minute pad between machine events", value=False).bind_value(config, "add_pad_between_events_constraint")

	status_label = ui.label("Ready.").classes("text-sm")
	summary_label = ui.label("").classes("text-sm")

	run_button = ui.button("Run Scheduler")

	with ui.tabs().classes("w-full") as tabs:
		table_tab = ui.tab("Schedule Table")
		graph_tab = ui.tab("Schedule Graph")

	with ui.tab_panels(tabs= tabs, value= state["active_tab"], on_change= lambda e: state.update({"active_tab": e.value})).classes("w-full"):
		with ui.tab_panel(table_tab):
			result_table = ui.table(
				columns=[
					{"name": "groupId", "label": "Group ID", "field": "groupId", "sortable": True},
					{"name": "designId", "label": "Design ID", "field": "designId", "sortable": True},
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
	
	if state["schedule_result"]:
		result_table.rows = state["schedule_result"]["rows"]
		result_table.update()
		graph_plot.figure = go.Figure(state["schedule_result"]["graph_figure"])
		graph_plot.update()
		summary_label.text = (
				f"Status: {state['schedule_result']['status']} | Objective: {state['schedule_result']['objective_value']:.3f} | "
				f"Rows: {state['schedule_result']['row_count']} | Equally optimal alternates: {state['schedule_result']['equally_optimal_count']}"
			)


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
				config['excel_path'],
				str(config['excel_filter']),
				str(config['objective']),
				int(config['time_limit_seconds']),
				int(config['worker_count']),
				float(config['optimization_tolerance']),
				bool(config['enumerate_all_solutions']),
				bool(config['add_force_before_ship_date']),
				bool(config['add_force_before_ship_date_ignore_lates']),
				bool(config['add_sequence_constraint']),
				bool(config['add_pad_between_events_constraint']),
				int(config['multi_makespan_checks']),
			)

			rows = state["schedule_result"]["rows"]
			result_table.rows = rows
			result_table.update()
			graph_plot.figure = go.Figure(state["schedule_result"]["graph_figure"])
			graph_plot.update()
			selected_job_label.text = "Click a bar to inspect a scheduled job."

			summary_label.text = (
				f"Status: {state['schedule_result']['status']} | Objective: {state['schedule_result']['objective_value']:.3f} | "
				f"Rows: {state['schedule_result']['row_count']} | Equally optimal alternates: {state['schedule_result']['equally_optimal_count']}"
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
