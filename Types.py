from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, TypedDict


@dataclass
class WeekSchedule:
    startDate: date
    hours: int = 8
    monday: bool = True
    tuesday: bool = True
    wednesday: bool = True
    thursday: bool = True
    friday: bool = True
    saturday: bool = False
    sunday: bool = False
    
    @property
    def week(self) -> List[bool]:
        return [self.monday, self.tuesday, self.wednesday, self.thursday, self.friday, self.saturday, self.sunday]
    @property
    def daysScheduledCount(self) -> int:
        return sum(self.week)

    def __init__(self, startDate: date, hours: int = 8, monday: bool = True, tuesday: bool = True, wednesday: bool = True, thursday: bool = True, friday: bool = True, saturday: bool = False, sunday: bool = False):
        self.startDate = startDate
        self.hours = hours
        self.monday = monday
        self.tuesday = tuesday
        self.wednesday = wednesday
        self.thursday = thursday
        self.friday = friday
        self.saturday = saturday
        self.sunday = sunday

@dataclass
class Event:
    orderId: int
    designId: str
    runTime: int
    setupTime: int
    requestedShipDate: date
    # complexity: int [DEPRECATED] - now using total color/flash data
    colors: int
    flashes: int

    # [DEPRECATED] was used for calculating complexity tiers but now we have more granular color/flash data for better scheduling logic
    # @staticmethod
    # def complexityFromColorCount(colors: int) -> int:
    #     if colors <= 6:
    #         return 0
    #     elif colors <= 8:
    #         return 1
    #     elif colors <= 12:
    #         return 2
    #     else:
    #         return 3

    # [DEPRECATED] was used for calculating total estTime but now its split into runTime and setupTime for better granularity in scheduling logic 
    # @staticmethod
    # def estTimeFromQuantityAndColorsInMinutes(quantity: int, colors: int) -> int:
    #     return int(((quantity / 250) * 60) + (colors * 10))

@dataclass
class EventGroup:
    groupId: int
    designId: str
    estTime: int
    # complexity: int [DEPRECATED] - now using total color/flash data for better scheduling logic
    colors: int
    flashes: int
    requestedShipDate: date

@dataclass
class ProductionEvent:
    # Static attributes retrieved from the database
    orderId: int
    orderDesignName: str
    designId: str
    printLocation: str
    colorsTotal: int
    flashesTotal: int
    quantity: int
    priority: int
    requestedShipDate: date
    productionDoneDate: Optional[date] = None

    # Calculated / derived attributes
    scheduleValue: Optional[int] = None # Experimental value for scheduling priority
    scheduledStartDate: Optional[date] = None
    assignedMachineId: Optional[int] = None
    estTime: Optional[int] = None # Estimated time in minutes
    @property
    def headsTotal(self) -> int:
        return self.colorsTotal + 2 * self.flashesTotal

    def __init__(self, orderId: int, orderDesignName: str, designId: str, printLocation: str, colorsTotal: int, flashesTotal: int, quantity: int, priority: int, requestedShipDate: date, productionDoneDate: Optional[date] = None):
        self.orderId = orderId
        self.orderDesignName = orderDesignName
        self.designId = designId
        self.printLocation = printLocation
        self.colorsTotal = colorsTotal
        self.flashesTotal = flashesTotal
        self.quantity = quantity
        self.priority = priority
        self.requestedShipDate = requestedShipDate
        self.productionDoneDate = productionDoneDate
    def __str__(self):
        return f"ProductionEvent(orderId={self.orderId}, orderDesignName='{self.orderDesignName}', designId={self.designId}, printLocation='{self.printLocation}', colorsTotal={self.colorsTotal}, flashesTotal={self.flashesTotal}, quantity={self.quantity}, priority={self.priority}, requestedShipDate={self.requestedShipDate}, productionDoneDate={self.productionDoneDate}, scheduleValue={self.scheduleValue}, scheduledStartDate={self.scheduledStartDate}, assignedMachineId={self.assignedMachineId}, estTime={self.estTime})"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "orderId": self.orderId,
            "orderDesignName": self.orderDesignName,
            "designId": self.designId,
            "printLocation": self.printLocation,
            "colorsTotal": self.colorsTotal,
            "flashesTotal": self.flashesTotal,
            "quantity": self.quantity,
            "priority": self.priority,
            "requestedShipDate": self.requestedShipDate.isoformat(),
            "scheduleValue": self.scheduleValue,
            "scheduledStartDate": self.scheduledStartDate.isoformat() if self.scheduledStartDate else None,
            "assignedMachineId": self.assignedMachineId,
            "estTime": self.estTime,
            "productionDoneDate": self.productionDoneDate.isoformat() if self.productionDoneDate else None
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'ProductionEvent': #type: ignore  -  unused currently
        event = ProductionEvent(
            orderId=data["orderId"],
            orderDesignName=data["orderDesignName"],
            designId=data["designId"],
            printLocation=data["printLocation"],
            colorsTotal=data["colorsTotal"],
            flashesTotal=data["flashesTotal"],
            quantity=data["quantity"],
            priority=data["priority"],
            requestedShipDate=date.fromisoformat(data["requestedShipDate"]),
            productionDoneDate=date.fromisoformat(data["productionDoneDate"]) if data.get("productionDoneDate") else None
        )
        if data.get("scheduledStartDate"):
            event.scheduledStartDate = date.fromisoformat(data["scheduledStartDate"])
        event.assignedMachineId = data.get("assignedMachineId")
        event.scheduleValue = data.get("scheduleValue")
        event.estTime = data.get("estTime")
        if data.get("productionDoneDate"):
            event.productionDoneDate = date.fromisoformat(data["productionDoneDate"])
   
@dataclass
class Machine():
    # Static attributes retrieved from the database
    machineId: int
    machineName: str
    heads: int # semi-constant, but effectively static

    # Calculated / derived attributes
    printRate: Optional[int] = None # Prints per hour
    schedule: Optional[WeekSchedule] = None 

    def __init__(self, machineId: int, machineName: str, heads: int, printRate: Optional[int] = None, schedule: Optional[WeekSchedule] = None):
        self.machineId = machineId
        self.machineName = machineName
        self.heads = heads
        self.printRate = printRate
        self.schedule = schedule

    def to_dict(self) -> Dict[str, Any]:
        return {
            "machineId": self.machineId,
            "machineName": self.machineName,
            "heads": self.heads,
            "printRate": self.printRate,
            "schedule": {
                "startDate": self.schedule.startDate.isoformat() if self.schedule else None,
                "hours": self.schedule.hours if self.schedule else None,
                "monday": self.schedule.monday if self.schedule else None,
                "tuesday": self.schedule.tuesday if self.schedule else None,
                "wednesday": self.schedule.wednesday if self.schedule else None,
                "thursday": self.schedule.thursday if self.schedule else None,
                "friday": self.schedule.friday if self.schedule else None,
                "saturday": self.schedule.saturday if self.schedule else None,
                "sunday": self.schedule.sunday if self.schedule else None
                }
            }
    
    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'Machine':
        schedule_data = data.get("schedule", {})
        schedule = WeekSchedule(
            startDate=date.fromisoformat(schedule_data["startDate"]) if schedule_data.get("startDate") else date.today(),
            hours=schedule_data.get("hours", 8),
            monday=schedule_data.get("monday", True),
            tuesday=schedule_data.get("tuesday", True),
            wednesday=schedule_data.get("wednesday", True),
            thursday=schedule_data.get("thursday", True),
            friday=schedule_data.get("friday", True),
            saturday=schedule_data.get("saturday", False),
            sunday=schedule_data.get("sunday", False)
         ) if schedule_data else None
        machine = Machine(
            machineId=data["machineId"],
            machineName=data["machineName"],
            heads=data["heads"],
            printRate=data.get("printRate"),
            schedule=schedule
         )
        return machine

class TableProfile(TypedDict):
    """
    A TypedDict representing the profile information of a database table.

    Attributes:
        name (str): The name of the database table.
        rowCount (int): The total number of rows in the table.
        columns (List[Dict[str, str]]): A list of dictionaries containing 
            column information, where each dictionary represents a column with 
            string keys and string values (e.g., column name, data type, etc.).
        dateRefreshed (str): The date and time when the table profile was last 
            refreshed, typically in ISO format or other string representation.
    """
    name: str
    rowCount: int
    columns: List[Dict[str, str]]
    dateRefreshed: str

def DEBUG_loadFromJsonFile() -> List[ProductionEvent]:
    import json
    with open('unscheduled_events2.json', 'r') as f:
        data = json.load(f)
    return [ProductionEvent.from_dict(item) for item in data]
