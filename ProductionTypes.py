from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from datetime import date

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
class ProductionEvent:
    # Static attributes retrieved from the database
    orderId: int
    orderDesignName: str
    printLocation: str
    colorsTotal: int
    flashesTotal: int
    quantity: int
    priority: int
    requestedShipDate: date

    # Calculated / derived attributes
    scheduleValue: Optional[int] = None # Experimental value for scheduling priority
    scheduledStartDate: Optional[date] = None
    assignedMachine: Optional[int] = None
    @property
    def headsTotal(self) -> int:
        return self.colorsTotal + 2 * self.flashesTotal

    def __init__(self, orderId: int, orderDesignName: str, printLocation: str, colorsTotal: int, flashesTotal: int, quantity: int, priority: int, requestedShipDate: date):
        self.orderId = orderId
        self.orderDesignName = orderDesignName
        self.printLocation = printLocation
        self.colorsTotal = colorsTotal
        self.flashesTotal = flashesTotal
        self.quantity = quantity
        self.priority = priority
        self.requestedShipDate = requestedShipDate
    
    def __str__(self):
        return f"ProductionEvent(orderId={self.orderId}, orderDesignName='{self.orderDesignName}', printLocation='{self.printLocation}', colorsTotal={self.colorsTotal}, flashesTotal={self.flashesTotal}, quantity={self.quantity}, priority={self.priority}, requestedShipDate={self.requestedShipDate}, scheduleValue={self.scheduleValue}, scheduledStartDate={self.scheduledStartDate}, assignedMachine={self.assignedMachine})"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "orderId": self.orderId,
            "orderDesignName": self.orderDesignName,
            "printLocation": self.printLocation,
            "colorsTotal": self.colorsTotal,
            "flashesTotal": self.flashesTotal,
            "quantity": self.quantity,
            "priority": self.priority,
            "requestedShipDate": self.requestedShipDate.isoformat(),
            "scheduleValue": self.scheduleValue,
            "scheduledStartDate": self.scheduledStartDate.isoformat() if self.scheduledStartDate else None,
            "assignedMachine": self.assignedMachine
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'ProductionEvent':
        event = ProductionEvent(
            orderId=data["orderId"],
            orderDesignName=data["orderDesignName"],
            printLocation=data["printLocation"],
            colorsTotal=data["colorsTotal"],
            flashesTotal=data["flashesTotal"],
            quantity=data["quantity"],
            priority=data["priority"],
            requestedShipDate=date.fromisoformat(data["requestedShipDate"])
        )
        if data.get("scheduledStartDate"):
            event.scheduledStartDate = date.fromisoformat(data["scheduledStartDate"])
        event.assignedMachine = data.get("assignedMachine")
        event.scheduleValue = data.get("scheduleValue")
        return event
   
def DEBUG_loadFromJsonFile() -> List[ProductionEvent]:
    import json
    with open('unscheduled_events2.json', 'r') as f:
        data = json.load(f)
    return [ProductionEvent.from_dict(item) for item in data]

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