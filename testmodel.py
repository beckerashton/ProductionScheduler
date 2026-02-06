# type: ignore
"""
Production Scheduling Model

This script uses historical production event data to schedule new production events.
It analyzes patterns in lead times, production complexity (colors, stitches), and
capacity constraints to generate optimized production schedules.
"""

from pyodbc import Connection, Row
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import pandas as pd
import numpy as np
import logging
import os
import json
import pickle
from pathlib import Path
from dotenv import load_dotenv
import tempfile

from DbUtils import getConnection, getCursor, qryToDataFrame

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

CON_STRING: str = os.getenv("DB_CONNECTION_STRING", "")
CACHE_FILE: Path = Path(os.getenv("CACHE_FILE_PATH", "scheduling_cache.pkl"))
CACHE_EXPIRY_DAYS: int = 7  # Refresh cache after this many days

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)


@dataclass
class ProductionEvent:
    """Represents a production event with scheduling information."""
    order_id: int
    design_loc_id: int
    order_design_name: str
    colors_total: int
    quantity: int
    location: str
    requested_ship_date: datetime
    flashes_total: int = 0
    priority: int = 5  # 1-10 scale, 1 is highest
    stitches_total: int = 0     # Unused in current code but kept for compatibility
    estimated_duration_hours: float = 0.0
    scheduled_start: Optional[datetime] = None
    scheduled_end: Optional[datetime] = None
    assigned_machine: Optional[str] = None
    
    def __str__(self) -> str:
        return (f"Order {self.order_id} - Location: {self.location}, "
                f"Colors: {self.colors_total}, Flashes: {self.flashes_total}, Qty: {self.quantity}, "
                f"Ship: {self.requested_ship_date.date() if self.requested_ship_date else 'N/A'}, "
                f"Design Name: {self.order_design_name}")


@dataclass
class MachineCapacity:
    """Represents machine capacity and availability."""
    machine_id: str
    machine_name: str
    max_colors: int
    rate_per_hour: float  # units per hour
    available_hours: Dict[datetime, float]  # date -> available hours that day
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for caching."""
        return {
            'machine_id': self.machine_id,
            'machine_name': self.machine_name,
            'max_colors': self.max_colors,
            'rate_per_hour': self.rate_per_hour
        }
    
    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'MachineCapacity':
        """Create from cached dictionary."""
        # Reinitialize available hours
        available_hours = {}
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        for i in range(30):
            date = today + timedelta(days=i)
            available_hours[date] = 8.0
        
        return MachineCapacity(
            machine_id=data['machine_id'],
            machine_name=data['machine_name'],
            max_colors=data['max_colors'],
            rate_per_hour=data['rate_per_hour'],
            available_hours=available_hours
        )


class ProductionScheduler:
    """Main scheduling class that learns from historical data and schedules new events."""
    
    def __init__(self, cnxn: Connection):
        self.cnxn = cnxn
        self.historical_data: Optional[pd.DataFrame] = None
        self.lead_time_model: Dict[str, Any] = {}
        self.machines: List[MachineCapacity] = []
        
    def load_historical_data(self, start_date: str = "01/01/2024", end_date: str = "12/31/2025", use_cache: bool = True) -> pd.DataFrame:
        """
        Load historical production event data for analysis.
        
        Args:
            start_date: Start date for historical data (MM/DD/YYYY)
            end_date: End date for historical data (MM/DD/YYYY)
            use_cache: If True, try to load from cache first
            
        Returns:
            DataFrame with historical production events
        """
        log.info("Loading historical production data...")
        
        # Try cache first
        if use_cache:
            cached_model = self._load_lead_time_model_from_cache()
            if cached_model:
                self.lead_time_model = cached_model
                log.info("Loaded lead time model from cache, skipping historical data query")
                self.historical_data = pd.DataFrame()  # Empty placeholder
                return self.historical_data
        
        query = f"""
            SELECT 
                eodl.ID_OrderDesLoc,
                eodl.id_OrderDesign,
                eodl.id_Order,
                eo.date_OrderPlaced,
                eo.date_OrderRequestedToShip,
                eo.date_OrderDropDead,
                eodl.date_Creation as date_EventCreation,
                eodl.ColorsTotal,
                eodl.Location,
                eodl.cn_QtyToProduce,
                eodl.cn_LocationCountOrder,
                eodl.cn_LocationNumberOrderDesign,
                eodl.FlashesTotal,
                eo.id_OrderType
            FROM 
                Events_OrderDesLoc eodl
            LEFT JOIN 
                Events_Order eo ON eodl.id_Order = eo.ID_Order
            WHERE
                eodl.date_Creation BETWEEN '{start_date}' AND '{end_date}'
                AND eodl.ColorsTotal IS NOT NULL
                AND eodl.ColorsTotal > 0
                AND eo.date_OrderRequestedToShip IS NOT NULL
            ORDER BY
                eodl.date_Creation DESC
        """
        
        self.historical_data = qryToDataFrame(cnxn=self.cnxn, query=query)
        
        # Convert date columns to datetime
        date_cols = ['date_OrderPlaced', 'date_OrderRequestedToShip', 
                     'date_OrderDropDead', 'date_EventCreation']
        for col in date_cols:
            if col in self.historical_data.columns:
                self.historical_data[col] = pd.to_datetime(
                    self.historical_data[col], errors='coerce'
                )
        
        # Calculate actual lead time (days between order placed and requested ship)
        self.historical_data['lead_time_days'] = (
            self.historical_data['date_OrderRequestedToShip'] - 
            self.historical_data['date_OrderPlaced']
        ).dt.days
        
        # Calculate processing time estimate based on complexity (ignoring stitches)
        self.historical_data['complexity_score'] = (
            self.historical_data['ColorsTotal'] * 2.0 + 
            self.historical_data['FlashesTotal'].fillna(0) * 0.5
        )
        
        log.info(f"Loaded {len(self.historical_data)} historical production events")
        return self.historical_data
    
    def analyze_lead_times(self) -> Dict[str, Any]:
        """
        Analyze historical lead time patterns based on complexity factors.
        
        Returns:
            Dictionary with lead time statistics and patterns
        """
        if self.historical_data is None or len(self.historical_data) == 0:
            log.warning("No historical data loaded")
            return {}
        
        log.info("Analyzing lead time patterns...")
        
        # Filter valid lead times
        valid_data = self.historical_data[
            (self.historical_data['lead_time_days'] > 0) & 
            (self.historical_data['lead_time_days'] < 365)
        ].copy()
        
        # Categorize by color complexity
        valid_data['color_category'] = pd.cut(
            valid_data['ColorsTotal'],
            bins=[0, 2, 4, 8, 100],
            labels=['1-2 colors', '3-4 colors', '5-8 colors', '9+ colors']
        )
        
        # Calculate statistics by category
        lead_time_stats = valid_data.groupby('color_category')['lead_time_days'].agg([
            'mean', 'median', 'std', 'min', 'max', 'count'
        ]).round(2)
        
        # Overall statistics
        overall_stats = {
            'mean_lead_time': valid_data['lead_time_days'].mean(),
            'median_lead_time': valid_data['lead_time_days'].median(),
            'std_lead_time': valid_data['lead_time_days'].std(),
            'by_color_category': lead_time_stats.to_dict('index')
        }
        
        self.lead_time_model = overall_stats
        
        # Save to cache
        self._save_lead_time_model_to_cache(overall_stats)
        
        log.info(f"Average lead time: {overall_stats['mean_lead_time']:.1f} days")
        log.info(f"Median lead time: {overall_stats['median_lead_time']:.1f} days")
        
        return overall_stats
    
    def estimate_production_time(self, colors: int, quantity: int, stitches: int = 0, 
                                 flashes: int = 0) -> float:
        """
        Estimate production time in hours based on complexity factors.
        Note: Stitches parameter kept for compatibility but not used in calculation.
        
        Args:
            colors: Number of colors in the design
            stitches: Total number of stitches (not used)
            quantity: Quantity to produce
            flashes: Number of flashes (optional)
            
        Returns:
            Estimated production time in hours
        """
        # Base setup time (hours) - increases with colors
        setup_time = 0.5 + (colors * 0.2)
        
        # Production time based primarily on quantity and colors
        # Base rate: ~120 units per hour for simple designs
        # Adjust rate down for more colors (more complex = slower)
        base_rate = 120.0 - (colors * 8)  # Slower for more colors
        base_rate = max(base_rate, 40.0)  # Minimum rate of 40 units/hour
        
        production_time = quantity / base_rate
        
        # Additional time for color changes during production
        color_change_time = (colors - 1) * 0.15 * (quantity / 100)
        
        # Additional time for flashes (more significant impact)
        flash_time = flashes * 0.25 * (quantity / 100)
        
        total_time = setup_time + production_time + color_change_time + flash_time
        
        # Add 20% buffer for typical inefficiencies
        total_time *= 1.20
        
        return round(total_time, 2)
    
    def predict_recommended_lead_time(self, colors: int, quantity: int, stitches: int = 0) -> int:
        """
        Predict recommended lead time in days for a new order.
        Note: Stitches parameter kept for compatibility but not used in calculation.
        
        Args:
            colors: Number of colors
            stitches: Total stitches (not used)
            quantity: Quantity to produce
            
        Returns:
            Recommended lead time in days
        """
        if not self.lead_time_model:
            # Default fallback
            base_days = 7
        else:
            # Use historical median as base
            base_days = self.lead_time_model.get('median_lead_time', 7)
        
        # Adjust based on complexity (colors and quantity only)
        complexity_multiplier = 1.0
        
        if colors > 8:
            complexity_multiplier += 0.35
        elif colors > 4:
            complexity_multiplier += 0.20
        
        if quantity > 500:
            complexity_multiplier += 0.25
        elif quantity > 200:
            complexity_multiplier += 0.15
        
        recommended_days = int(base_days * complexity_multiplier)
        
        # Ensure minimum lead time
        return max(recommended_days, 3)
    
    def load_machine_capacity(self, use_cache: bool = True) -> List[MachineCapacity]:
        """
        Load machine capacity information from the database or cache.
        
        Args:
            use_cache: If True, try to load from cache first
        
        Returns:
            List of MachineCapacity objects
        """
        log.info("Loading machine capacity data...")
        
        # Try to load from cache first
        if use_cache:
            cached_machines = self._load_machines_from_cache()
            if cached_machines:
                self.machines = cached_machines
                log.info(f"Loaded {len(self.machines)} machines from cache")
                return self.machines
        
        log.info("Querying machine data from database...")
        # Query database - filter to machines 101-107 only
        query = """
            SELECT 
                ID_Machine,
                MachineName,
                MaxNumberOfColors,
                cur_MachineRate,
                UnitsPerHour
            FROM 
                Events_Machine
            WHERE
                ID_Machine IN ('101', '102', '103', '104', '105', '106', '107')
        """
        
        try:
            machines_df = qryToDataFrame(cnxn=self.cnxn, query=query)
            
            log.info(f"Query returned {len(machines_df)} machine records")
            
            if len(machines_df) == 0:
                log.warning("No machines found matching IDs 101-107. Check if these machines exist in database.")
                log.warning("Creating dummy machines for testing...")
                self._create_dummy_machines()
                return self.machines
            
            # Create MachineCapacity objects
            for _, row in machines_df.iterrows():
                # Initialize with 8 hours available per day for next 30 days
                available_hours = {}
                today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                for i in range(30):
                    date = today + timedelta(days=i)
                    available_hours[date] = 8.0  # 8 hours per day available
                
                machine = MachineCapacity(
                    machine_id=str(row['ID_Machine']),
                    machine_name=row['MachineName'],
                    max_colors=int(row['MaxNumberOfColors']),
                    rate_per_hour=2*float(row.get('UnitsPerHour') or row.get('cur_MachineRate') or 100),
                    available_hours=available_hours
                )
                self.machines.append(machine)
                log.debug(f"  Loaded Machine {machine.machine_id}: {machine.machine_name} ({machine.max_colors} colors)")
            
            log.info(f"Successfully loaded {len(self.machines)} machines from database")
            
            # Only save to cache if we actually loaded machines
            if len(self.machines) > 0:
                self._save_machines_to_cache(self.machines)
            else:
                log.warning("Not caching empty machine list")
            
        except Exception as e:
            log.warning(f"Could not load machine data: {e}")
            # Create dummy machines for testing
            self._create_dummy_machines()
        
        return self.machines
    
    def _create_dummy_machines(self):
        """Create dummy machines for testing when database access fails."""
        available_hours = {}
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        for i in range(30):
            date = today + timedelta(days=i)
            available_hours[date] = 8.0
        
        # Create machines matching IDs 101-107
        for i in range(7):
            machine = MachineCapacity(
                machine_id=f"{101+i}",
                machine_name=f"Machine {101+i}",
                max_colors=6 + (i % 3) * 2,  # Varying capacities: 6, 8, 10 colors
                rate_per_hour=100.0 + (i * 20),
                available_hours=available_hours.copy()
            )
            self.machines.append(machine)
    
    def _load_cache(self) -> Optional[Dict[str, Any]]:
        """Load all cached data."""
        if not CACHE_FILE.exists():
            return None
        
        try:
            with open(CACHE_FILE, 'rb') as f:
                cache_data = pickle.load(f)
            
            # Check if cache is expired
            cache_date = cache_data.get('timestamp', datetime.min)
            if isinstance(cache_date, str):
                cache_date = datetime.fromisoformat(cache_date)
            
            age_days = (datetime.now() - cache_date).days
            if age_days > CACHE_EXPIRY_DAYS:
                log.info(f"Cache is {age_days} days old, expired")
                return None
            
            log.info(f"Cache loaded (age: {age_days} days)")
            return cache_data
        except Exception as e:
            log.warning(f"Could not load cache: {e}")
            return None
    
    def _save_cache(self, cache_data: Dict[str, Any]) -> None:
        """Save data to cache."""
        try:
            cache_data['timestamp'] = datetime.now().isoformat()
            with open(CACHE_FILE, 'wb') as f:
                pickle.dump(cache_data, f)
            log.info(f"Cache saved to {CACHE_FILE}")
        except Exception as e:
            log.warning(f"Could not save cache: {e}")
    
    def _load_machines_from_cache(self) -> Optional[List[MachineCapacity]]:
        """Load machines from cache."""
        cache_data = self._load_cache()
        if not cache_data or 'machines' not in cache_data:
            return None
        
        try:
            machines = [MachineCapacity.from_dict(m) for m in cache_data['machines']]
            return machines
        except Exception as e:
            log.warning(f"Could not deserialize machines from cache: {e}")
            return None
    
    def _save_machines_to_cache(self, machines: List[MachineCapacity]) -> None:
        """Save machines to cache."""
        if not machines:
            log.warning("Attempted to save empty machines list to cache - skipping")
            return
        
        cache_data = self._load_cache() or {}
        cache_data['machines'] = [m.to_dict() for m in machines]
        self._save_cache(cache_data)
        log.info(f"Cached {len(machines)} machines")
    
    def _load_lead_time_model_from_cache(self) -> Optional[Dict[str, Any]]:
        """Load lead time model from cache."""
        cache_data = self._load_cache()
        if not cache_data or 'lead_time_model' not in cache_data:
            return None
        return cache_data['lead_time_model']
    
    def _save_lead_time_model_to_cache(self, model: Dict[str, Any]) -> None:
        """Save lead time model to cache."""
        cache_data = self._load_cache() or {}
        cache_data['lead_time_model'] = model
        self._save_cache(cache_data)
    
    def find_best_machine(self, event: ProductionEvent) -> Optional[MachineCapacity]:
        """
        Find the best machine for a production event based on capacity and availability.
        Flashes count as 2 spots on the machine.
        
        Args:
            event: ProductionEvent to schedule
            
        Returns:
            Best matching MachineCapacity or None
        """
        # Calculate total spots needed: colors + (flashes * 2)
        total_spots_needed = event.colors_total + (event.flashes_total * 2)
        
        # Filter machines that can handle the total spot count
        capable_machines = [m for m in self.machines if m.max_colors >= total_spots_needed]
        
        if not capable_machines:
            log.warning(f"No machine found with capacity for {event.colors_total} colors + {event.flashes_total} flashes (total {total_spots_needed} spots)")
            return None
        
        # Calculate total available hours for each machine
        machine_scores = []
        for machine in capable_machines:
            total_available = sum(machine.available_hours.values())
            
            # Scoring factors:
            # 1. Prefer machines with more available time (higher availability = lower score for sorting)
            # 2. Prefer machines with capacity close to requirement (avoid overkill)
            # 3. Consider production rate as tiebreaker
            
            capacity_match_penalty = machine.max_colors - total_spots_needed
            availability_score = -total_available  # Negative so more available = better (lower score)
            rate_bonus = -machine.rate_per_hour  # Negative so higher rate = better (lower score)
            
            # Weighted score: prioritize availability, then capacity match, then rate
            score = (availability_score * 0.5, capacity_match_penalty, rate_bonus * 0.1)
            
            machine_scores.append((score, machine))
        
        # Sort by score and return best machine
        machine_scores.sort(key=lambda x: x[0])
        
        selected_machine = machine_scores[0][1]
        log.debug(f"Selected {selected_machine.machine_name} for {event.colors_total} colors + {event.flashes_total} flashes ({total_spots_needed} total spots)")
        
        return selected_machine
    
    def schedule_event(self, event: ProductionEvent, 
                      start_date: Optional[datetime] = None) -> ProductionEvent:
        """
        Schedule a single production event.
        
        Args:
            event: ProductionEvent to schedule
            start_date: Preferred start date (defaults to today)
            
        Returns:
            Updated ProductionEvent with scheduling information
        """
        if start_date is None:
            start_date = datetime.now().replace(hour=8, minute=0, second=0, microsecond=0)
        
        # Estimate production time
        event.estimated_duration_hours = self.estimate_production_time(
            event.colors_total,
            quantity=event.quantity,
            flashes=event.flashes_total
        )
        
        # Find best machine
        machine = self.find_best_machine(event)
        if machine:
            event.assigned_machine = machine.machine_name
            
            # Find available slot
            current_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            hours_needed = event.estimated_duration_hours
            
            while hours_needed > 0:
                # Auto-extend availability with 8 hours for new dates
                if current_date not in machine.available_hours:
                    machine.available_hours[current_date] = 8.0
                
                available = machine.available_hours[current_date]
                if available > 0:
                    if event.scheduled_start is None:
                        event.scheduled_start = current_date.replace(hour=8)
                    
                    hours_to_use = min(hours_needed, available)
                    machine.available_hours[current_date] -= hours_to_use
                    hours_needed -= hours_to_use
                
                if hours_needed > 0:
                    current_date += timedelta(days=1)
            
            event.scheduled_end = current_date.replace(hour=8) + timedelta(
                hours=event.estimated_duration_hours % 8
            )
        
        return event
    
    def schedule_event_new(self, event: ProductionEvent) -> ProductionEvent:
        # my custom verion of scheduling and event cause i dont 100% like the og one
        # calc and assign prod time
        

    def schedule_multiple_events(self, events: List[ProductionEvent], 
                                 prioritize_by_date: bool = True) -> List[ProductionEvent]:
        """
        Schedule multiple production events with optimization.
        
        Args:
            events: List of ProductionEvents to schedule
            prioritize_by_date: If True, prioritize by requested ship date
            
        Returns:
            List of scheduled ProductionEvents
        """
        log.info(f"Scheduling {len(events)} production events...")
        
        # Sort events by priority
        if prioritize_by_date:
            events.sort(key=lambda e: (e.priority, e.requested_ship_date or datetime.max))
        else:
            events.sort(key=lambda e: e.priority)
        
        scheduled_events = []
        for event in events:
            scheduled_event = self.schedule_event(event)
            scheduled_events.append(scheduled_event)
        
        log.info("Scheduling complete")
        return scheduled_events
    
    def get_schedule_summary(self, events: List[ProductionEvent]) -> pd.DataFrame:
        """
        Generate a summary DataFrame of scheduled events.
        
        Args:
            events: List of scheduled ProductionEvents
            
        Returns:
            DataFrame with schedule summary
        """
        data = []
        for event in events:
            data.append({
                'Order ID': event.order_id,
                'Location': event.location,
                'Colors': event.colors_total,
                'Flashes': event.flashes_total,
                'Quantity': event.quantity,
                'Est. Hours': event.estimated_duration_hours,
                'Machine': event.assigned_machine or 'N/A',
                'Start Date': event.scheduled_start.strftime('%Y-%m-%d %H:%M') if event.scheduled_start else 'N/A',
                'End Date': event.scheduled_end.strftime('%Y-%m-%d %H:%M') if event.scheduled_end else 'N/A',
                'Requested Ship': event.requested_ship_date.strftime('%Y-%m-%d') if event.requested_ship_date else 'N/A',
                'Priority': event.priority
            })
        
        return pd.DataFrame(data)
    
    def generate_schedule_report(self, events: List[ProductionEvent]) -> None:
        """
        Generate and display a comprehensive schedule report.
        
        Args:
            events: List of scheduled ProductionEvents
        """
        print("\n" + "="*100)
        print("PRODUCTION SCHEDULE REPORT")
        print("="*100 + "\n")
        
        summary_df = self.get_schedule_summary(events)
        
        # Print with column headers explicitly shown
        if len(summary_df) > 0:
            print(summary_df.to_string(index=False, justify='left'))
        else:
            print("No events to display")
        
        print("\n" + "-"*100)
        print("SUMMARY STATISTICS")
        print("-"*100)
        
        total_hours = summary_df['Est. Hours'].sum()
        avg_hours = summary_df['Est. Hours'].mean()
        
        print(f"Total Production Events: {len(events)}")
        print(f"Total Estimated Hours: {total_hours:.2f}")
        print(f"Average Hours per Event: {avg_hours:.2f}")
        
        # Machine utilization
        machine_counts = summary_df['Machine'].value_counts()
        print(f"\nMachine Assignments:")
        for machine, count in machine_counts.items():
            print(f"  {machine}: {count} events")
        
        # Color complexity distribution
        print(f"\nColor Complexity Distribution:")
        color_dist = summary_df['Colors'].value_counts().sort_index()
        for colors, count in color_dist.items():
            print(f"  {colors} colors: {count} events")
        
        print("\n" + "="*100 + "\n")


def fetch_unscheduled_orders(cnxn: Connection, days_ahead: int = 30, days_behind: int = 30) -> List[ProductionEvent]:
    """
    Fetch orders that need scheduling from the database.
    
    Args:
        cnxn: Database connection
        days_ahead: Look ahead this many days for orders
        days_behind: Look behind this many days for orders
    Returns:
        List of ProductionEvents that need scheduling
    """
    log.info("Fetching unscheduled orders...")
    
    today = datetime.now()
    future_date = today + timedelta(days=days_ahead)
    past_date = today - timedelta(days=days_behind)
    
    today_str = today.strftime("%m/%d/%Y")
    future_str = future_date.strftime("%m/%d/%Y")
    past_str = past_date.strftime("%m/%d/%Y")
    
    query = f"""
                SELECT
                    eodl.id_Order,
                    eod.ct_DesignName,
                    eodl.Location,
                    eodl.ColorsTotal,
                    eodl.FlashesTotal,
                    eodl.cn_QtyToProduce,
                    eo.date_OrderRequestedToShip,
                    eodl.ID_OrderDesLoc
                FROM 
                    Events_OrderDesLoc eodl
                INNER JOIN 
                    Events_Order eo ON eodl.id_Order = eo.ID_Order
                INNER JOIN
                    Events_OrderDes eod ON eodl.id_Order = eod.id_Order AND eod.id_DesignType = 1
                WHERE
                    eodl.date_Creation >= '01/01/2025'
                    AND eo.date_OrderRequestedToShip >= '{past_str}'
                    AND eo.date_OrderRequestedToShip <= '{future_str}'
                    AND eodl.ColorsTotal > 0
                    AND eodl.cn_QtyToProduce > 0
                    AND eo.id_OrderType = 11
                    AND eo.cn_sts_HoldOrder = 0
                    AND eo.sts_ArtDone = 1
                    AND eo.sts_Purchased = 1
                    AND eo.sts_Received = 1
                    AND eo.id_SalesStatus IN (0, 1)
                ORDER BY
                    eo.date_OrderRequestedToShip ASC
            """
    
    try:
        df = qryToDataFrame(cnxn=cnxn, query=query)
        df['date_OrderRequestedToShip'] = pd.to_datetime(df['date_OrderRequestedToShip'], errors='coerce')
        
        # Fill NaN values with defaults before iterating
        df['FlashesTotal'] = df['FlashesTotal'].fillna(0)
        # df['StitchesTotal'] = df['StitchesTotal'].fillna(10000)
        df['Location'] = df['Location'].fillna('Unknown')
        
        events = []
        for _, row in df.iterrows():
            event = ProductionEvent(
                order_id=int(row['id_Order']),
                order_design_name=str(row['ct_DesignName']),
                design_loc_id=int(row['ID_OrderDesLoc']),
                colors_total=int(row['ColorsTotal']),
                # stitches_total=int(row['StitchesTotal']),
                quantity=int(row['cn_QtyToProduce']),
                location=str(row['Location']),
                requested_ship_date=row['date_OrderRequestedToShip'],
                flashes_total=int(row['FlashesTotal']),
                priority=5  # Default priority
            )
            events.append(event)
        
        log.info(f"Found {len(events)} unscheduled orders")
        return events
        
    except Exception as e:
        log.error(f"Error fetching unscheduled orders: {e}")
        return []


def check_available_machines(cnxn: Connection) -> None:
    """Diagnostic function to check what machines exist in the database."""
    query = """
        SELECT 
            ID_Machine,
            MachineName,
            MaxNumberOfColors,
            sts_Omit
        FROM 
            Events_Machine
        ORDER BY
            ID_Machine
    """
    
    try:
        df = qryToDataFrame(cnxn=cnxn, query=query)
        print("\n" + "="*100)
        print("AVAILABLE MACHINES IN DATABASE")
        print("="*100 + "\n")
        
        if len(df) > 0:
            print(df.to_string(index=False, justify='left'))
        else:
            print("No machines found in database")
        
        print(f"\nTotal: {len(df)} machines")
        print("\nMachines with IDs 101-107 and not omitted:")
        filtered = df[(df['ID_Machine'].isin(['101', '102', '103', '104', '105', '106', '107'])) & 
                     (df['sts_Omit'] == 0)]
        
        if len(filtered) > 0:
            print(filtered.to_string(index=False, justify='left'))
        else:
            print("No machines found matching criteria")
        
        print(f"Count: {len(filtered)}")
        print("="*100 + "\n")
    except Exception as e:
        log.error(f"Error checking machines: {e}")


def create_sample_events() -> List[ProductionEvent]:
    """Create sample production events for testing."""
    today = datetime.now()
    
    events = [
        ProductionEvent(
            order_id=1001,
            design_loc_id=5001,
            colors_total=4,
            order_design_name="Sample Design A",
            # stitches_total=15000,
            quantity=100,
            location="Left Chest",
            requested_ship_date=today + timedelta(days=7),
            priority=3
        ),
        ProductionEvent(
            order_id=1002,
            design_loc_id=5002,
            colors_total=8,
            order_design_name="Sample Design B",
            # stitches_total=35000,
            quantity=250,
            location="Full Back",
            requested_ship_date=today + timedelta(days=10),
            priority=2
        ),
        ProductionEvent(
            order_id=1003,
            design_loc_id=5003,
            colors_total=2,
            order_design_name="Sample Design C",
            # stitches_total=8000,
            quantity=50,
            location="Left Chest",
            requested_ship_date=today + timedelta(days=5),
            priority=1
        ),
        ProductionEvent(
            order_id=1004,
            design_loc_id=5004,
            colors_total=6,
            order_design_name="Sample Design D",
            # stitches_total=22000,
            quantity=150,
            location="Left Sleeve",
            requested_ship_date=today + timedelta(days=14),
            priority=4
        ),
        ProductionEvent(
            order_id=1005,
            design_loc_id=5005,
            colors_total=10,
            order_design_name="Sample Design E",
            # stitches_total=45000,
            quantity=300,
            location="Full Front",
            requested_ship_date=today + timedelta(days=21),
            priority=5
        ),
    ]
    
    return events


def main(force_refresh: bool = False):
    """Main execution function.
    Args:
        force_refresh: If True, ignore cache and refresh all data from database
    """
    try:
        if force_refresh:
            log.info("Force refresh enabled - ignoring cache")
        
        # Connect to database
        with getConnection(connectionString=CON_STRING.replace("?", "Data_Events")) as cnxn:
            # Initialize scheduler
            scheduler = ProductionScheduler(cnxn)
            
            # Load and analyze historical data
            print("\n" + "="*100)
            print("LOADING HISTORICAL DATA")
            print("="*100 + "\n")
            
            scheduler.load_historical_data(start_date="01/01/2024", end_date="12/31/2025", use_cache=not force_refresh)
            
            # Analyze lead times
            # print("\n" + "="*100)
            # print("ANALYZING LEAD TIME PATTERNS")
            # print("="*100 + "\n")
            
            # Only analyze if we don't have cached model
            if not scheduler.lead_time_model:
                lead_time_stats = scheduler.analyze_lead_times()
            else:
                lead_time_stats = scheduler.lead_time_model
            
            # if lead_time_stats and 'by_color_category' in lead_time_stats:
            #     print("\nLead Time Statistics by Color Complexity:")
            #     for category, stats in lead_time_stats['by_color_category'].items():
            #         print(f"\n{category}:")
            #         print(f"  Mean: {stats.get('mean', 0):.1f} days")
            #         print(f"  Median: {stats.get('median', 0):.1f} days")
            #         print(f"  Std Dev: {stats.get('std', 0):.1f} days")
            #         print(f"  Sample Size: {stats.get('count', 0)}")
            
            # Load machine capacity
            scheduler.load_machine_capacity(use_cache=not force_refresh)
            
            # Fetch unscheduled orders or use sample data
            print("\n" + "="*100)
            print("FETCHING ORDERS TO SCHEDULE ---- DISABLED FOR TESTING")
            print("="*100 + "\n")
            
            events_to_schedule = fetch_unscheduled_orders(cnxn, days_ahead=30)
            # events_to_schedule = None

            if not events_to_schedule:
                log.info("No unscheduled orders found in database, using sample data")
                events_to_schedule = create_sample_events()
            
            print(f"Orders to schedule: {len(events_to_schedule)}")
            
            # Schedule the events
            print("\n" + "="*100)
            print("SCHEDULING PRODUCTION EVENTS")
            print("="*100 + "\n")
            
            scheduled_events = scheduler.schedule_multiple_events(events_to_schedule)
            
            # Generate report
            scheduler.generate_schedule_report(scheduled_events)
            
            # open schedule in temporary excel file
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmpfile:
                schedule_df = scheduler.get_schedule_summary(scheduled_events)
                schedule_df.to_excel(tmpfile.name, index=False)
                log.info(f"Schedule exported to Excel file: {tmpfile.name}")
            os.startfile(tmpfile.name)  # Uncomment this line to open the file automatically on Windows

            # Demo: Predict lead time for a new order
            # print("\n" + "="*100)
            # print("LEAD TIME PREDICTION EXAMPLE")
            # print("="*100 + "\n")
            
            # test_colors = [2, 4, 6, 10]
            # test_quantities = [50, 100, 250, 500]
            
            # print("Recommended lead times for new orders (stitches not considered):\n")
            # for colors in test_colors:
            #     for qty in test_quantities:
            #         lead_time = scheduler.predict_recommended_lead_time(colors, quantity=qty)
            #         prod_time = scheduler.estimate_production_time(colors, quantity=qty)
            #         print(f"Colors: {colors:2d}, Qty: {qty:3d} "
            #               f"-> Lead Time: {lead_time:2d} days, Est. Prod Time: {prod_time:.2f} hrs")
            
            # print("\n" + "="*100 + "\n")
            
    except Exception as e:
        log.error(f"Error in main execution: {e}", exc_info=True)
        raise

def test() -> None:
    try:
        with getConnection(connectionString=CON_STRING.replace("?", "Data_Events")) as cnxn:
            scheduler = ProductionScheduler(cnxn)

            
            scheduler.schedule_event(testEvent1)
    except Exception as e:
        log.error(f"Error in test execution: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    import sys
    
    if not os.environ.get("DB_CONNECTION_STRING"):
        raise EnvironmentError("DB_CONNECTION_STRING not found in environment variables.")
    
    # Check for --check-machines flag
    if "--check-machines" in sys.argv:
        with getConnection(connectionString=CON_STRING.replace("?", "Data_Events")) as cnxn:
            check_available_machines(cnxn)
        exit(0)

    # Check for --force-refresh flag
    force_refresh = "--force-refresh" in sys.argv or "--refresh" in sys.argv or "-r" in sys.argv
    
    if force_refresh:
        print(f"\n{'='*100}")
        print("FORCE REFRESH MODE - Clearing cache and fetching fresh data")
        print(f"{'='*100}\n")
    
    # main(force_refresh=force_refresh)
    test()
