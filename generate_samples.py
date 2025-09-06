# FlowForge Sample Data Generator
# This script can be used to generate additional sample datasets

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path

def generate_ecommerce_data(num_records=200):
    """Generate e-commerce transaction data"""
    
    products = [
        ("Smartphone", "Electronics", 599.99),
        ("Laptop", "Electronics", 1299.99),
        ("Headphones", "Electronics", 149.99),
        ("Coffee Maker", "Appliances", 89.99),
        ("Blender", "Appliances", 79.99),
        ("Running Shoes", "Sports", 129.99),
        ("Yoga Mat", "Sports", 29.99),
        ("Protein Powder", "Health", 49.99),
        ("Vitamins", "Health", 24.99),
        ("Desk Chair", "Furniture", 199.99),
    ]
    
    customers = [
        ("Premium", 0.7, 0.3),  # (type, repeat_probability, high_value_probability)
        ("Regular", 0.4, 0.1),
        ("New", 0.1, 0.05)
    ]
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(num_records):
        # Generate transaction
        product = random.choice(products)
        customer_type = random.choice(customers)
        
        transaction_date = start_date + timedelta(days=random.randint(0, 90))
        quantity = random.randint(1, 3)
        
        # Add some price variation
        base_price = product[2]
        price_variation = random.uniform(0.9, 1.1)
        unit_price = base_price * price_variation
        
        total_amount = unit_price * quantity
        
        # Apply customer type effects
        if customer_type[0] == "Premium":
            # Premium customers might get discounts on bulk purchases
            if quantity > 1:
                total_amount *= 0.95
        
        data.append({
            "TransactionID": f"TXN{i+1:04d}",
            "Date": transaction_date.strftime("%Y-%m-%d"),
            "ProductName": product[0],
            "Category": product[1], 
            "Quantity": quantity,
            "UnitPrice": round(unit_price, 2),
            "TotalAmount": round(total_amount, 2),
            "CustomerType": customer_type[0]
        })
    
    return pd.DataFrame(data)

def generate_employee_data(num_records=100):
    """Generate employee data"""
    
    departments = ["Engineering", "Marketing", "Sales", "HR", "Finance", "Operations"]
    positions = {
        "Engineering": ["Software Engineer", "Senior Engineer", "Tech Lead", "Engineering Manager"],
        "Marketing": ["Marketing Specialist", "Digital Marketer", "Marketing Manager", "Brand Manager"],
        "Sales": ["Sales Representative", "Account Manager", "Sales Manager", "VP Sales"],
        "HR": ["HR Specialist", "Recruiter", "HR Manager", "VP HR"],
        "Finance": ["Financial Analyst", "Accountant", "Finance Manager", "CFO"],
        "Operations": ["Operations Specialist", "Project Manager", "Operations Manager", "VP Operations"]
    }
    
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Lisa", "Robert", "Emily", 
                   "James", "Ashley", "William", "Jessica", "Richard", "Amanda", "Thomas"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", 
                  "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez"]
    
    data = []
    
    for i in range(num_records):
        department = random.choice(departments)
        position = random.choice(positions[department])
        
        # Generate salary based on position level
        base_salaries = {
            "Specialist": random.randint(45000, 65000),
            "Representative": random.randint(40000, 60000),
            "Engineer": random.randint(70000, 100000),
            "Analyst": random.randint(55000, 75000),
            "Manager": random.randint(85000, 120000),
            "Lead": random.randint(95000, 130000),
            "VP": random.randint(150000, 200000),
            "CFO": random.randint(200000, 300000)
        }
        
        salary = base_salaries.get("Specialist", 50000)  # default
        for key in base_salaries:
            if key in position:
                salary = base_salaries[key]
                break
        
        hire_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))  # 4 years
        
        data.append({
            "EmployeeID": f"EMP{i+1:04d}",
            "FirstName": random.choice(first_names),
            "LastName": random.choice(last_names),
            "Department": department,
            "Position": position,
            "Salary": salary,
            "HireDate": hire_date.strftime("%Y-%m-%d"),
            "Age": random.randint(22, 65),
            "YearsExperience": random.randint(0, 25)
        })
    
    return pd.DataFrame(data)

def generate_sensor_data(num_records=500):
    """Generate IoT sensor data"""
    
    sensors = ["Temperature", "Humidity", "Pressure", "Motion", "Light"]
    locations = ["Building A", "Building B", "Building C", "Warehouse", "Parking Lot"]
    
    data = []
    start_time = datetime(2024, 1, 1)
    
    for i in range(num_records):
        sensor_type = random.choice(sensors)
        location = random.choice(locations)
        
        timestamp = start_time + timedelta(minutes=random.randint(0, 43200))  # 30 days in minutes
        
        # Generate realistic values based on sensor type
        if sensor_type == "Temperature":
            value = round(random.uniform(18.0, 26.0), 2)  # Celsius
            unit = "°C"
        elif sensor_type == "Humidity":
            value = round(random.uniform(30.0, 80.0), 1)  # Percentage
            unit = "%"
        elif sensor_type == "Pressure":
            value = round(random.uniform(1010.0, 1025.0), 1)  # hPa
            unit = "hPa"
        elif sensor_type == "Motion":
            value = random.choice([0, 1])  # Binary
            unit = "boolean"
        else:  # Light
            value = random.randint(0, 1000)  # Lux
            unit = "lux"
        
        data.append({
            "SensorID": f"SENSOR_{i+1:04d}",
            "Timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "SensorType": sensor_type,
            "Location": location,
            "Value": value,
            "Unit": unit,
            "Status": random.choice(["Active", "Active", "Active", "Maintenance"])  # Mostly active
        })
    
    return pd.DataFrame(data)

if __name__ == "__main__":
    # Generate sample datasets
    samples_dir = Path("data/samples")
    samples_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate and save datasets
    ecommerce_df = generate_ecommerce_data(200)
    ecommerce_df.to_csv(samples_dir / "ecommerce.csv", index=False)
    
    employee_df = generate_employee_data(100)
    employee_df.to_csv(samples_dir / "employees.csv", index=False)
    
    sensor_df = generate_sensor_data(500)
    sensor_df.to_csv(samples_dir / "sensor_data.csv", index=False)
    
    print("Sample datasets generated successfully!")
    print(f"- ecommerce.csv: {len(ecommerce_df)} records")
    print(f"- employees.csv: {len(employee_df)} records") 
    print(f"- sensor_data.csv: {len(sensor_df)} records")