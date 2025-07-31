import csv
from faker import Faker
# Initialize the Faker library
fake = Faker()
# Number of users to generate
num_users = 10000
# Create a CSV file
import csv
import csv
from faker import Faker
import random
# Initialize the Faker library
fake = Faker()
# Number of users to generate
num_users = 10000
# List of vehicles to choose from
vehicles = ['Toyota', 'Honda', 'Ford', 'Chevrolet', 'Nissan']
# Create a CSV file
with open('fake_users.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    
    # Write the header
    writer.writerow(['name', 'email', 'licenseNo', 'vehicle'])
    
    # Generate fake users
    for _ in range(num_users):
        name = fake.name()
        email = fake.email()
        license_no = f'DRV{fake.random_int(min=100, max=999)}'
        vehicle = random.choice(vehicles)  # Use random.choice to select a vehicle
        
        # Write the user data
        writer.writerow([name, email, license_no, vehicle])
print(f'{num_users} fake users have been generated and saved to fake_users.csv')
import random
# Initialize the Faker library
fake = Faker()
# Number of users to generate
num_users = 10000
# List of vehicles to choose from
vehicles = ['Toyota', 'Honda', 'Ford', 'Chevrolet', 'Nissan']
# Create a CSV file
with open('fake_users.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    
    # Write the header
    writer.writerow(['name', 'email', 'licenseNo', 'vehicle'])
    
    # Generate fake users
    for _ in range(num_users):
        name = fake.name()
        email = fake.email()
        license_no = f'DRV{fake.random_int(min=100, max=999)}'
        vehicle = random.choice(vehicles)  # Use random.choice to select a vehicle
        
        # Write the user data
        writer.writerow([name, email, license_no, vehicle])
print(f'{num_users} fake users have been generated and saved to fake_users.csv')
    writer = csv.writer(file)
    
    # Write the header
    writer.writerow(['name', 'email', 'licenseNo', 'vehicle'])
    
    # Generate fake users
    for _ in range(num_users):
        name = fake.name()
        email = fake.email()
        license_no = f'DRV{fake.random_int(min=100, max=999)}'
        vehicle = fake.random_choice(elements=('Toyota', 'Honda', 'Ford', 'Chevrolet', 'Nissan'))
        
        # Write the user data
        writer.writerow([name, email, license_no, vehicle])
print(f'{num_users} fake users have been generated and saved to fake_users.csv')