###############################################################################################################
# component name        : assign_rides.py
# Purpose               : Ride Allocation Algorithm

"""
Introduction:
-------------
We have a scenario where there are a lot of rides and drivers in New York for tomorrow. 
We need to develop a backend algorithm to allocate the rides to the drivers efficiently, considering multiple factors. 
The algorithm needs to ensure that there are no conflicts in ride allocation, 
allocate rides to lower-priced drivers if possible, 
and ensure that drivers do not have to drive a lot without a paying passenger.

Input:
------
The algorithm takes a collection of rides as input. 
Each ride consists of the pickup time, pickup location (latitude/longitude), pickup address, 
drop-off location/address, and estimated ride duration.

Output:
------
The output of the algorithm is the allocation of rides to drivers in an optimized way that satisfies the constraints specified above.

Algorithm: (Please check the attached images for flow charts)
----------
The ride allocation algorithm (Batch Way) can be implemented using the following steps:
    1. Collect all the ride requests from Database/Cache DB like Redis for that particular city or area based on the architecture plan
    2. Collect all the drivers details from Database/Cache DB like Redis for that particular city or area based on the archiecture plan
    3. Iterate through each ride 
    4. Find nearby drivers for some specific radius like 10 KMs or 50 KMs based on the business need and also on available schedule details
       by using libraries like sklearn or any other best library based on POC with parameters like performance, load testing, accuracy etc
    5. Find the best possible drivers based on total incur cost for that trip , possibility of reaching the place within expected pickup time etc
    6. Assign the driver to that trip by choosing the best driver with scoring as first 
    7. If no driver is available for a ride, add the ride to a queue for future allocation


The ride allocation algorithm (Realtime and parrallel processing way) can be implemented using the following steps:
    1. Collect all the ride requests from Database/Cache DB like Redis for that particular city or area based on the architecture plan
    2. Iterate through each ride and place a message in real time processing queues/or any other module which will process next steps
    3. Next process will pick multiple messages parallelly and follows the below steps
    4. Collect all the drivers details from Database/Cache DB like Redis for that particular city or area based on the architecture plan
    5. Find nearby drivers for some specific radius like 10 KMs or 50 KMs based on the business need and also on available schedule details
       by using libraries like sklearn or any other best library based on POC with parameters like performance, load testing, accuracy etc
    6. Find the best possible drivers based on total incur cost for that trip , possibility of reaching the place within expected pickup time etc
    7. Assign the driver to that trip by choosing the best driver with scoring as first 
    8. If no driver is available for a ride, add the ride to a queue for future allocation


Conclusion:
----------
The ride allocation algorithm allocates rides to drivers efficiently, ensuring there are no conflicts in ride allocation, 
allocating rides to lower-priced drivers if possible, and ensuring that drivers do not have to drive a lot without a paying passenger. 
The algorithm can be implemented using various programming languages and can be further optimized to improve its efficiency.

Points to be noted:
-------------------
1. This is the basic algorithm developed within the expected time frame and subject to enhancements 
2. We can enhance this algorithm based on the business needs, scenarios , technical advancements like new effective libraries etc
3. Please note that I didnt document lot of non functional engineering steps and modules due to time constraints 

"""


#Revision History

#Version            Date                Author
#0.1                04/12/2023          Ramasamy Ramar

###############################################################################################################

####### Import section ########################################################################################
from geopy.distance import geodesic
import random
import os
from dotenv import load_dotenv
import smtplib
import json
import datetime
from find_nearest_drivers_within_radius import find_nearest_drivers_within_radius


##### Loading Environment variables ###############################################################################################
load_dotenv()

##### Variables Section ###########################################################################################################
newyork_latitude = 40.7128
newyork_longitude = -74.0060
geo_radius=0.3  # New York City, 0.3 variance in latitude corresponds to approximately 33 kilometers (20.5 miles) and 0.3 variance in longitude corresponds to approximately 25 kilometers (15.5 miles)
sender_email = "sender@test.com" 
receiver_email = "receiver@test.com" 
email_password = os.getenv("EMAIL_PASSWORD")


############################### Classes and Functions definitions ##################################################################

#### Common Functions #####################################################
def generate_latitude(geo_radius):
    return newyork_latitude + random.uniform(-geo_radius, geo_radius)

def generate_longitude(geo_radius):
    return newyork_longitude + random.uniform(-geo_radius, geo_radius)

def send_email(subject, body, sender_email, receiver_email, email_password):
    message = f"Subject: {subject}\n\n{body}"

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, email_password)
        server.sendmail(sender_email, receiver_email, message)

############ Step 01 - Get Available Rides data - Using mocked up data , but ideally real data can be pulled ##########################
""" Notes:
The current rides can be gathered from Cache DB like Redis or may be from DB based on architecture.
But here just used library to generate mockup data.
"""
def generate_mockup_rides(num_rides):
    rides = []
    for i in range(num_rides):
        pickup_time = random.randint(30, 60) # minutes from now
        pickup_latitude = generate_latitude(geo_radius)
        pickup_longitude = generate_longitude(geo_radius)
        dropoff_latitude = generate_latitude(geo_radius)
        dropoff_longitude = generate_longitude(geo_radius)
        ride_duration = random.randint(10, 60)  # minutes
        ride_distance=geodesic((pickup_latitude, pickup_longitude), (dropoff_latitude, dropoff_longitude)).km
        rides.append({
            'id' : i,
            'pickup_location': (pickup_latitude, pickup_longitude),
            'pickup_time' : pickup_time,
            'dropoff_location': (dropoff_latitude, dropoff_longitude),
            'estimated_ride_duration': ride_duration,
            'estimated_ride_distance': ride_distance
        })
    return rides

try: 
    newyork_rides=generate_mockup_rides(5) # Data Sampling - ride numbers taken as 5 and can be modified.
    print(newyork_rides)
except Exception as error:  
    body=error  
    send_email("Module: Generate Mockup Rides", body, sender_email, receiver_email, email_password)
    #Send_alert_message(**vars) - we can write a module to send slack/whatsapp message to production support group
    #send_automated_alert_call(**vars) - we can write a trigger function using cloud telephony for a phone call to on-call support person as a critical app failure


############ Step 02 - Get All Available Drivers data in NewYork - Used mocked up data , but ideally real data can be pulled ##########################
""" Notes:
The current drivers details can be gathered from Cache DB like Redis or may be from DB based on architecture.
But here just used library to generate mockup data.
"""

def generate_mockup_drivers(num_drivers):
    drivers = []
    for i in range(num_drivers):
        current_latitude = generate_latitude(geo_radius)
        current_longitude = generate_longitude(geo_radius)
        driver_price = random.randint(20, 30)  # dollars per hour
        vehicle_price = random.randint(1, 8)  # dollars per km
        drivers.append({
            'id': i,
            'current_location': (current_latitude, current_longitude),
            'driver_price': driver_price,
            'price_per_km': vehicle_price
        })
    return drivers

try: 
    newyork_drivers=generate_mockup_drivers(4) # Data Sampling - driver numbers taken as 4 and can be modified.
    print(newyork_drivers)
except Exception as error:  
    body=error  
    send_email("Module: Generate Mockup Drivers", body, sender_email, receiver_email, email_password)
    #Send_alert_message(**vars) - we can write a module to send slack/whatsapp message to production support group
    #send_automated_alert_call(**vars) - we can write a trigger function using cloud telephony for a phone call to on-call support person as a critical app failure



########## Step 3 : Assign each ride to best possible driver ###############################################################################
"""
1. Drivers will be filtered based on the specific raduis like 3 KM or any other value based on the need.
2. Find the oppourtunity cost between the filtered drivers costing
3. Try to assign the best possible driver if he is still available 
4. If best possible driver is already assigned by another request , use the next available driver
"""

radius = 100 #100 KM radius is considered to find the nearby taxi drivers 

def assign_rides_to_drivers(rides, drivers):
    assigned_drivers=[]
    for ride in rides[0:3]: ### Restricted to 3 rides for testing purpose
        ride_lat, ride_long = ride['pickup_location']
        expected_pickup_time = ride['pickup_time']
        ######################### Step 3: Find only the nearby drivers #########################################
        nearby_drivers = find_nearest_drivers_within_radius(ride_lat,ride_long,radius,drivers)

        ######################### Step 4: Find costing for each nearby driver ##################################
        final_data = []
        for each_driver in nearby_drivers:
            if each_driver['id'] not in assigned_drivers:
                print(each_driver)
                distance_between_entities = round(geodesic(each_driver['current_location'], ride['pickup_location']).km,2)
                print(distance_between_entities)
                hours_to_reach_the_pickup_location = round( (distance_between_entities * 5 / 60 ) , 2) # Considering 5 minutes to travel 1 KM , but ideally we can use separate module using google APIs to find out the actual time based on traffic
                hours_to_reach_the_destination_location = round( (ride['estimated_ride_distance'] * 5 / 60 ) , 2)
                cost_to_reach_pickup_location = ( each_driver['price_per_km'] * distance_between_entities )  + (hours_to_reach_the_pickup_location * each_driver['driver_price'])
                cost_of_travel_to_destination = ( each_driver['price_per_km'] * ride['estimated_ride_distance'] )  + (hours_to_reach_the_destination_location * each_driver['driver_price'])
                total_cost = round( cost_to_reach_pickup_location + cost_of_travel_to_destination , 2)
                print(total_cost)

                data = {"driver_id": each_driver['id'], "hours_to_reach_the_pickup_location": hours_to_reach_the_pickup_location, "total_cost": total_cost}
                final_data.append(data)


        # Calculate the weighted score for each suitable driver
        for driver in final_data:
            cost_weight = 0.7
            pickup_time_weight = 0.3
            cost_score = (1 / driver['total_cost']) * cost_weight
            pickup_time_score = hours_to_reach_the_pickup_location / 3600 * pickup_time_weight
            driver['score'] = cost_score + pickup_time_score

        # Choose the best suitable driver based on the weighted score
        if final_data:    
            best_driver = max(final_data, key=lambda driver: driver['score']) 

        print(final_data)
        print(best_driver)

        ### Assign driver to this ride 
        assigned_drivers.append(best_driver['driver_id'])

        
        print("\n\n\nBest driver for trip id - " + str(ride['id']) + " is with id - " + str(best_driver['driver_id']) + "\n\n\n")
        ########## Here we can all API/another module to set the driver id for that ride and also other functions like sending push notifications, message and email etc





try: 
    assign_rides_to_drivers(newyork_rides,newyork_drivers)
except Exception as error:  
    body=error  
    send_email("Module: Assign Drivers", body, sender_email, receiver_email, email_password)
    #Send_alert_message(**vars) - we can write a module to send slack/whatsapp message to production support group
    #send_automated_alert_call(**vars) - we can write a trigger function using cloud telephony for a phone call to on-call support person as a critical app failure


