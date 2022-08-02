import json
import datetime
from geopy.geocoders import Nominatim
import config as config
import country_converter as coco


def inputs():
    """
    This function takes the user input and returns a dictionary with the
    input values.
    """
    # Get the user input
    city = input("Enter the city name: ")
    from_date = input("Enter the start date (dd-mm-yyyy) : ")
    to_date = input("Enter the end date (dd-mm-yyyy) : ")
    # Convert the date to datetime format
    try:
        from_date = datetime.datetime.strptime(from_date, "%d-%m-%Y").strftime('%Y-%m-%d')
    except ValueError:
        print("Invalid date format. Please enter the date in dd-mm-yyyy format.")
        exit()
    try:
        to_date = datetime.datetime.strptime(to_date, "%d-%m-%Y").strftime('%Y-%m-%d')
    except ValueError:
        print("Invalid date format. Please enter the date in dd-mm-yyyy format.")
        exit()
    # Create a dictionary with the input values
    geolocator = Nominatim(user_agent="city_events")
    location = geolocator.geocode(city.lower())
    str_location = str(location)
    country = str_location.split(',')[-1]
    country_code = coco.convert(country, to='ISO2', not_found=None)
    _inputs = {"city": city.lower(), "from_date": from_date, "to_date": to_date, "country": country, "country_code": country_code}
    return _inputs


# get the input values
# For big cities divide the date range into multiple requests and get the data as API only support retrieving the 1000th
# item. i.e. ( size * page < 1000) and size(numbers of events in 1 page) = 200
inputs_api = inputs()
# get the events data
data_json = config.get_events_data(inputs_api)
# Print the events' data
# print(json.dumps(data_json, indent=4))
# Write the events' data to a file
with open("results.json", "w") as outfile:
    json.dump(data_json, outfile, indent=4)
