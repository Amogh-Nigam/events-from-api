from configparser import ConfigParser
import json
import pandas as pd
import asyncio
import aiohttp

config = ConfigParser()
config.read('config.ini')
phq_events_url = config.get('url', 'predictHQ_url')
phq_api_key = config.get('predictHQ_param', 'auth')
category = config.get('predictHQ_param', 'category')
places_url = config.get('url', 'predictHQ_places')
tm_events_url = config.get('url', 'ticket_master_url')
tm_api_key = config.get('ticket_master_param', 'apikey')


def data_frame(raw_dat):
    """
    This function takes the raw data and returns the data frame.
    :param raw_dat: raw data
    :return df: data frame
    """
    df = pd.DataFrame(raw_dat,
                      columns=['event_name', 'start_date', 'end_date', 'timezone', 'venue_name', 'venue_address',
                               'venue_lat', 'venue_long', 'city', 'country', 'source'])
    return df


def extract_data_ticketmaster(data):
    """
    This function takes the JSON data, extracts the event data and returns a dictionary.
    :param data: JSON response data
    :return raw: dictionary list of events
    """
    raw = {
        "event_name": [],
        "start_date": [],
        "end_date": [],
        "timezone": [],
        "venue_name": [],
        "venue_address": [],
        "venue_lat": [],
        "venue_long": [],
        "city": [],
        "country": [],
        "source": []
    }
    try:
        for i in data['_embedded']['events']:
            raw['event_name'].append(i['name'])
            raw['start_date'].append(i['dates']['start']['localDate'])
            try:
                raw['end_date'].append(i['dates']['end']['localDate'])
            except KeyError:
                raw['end_date'].append(i['dates']['start']['localDate'])
            try:
                raw['timezone'].append(i['dates']['timezone'])
            except KeyError:
                try:
                    raw['timezone'].append(i['_embedded']['venues'][0]['timezone'])
                except KeyError:
                    raw['timezone'].append('')
            try:
                raw['venue_name'].append(i['_embedded']['venues'][0]['name'])
            except KeyError:
                raw['venue_name'].append('')
            try:
                raw['venue_address'].append(i['_embedded']['venues'][0]['address']['line1'])
            except KeyError:
                raw['venue_address'].append('')
            try:
                raw['venue_lat'].append(i['_embedded']['venues'][0]['location']['latitude'])
            except KeyError:
                raw['venue_lat'].append(" ")
            try:
                raw['venue_long'].append(i['_embedded']['venues'][0]['location']['longitude'])
            except KeyError:
                raw['venue_long'].append(" ")
            raw['city'].append(i['_embedded']['venues'][0]['city']['name'])
            try:
                raw['country'].append(i['_embedded']['venues'][0]['country']['name'])
            except KeyError:
                raw['country'].append(" ")
            raw['source'].append("Ticketmaster")
    except KeyError:
        pass
    return raw


async def get_events_data_ticketmaster(_inputs):
    """
    This function takes the inputs and returns the events in data range.
    :param _inputs: input data
    :return: data frame: events data
    """
    tm_main_df = pd.DataFrame()
    async with aiohttp.ClientSession() as session:
        base_url = tm_events_url + tm_api_key + f"&locale=*&startDateTime={_inputs['from_date']}T00:00:00Z&endDateTime={_inputs['to_date']}T00:00:00Z&size=200&city={_inputs['city']}&countryCode={_inputs['country_code']} "
        response = await session.get(base_url, ssl=False)
        data = await response.json()
        # Get the events data
        page = data['page']['totalPages']
        for i in range(0, page):
            if i == 0:
                raw_data = extract_data_ticketmaster(data)
                tm_main_df = data_frame(raw_data)
            else:
                url = base_url + f"&page={i}"
                response = await session.get(url, ssl=False)
                data = await response.json()
                raw_data = extract_data_ticketmaster(data)
                raw_df = data_frame(raw_data)
                tm_main_df = pd.concat([tm_main_df, raw_df], ignore_index=True)
        try:
            tm_main_df.drop_duplicates(inplace=True, keep='first',
                                       subset=['event_name', 'start_date', 'end_date', 'city'])
        except KeyError:
            pass
    return tm_main_df


def extract_data_predicthq(data, input_1):
    """
    This function takes the JSON data, extracts the event data and returns a dictionary.
    :param data: raw data
    :param input_1: input data
    :return: dictionary list of events
    """
    raw = {
        "event_name": [],
        "start_date": [],
        "end_date": [],
        "timezone": [],
        "venue_name": [],
        "venue_address": [],
        "venue_lat": [],
        "venue_long": [],
        "city": [],
        "country": [],
        "source": []
    }
    for i in data['results']:
        raw['event_name'].append(i['title'])
        raw['start_date'].append(i['start'])
        try:
            raw['end_date'].append(i['end'])
        except KeyError:
            raw['end_date'].append(i['start'])
        try:
            raw['timezone'].append(i['timezone'])
        except KeyError:
            raw['timezone'].append('')
        if i['entities']:
            try:
                raw['venue_name'].append(i['entities'][0]['name'])
            except KeyError:
                raw['venue_name'].append("")
            try:
                raw['venue_address'].append(i['entities'][0]['formatted_address'].replace('\n', ', '))
            except KeyError or IndexError:
                raw['venue_address'].append("")
        else:
            raw['venue_name'].append("")
            raw['venue_address'].append("")
        try:
            raw['venue_lat'].append(i['location'][1])
        except KeyError:
            raw['venue_lat'].append(" ")
        try:
            raw['venue_long'].append(i['location'][0])
        except KeyError:
            raw['venue_long'].append(" ")
        raw['city'].append(input_1['city'])
        raw['country'].append(input_1['country'])
        raw['source'].append("PredictHQ")
    return raw


async def get_events_data_predicthq(_inputs):
    """
    This function takes the inputs and returns the events in data range.
    :param _inputs: input data
    :return: dataframe: events data
    """
    phq_main_df = pd.DataFrame()
    async with aiohttp.ClientSession() as session:
        response2 = await session.get(
            url=places_url,
            headers={
                "Authorization": phq_api_key,
                "Accept": "application/json"
            },
            params={
                "q": _inputs['city'],
                "country": _inputs['country_code']
            },
            ssl=False
        )
        data = await response2.json()
        city_id = data['results'][0]['id']
        resp = await session.get(
            url=phq_events_url,
            headers={
                "Authorization": phq_api_key,
                "Accept": "application/json"
            },
            params={
                "country": _inputs['country_code'],
                "sort": "start",
                "limit": 50,
                "active.gte": _inputs['from_date'],
                "active.lte": _inputs['to_date'],
                "category": category,
                "place.scope": city_id,
                "state": "active"
            },
            ssl=False
        )
        data = await resp.json()
        # Get the events data
        count = data['count']
        page = count // 50 + 1
        for i in range(0, page):
            if i == 0:
                raw_data = extract_data_predicthq(data, _inputs)
                phq_main_df = data_frame(raw_data)
            else:
                response = await session.get(
                    url=data['next'],
                    headers={
                        "Authorization": phq_api_key,
                        "Accept": "application/json"
                    },
                    ssl=False
                )
                data = await response.json()
                raw_data = extract_data_predicthq(data, _inputs)
                raw_df = data_frame(raw_data)
                phq_main_df = pd.concat([phq_main_df, raw_df], ignore_index=True)
        try:
            phq_main_df.drop_duplicates(inplace=True, keep='first',
                                        subset=['event_name', 'start_date', 'end_date', 'city'])
        except KeyError:
            pass
    return phq_main_df


def get_events_data(_inputs):
    """
    This function takes the inputs and returns the events in data range.
    :param _inputs: input data
    :return: json data: events data
    """
    main_df = pd.DataFrame()
    df_tm = asyncio.run(get_events_data_ticketmaster(_inputs))
    df_phq = asyncio.run(get_events_data_predicthq(_inputs))
    try:
        main_df = pd.concat([df_tm, df_phq], ignore_index=True)
        main_df.drop_duplicates(inplace=True, keep='first', subset=['event_name', 'start_date', 'end_date', 'city'])
    except KeyError:
        pass
    main_json = json.loads(main_df.to_json(orient="records"))
    return main_json


# sample input
# _inputs = {"city": "new york", "from_date": "2022-08-02", "to_date": "2022-08-15", "country": "USA",
#            "country_code": "US"}
# _inputs = {"city": "rome", "from_date": "2022-07-31", "to_date": "2022-08-30", "country": "Italia",
#            "country_code": "IT"}

# get the events data
# events_json = get_events_data(_inputs)
# print(json.dumps(events_json, indent=4))

# Write the events' data to a file
# with open("sample.json", "w") as outfile:
#     json.dump(events_json, outfile, indent=4)
