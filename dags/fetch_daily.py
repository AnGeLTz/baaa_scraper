import requests
from bs4 import BeautifulSoup as bs
import datetime
import psycopg2


connection = psycopg2.connect(
    host="164.90.174.46",
    port=5432,
    database="baaa",
    user="airflow",
    password="airflow"
)
cursor = connection.cursor()

def check_for_accident():

    url = 'https://www.baaa-acro.com/crash-archives?created=' + str(datetime.date.today())
    print("Checking ", url)
    response = requests.get(url)
    soup = bs(response.content, 'html.parser')
    trigger_point = soup.find('div', class_='table-responsive')
    if trigger_point:
        return True
    else:
        return False

def get_links():
    links = []
    url = 'https://www.baaa-acro.com/crash-archives?created=' + str(datetime.date.today())
    response = requests.get(url)
    soup = bs(response.content, 'html.parser')
    for i in soup.find_all('a', class_ = 'red-btn'):
        links.append('https://www.baaa-acro.com' + str(i.get('href')))
    return links

def get_data():
  links = get_links()
  for link in links:
    response = requests.get(link)
    soup = bs(response.content,'html.parser')
    try:
        date = soup.find('div', class_='crash-date').text.split(':')[1].strip()
        date = datetime.strptime(date, "%b %d, %Y at %H%M LT")
        date = datetime.strftime(date, "%Y-%m-%d")
    except:
        date = None

    try:
        type_aircraft = soup.find('div',class_='field field--name-field-crash-aircraft field--type-entity-reference field--label-hidden field--item').text
    except:
        type_aircraft = None

    try:
        operator = soup.find('div', class_='crash-operator').find('img').get('src').split('/')[-1].split('.jpg')[0]
        operator = operator.replace("%20", " ")
        operator = operator.replace("%26", " ")
        operator = operator.replace("%27", " ")
        operator = operator.replace(".png", "")
        operator = operator.replace(".gif", "")
    except:
        try:
            operator = soup.find('div', class_='crash-operator').find('div').text
        except:
            operator = None

    try:
        flight_phase = soup.find('div', class_='crash-flight-phase').find('div').text
    except:
        flight_phase = None

    try:
        flight_type = soup.find('div', class_='crash-flight-type').find('div').text
    except:
        flight_type = None

    try:
        survivors = soup.find('div', class_='crash-survivors').find('div').text
    except:
        survivors = None

    try:
        site = soup.find('div', class_='crash-site').find('div').text
    except:
        site = None

    try:
        schedule = soup.find('div', class_='crash-schedule').find('div').text
    except:
        schedule = None

    try:
        msn = soup.find('div', class_='crash-construction-num').find('div').text
    except:
        msn = None

    try:
        yom = int(soup.find('div', class_='crash-yom').find('div').text)
    except:
        yom = None

    try:
        flight_number = soup.find('div', class_='crash-flight-number').find('div').text
    except:
        flight_number = None

    try:
        city = soup.find('div',
                         class_='field field--name-field-crash-city field--type-entity-reference field--label-hidden field--item').text
    except:
        city = None

    try:
        zone = soup.find('div',
                         class_='field field--name-field-crash-zone field--type-entity-reference field--label-hidden field--item').text
    except:
        zone = None

    try:
        country = soup.find('div', class_='crash-country').find('div').text
    except:
        country = None

    try:
        region = soup.find('div', class_='crash-region').find('div').text
    except:
        region = None

    try:
        crew_on_board = int(soup.find('div', class_='crash-crew-on-board').find('div').text)
    except:
        crew_on_board = None

    try:
        crew_fatalities = int(soup.find('div', class_='crash-crew-fatalities').find('div').text)
    except:
        crew_fatalities = None

    try:
        pax_on_board = int(soup.find('div', class_='crash-pax-on-board').find('div').text)
    except:
        pax_on_board = None

    try:
        pax_fatalities = int(soup.find('div', class_='crash-pax-fatalities').find('div').text)
    except:
        pax_fatalities = None

    try:
        other_fatalities = int(soup.find('div', class_='crash-other-fatalities').find('div').text)
    except:
        other_fatalities = None

    try:
        total_fatalities = int(soup.find('div', class_='crash-total-fatalities').find('div').text)
    except:
        total_fatalities = None

    try:
        captain_hours = int(soup.find('div', class_='captain-total-flying-hours').find('div').text)
    except:
        captain_hours = None

    try:
        captain_hours_type = int(soup.find('div', class_='captain-total-hours-type').find('div').text)
    except:
        captain_hours_type = None

    try:
        copilot_hours = int(soup.find('div', class_='copilot-total-flying-hours').find('div').text)
    except:
        copilot_hours = None

    try:
        copilot_hours_type = int(soup.find('div', class_='copilot-total-hours-type').find('div').text)
    except:
        copilot_hours_type = None

    try:
        aircraft_hours = int(soup.find('div', class_='crash-aircraft-flight-hours').find('div').text)
    except:
        aircraft_hours = None

    try:
        circumstances = soup.find('div', class_='crash-circumstances').find('div').text
    except:
        circumstances = None

    try:
        probable_cause = soup.find('div',class_='field field--name-field-crash-causes field--type-string-long field--label-hidden field--item').text
    except:
        probable_cause = None

    check_query = """
            SELECT * FROM statistics.air_accidents WHERE date = %s AND msn = %s;
    """
    cursor.execute(check_query, (date, msn))

    if cursor.fetchone() is not None:
        print("The data already exists in the database")
    else:
        max_id_query = "SELECT MAX(id) FROM statistics.air_accidents"
        cursor.execute(max_id_query)
        max_id = cursor.fetchone()[0]
        new_id = max_id + 1 if max_id is not None else 1

        insert_query = """
                INSERT INTO statistics.air_accidents (
                    id,
                    date,
                    type,
                    operator,
                    flight_phase,
                    flight_type,
                    survivors,
                    site,
                    schedule,
                    msn,
                    yom,
                    flight_number,
                    city,
                    zone,
                    country,
                    region,
                    crew_on_board,
                    crew_fatalities,
                    pax_on_board,
                    pax_fatalities,
                    other_fatalities,
                    total_fatalities,
                    captain_hours,
                    captain_hours_type,
                    copilot_hours,
                    copilot_hours_type,
                    aircraft_hours,
                    circumstances,
                    probable_cause
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                """
        # Tuple containing the values to be inserted
        data = (
            new_id, date, type_aircraft, operator, flight_phase, flight_type, survivors, site, schedule,
            msn, yom, flight_number, city, zone, country, region, crew_on_board, crew_fatalities,
            pax_on_board, pax_fatalities, other_fatalities, total_fatalities, captain_hours,
            captain_hours_type, copilot_hours, copilot_hours_type, aircraft_hours, circumstances,
            probable_cause
        )

        # Execute the INSERT statement with the data tuple
        cursor.execute(insert_query, data)

  # Commit the changes to the database
  connection.commit()

  # Close the cursor and connection
  cursor.close()
  connection.close()



def run():
    if check_for_accident():
        get_data()
    else:
        print('No Accidents have been found')

run()