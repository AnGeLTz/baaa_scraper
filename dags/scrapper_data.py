import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import psycopg2


# Establish a connection to the PostgreSQL database
connection = psycopg2.connect(
    host="164.90.174.46",
    port=5432,
    database="baaa",
    user="airflow",
    password="airflow"
)

# Create a cursor object to interact with the database
cursor = connection.cursor()

with open("urls.json", "r") as file:

    links = json.load(file)
    for link in tqdm(links):
        url = link
        response = requests.get(url)
        soup = bs(response.content, 'html.parser')
        try:
            date = soup.find('div', class_='crash-date').text.split(':')[1].strip()
        except:
            date = None

        try:
            type_aircraft = soup.find('div',class_='field field--name-field-crash-aircraft field--type-entity-reference field--label-hidden field--item').text
        except:
            type_aircraft = None

        try:
            operator = soup.find('div', class_='crash-operator').find('img').get('src').split('/')[-1].split('.jpg')[0]
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
            probable_cause = soup.find('div',
                                       class_='field field--name-field-crash-causes field--type-string-long field--label-hidden field--item').text
        except:
            probable_cause = None


        #INSERT statement to insert data into the accidents table
        insert_query = """
        INSERT INTO raw_data.accidents (
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
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        # Tuple containing the values to be inserted
        data = (
            date, type_aircraft, operator, flight_phase, flight_type, survivors, site, schedule,
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




       












