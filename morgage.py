import requests
import xml.etree.ElementTree as ET
import unicodedata
import re
import time
from statistics import mean
from creds import username, password
from weds import webflow_bearer_token
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


field_mapping = {
    'mellomfinansiering': 'f-mellomfinansiering', 'trenger_ikke_pakke': 'f-trenger-ikke-pakke',
    'gront_boliglan_miljoboliglan': 'f-gront-boliglan-miljoboliglan', 'rammelan': 'f-rammelan',
    'forstehjemslan': 'f-forstehjemslan', 'student': 'f-student', 'lan_fritidsbolig': 'f-lan-fritidsbolig',
    'rentetak': 'f-rentetak', 'byggelan': 'f-byggelan', 'boliglan_for_unge': 'f-boliglan-for-unge',
    'pensjonist': 'f-pensjonist', 'renteberegning': 'f-renteberegning', 'produktpakke-tekst': 'f-produktpakke-tekst',
    'min_alder': 'f-min-alder', 'leverandor-tekst': 'f-leverandor-tekst', 'forbehold-2': 'f-forbehold-2',
    'etableringsgebyr': 'f-etableringsgebyr', 'maks-belaningsgrad': 'f-maks-belaningsgrad', 'title': 'name',
    'depotgebyr': 'f-depotgebyr', 'nominell_rente_1_a': 'f-nominell-rente-1-a', 'maks_belop_a': 'f-maks-belop-a',
    'markedsomraade': 'f-markedsomraade', 'maks_avdragsfrihet': 'f-maks-avdragsfrihet', 'min_belop_a': 'f-min-belop-a',
    'effektiv-rente': 'f-effektiv-rente', 'termingebyr_1_a': 'f-termingebyr-1-a', 'rentebinding_ar': 'f-rentebinding-ar'
}

namespaces = {'atom': 'http://www.w3.org/2005/Atom', 'f': 'http://www.finansportalen.no/feed/ns/1.0'}

def calculate_effective_interest_rate(xml_data):
    try:
        depotgebyr = float(xml_data.get('depotgebyr', '0') or '0')
        termingebyr = float(xml_data.get('termingebyr_1_a', '0') or '0')
        nominell_rente = float(xml_data.get('nominell_rente_1_a', '0') or '0') / 100

        laanebelop, laan_period, terminer_per_aar = 3000000, 25, 12
        total_gebyr = depotgebyr + (termingebyr * terminer_per_aar * laan_period)
        total_laanebelop = laanebelop + total_gebyr
        justert_nominell_rente = (total_laanebelop / laanebelop) * nominell_rente
        effektiv_rente = (1 + justert_nominell_rente / terminer_per_aar) ** terminer_per_aar - 1
        effektiv_rente_prosent = effektiv_rente * 100
        
        return effektiv_rente_prosent
    except Exception as e:
        logger.error(f"Error calculating effective interest rate: {str(e)}")
        return 0.0

def normalize_for_slug(text):
    return re.sub(r'[+%,:&()/.]', '', unicodedata.normalize('NFKD', (text or '').lower().replace('æ', 'a').replace('ø', 'o').replace('å', 'a')).encode('ascii', 'ignore').decode('utf-8').replace(' ', '-')).strip()

def extract_id(entry):
    return entry.find('atom:id', namespaces).text.split('/')[-1]

def get_norwegian_date():
    yesterday = datetime.now() - timedelta(days=1)
    months = ['januar', 'februar', 'mars', 'april', 'mai', 'juni', 'juli', 'august', 'september', 'oktober', 'november', 'desember']
    return f"Oppdatert {yesterday.day}. {months[yesterday.month - 1]} {yesterday.year} - 23:59"

def fetch_webflow_item(item_id):
    headers = {"accept": "application/json", "authorization": f"Bearer {webflow_bearer_token}"}
    response = requests.get(f"https://api.webflow.com/v2/collections/6686b2ab64e4a4d49a95b336/items/{item_id}", headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Failed to fetch Webflow item {item_id}. Status code: {response.status_code}")
        return None

def parse_xml_and_process():
    response = requests.get("https://www.finansportalen.no/services/feed/v3/bank/boliglan.atom", auth=(username, password))
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        entries = root.findall('atom:entry', namespaces)
        total_entries = len(entries)
        
        logger.info(f"Total number of entries in XML: {total_entries}")
        
        # Calculate average interest rate
        interest_rates = []
        for entry in entries:
            nominell_rente = entry.find('f:nominell_rente_1_a', namespaces)
            if nominell_rente is not None and nominell_rente.text:
                try:
                    rate = float(nominell_rente.text)
                    interest_rates.append(rate)
                except ValueError:
                    pass
        
        average_interest_rate = mean(interest_rates) if interest_rates else 0
        
        xml_entries = [(entry.find('atom:title', namespaces).text.strip(),
                        entry.find('f:leverandor_tekst', namespaces).text.strip() if entry.find('f:leverandor_tekst', namespaces) is not None else '',
                        {elem.tag.split('}')[1]: elem.text.strip() if elem.text else '' for elem in entry.findall('f:*', namespaces)},
                        extract_id(entry)) for entry in entries]
        
        check_webflow_existence(xml_entries, total_entries, average_interest_rate)
    else:
        logger.error(f"Failed to fetch XML data. Status code: {response.status_code}")


def update_specific_item(slug_id):
    headers = {"accept": "application/json", "authorization": f"Bearer {webflow_bearer_token}", "Content-Type": "application/json"}
    
    # Fetch the item
    item_response = requests.get(f"https://api.webflow.com/v2/collections/6686b2ab64e4a4d49a95b336/items/{slug_id}", headers=headers)
    
    if item_response.status_code != 200:
        logger.error(f"Failed to fetch item with slug ID {slug_id}. Status code: {item_response.status_code}")
        return
    
    item = item_response.json()
    
    # Prepare update payload (you may need to adjust this based on your needs)
    update_payload = {
        "isArchived": False,
        "isDraft": False,
        "fieldData": {
            'name': item['fieldData'].get('name', ''),
            'f-leverandor-tekst': item['fieldData'].get('f-leverandor-tekst', ''),
            'sist-oppdatert': get_norwegian_date()
            # Add other fields as needed
        }
    }
    
    # Attempt to update the item
    update_response = requests.patch(
        f"https://api.webflow.com/v2/collections/6686b2ab64e4a4d49a95b336/items/{slug_id}/live",
        json=update_payload,
        headers=headers
    )
    
    if update_response.status_code == 200:
        logger.info(f"Successfully updated item with slug ID {slug_id}")
    else:
        logger.error(f"Failed to update item with slug ID {slug_id}. Status code: {update_response.status_code}")        

def check_webflow_existence(xml_entries, total_entries, average_interest_rate):
    headers = {"accept": "application/json", "authorization": f"Bearer {webflow_bearer_token}", "Content-Type": "application/json"}
    
    webflow_items = fetch_all_webflow_items()
    successful_updates = 0
    new_items_created = 0
    
    for title, orgnr, xml_data, numerical_id in xml_entries:
        try:
            effektiv_rente_prosent = calculate_effective_interest_rate(xml_data)
            
            update_payload = {
                "isArchived": False,
                "isDraft": False,
                "fieldData": {
                    'name': title,
                    'f-leverandor-tekst': orgnr,
                    'f-effektiv-rente': f"{effektiv_rente_prosent:.2f}",
                    'total-banks': str(total_entries),
                    'average-interest-rate': f"{average_interest_rate:.2f}",
                    'boliglan': 'Boliglån' in title,
                    'f-maks-belaningsgrad': xml_data.get('maks_belaningsgrad', ''),
                    'f-rentebinding-ar': xml_data.get('rentebinding_ar', ''),
                    'f-maks-avdragsfrihet': xml_data.get('maks_avdragsfrihet', ''),
                    'f-maks-lopetid': xml_data.get('maks_lopetid', ''),
                    'f-produktpakke-tekst': xml_data.get('produktpakke_tekst', ''),
                    'f-nominell-rente-1-a': xml_data.get('nominell_rente_1_a', ''),
                    'f-termingebyr-1-a': xml_data.get('termingebyr_1_a', ''),
                    'f-mellomfinansiering': xml_data.get('mellomfinansiering', '').lower() == 'true',
                    'f-min-alder': xml_data.get('min_alder', ''),
                    'f-forbehold-2': xml_data.get('forbehold', ''),
                    'f-boliglan-for-unge': xml_data.get('boliglan_for_unge', '').lower() == 'true',
                    'f-forstehjemslan': xml_data.get('forstehjemslan', '').lower() == 'true',
                    'f-rammelan': xml_data.get('rammelan', '').lower() == 'true',
                    'f-lan-fritidsbolig': xml_data.get('lan_fritidsbolig', '').lower() == 'true',
                    'eksempel-rente': calculate_eksempel_rente(3000000, 25, effektiv_rente_prosent),
                    'sist-oppdatert': get_norwegian_date()
                }
            }

            bank_id = get_bank_id(orgnr)
            if bank_id:
                update_payload['fieldData']['bank'] = bank_id

            webflow_item = webflow_items.get(numerical_id)

            if webflow_item:
                # Update existing item
                update_response = requests.patch(
                    f"https://api.webflow.com/v2/collections/6686b2ab64e4a4d49a95b336/items/{webflow_item['id']}/live",
                    json=update_payload,
                    headers=headers
                )

                print(f"Item ID: {numerical_id}, Status Code: {update_response.status_code}")

                if update_response.status_code == 200:
                    successful_updates += 1
            else:
                # Create new item
                create_payload = update_payload.copy()
                create_payload['fieldData']['slug'] = numerical_id  # Set the slug for the new item
                
                create_response = requests.post(
                    f"https://api.webflow.com/v2/collections/6686b2ab64e4a4d49a95b336/items",
                    json=create_payload,
                    headers=headers
                )

                print(f"New Item ID: {numerical_id}, Status Code: {create_response.status_code}")

                if create_response.status_code == 200:
                    new_items_created += 1
                else:
                    print(f"Failed to create new item: {numerical_id}. Error: {create_response.text}")

        except Exception as e:
            print(f"Item ID: {numerical_id}, Error: {str(e)}")

        time.sleep(1)  # 1 second delay between API calls
    
    print(f"Total successful updates: {successful_updates} out of {total_entries} XML entries")
    print(f"New items created: {new_items_created}")

def fetch_all_webflow_items():
    headers = {"accept": "application/json", "authorization": f"Bearer {webflow_bearer_token}"}
    all_items = {}
    offset = 0
    limit = 100

    while True:
        response = requests.get(
            f"https://api.webflow.com/v2/collections/6686b2ab64e4a4d49a95b336/items?limit={limit}&offset={offset}",
            headers=headers
        )
        if response.status_code == 200:
            items = response.json()['items']
            for item in items:
                all_items[item['fieldData'].get('slug', '')] = item
            if len(items) < limit:
                break
            offset += limit
        else:
            print(f"Failed to fetch Webflow items. Status code: {response.status_code}")
            break
        time.sleep(1)

    print(f"Fetched {len(all_items)} unique items from Webflow")
    return all_items

def get_bank_id(orgnr):
    headers = {"accept": "application/json", "authorization": "Bearer a015cb2d28c98a432dd0d7dab54c5dc32861646565d33f42883c78815babb1de"}
    for offset in range(0, 400, 100):
        response = requests.get(f"https://api.webflow.com/v2/collections/66636a29a268f18ba1798b0a/items?limit=100&offset={offset}", headers=headers)
        if response.status_code == 200:
            for item in response.json().get('items', []):
                if item.get('fieldData', {}).get('name') == orgnr:
                    print(f"Found bank ID: {item['id']} for orgnr: {orgnr}")
                    return item['id']
        else:
            print(f"Failed to retrieve data for offset {offset} while fetching bank ID. Status code: {response.status_code}")
    print(f"No bank ID found for orgnr: {orgnr}")
    return None

def calculate_eksempel_rente(loan_amount, years, effective_rate):
    monthly_rate = effective_rate / 12 / 100
    num_payments = years * 12
    monthly_payment = loan_amount * (monthly_rate * (1 + monthly_rate)**num_payments) / ((1 + monthly_rate)**num_payments - 1)
    total_cost = monthly_payment * num_payments
    interest_cost = total_cost - loan_amount
    return f"Kostnad: {interest_cost:,.0f} kr, totalpris: {total_cost:,.0f} kr".replace(',', ' ')

def update_webflow_item(item_id, payload):
    headers = {"accept": "application/json", "authorization": f"Bearer {webflow_bearer_token}", "Content-Type": "application/json"}
    if 'slug' in payload['fieldData']:
        del payload['fieldData']['slug']

    try:
        effektiv_rente_prosent = float(payload['fieldData'].get('f-effektiv-rente', '0'))
        payload['fieldData']['eksempel-rente'] = calculate_eksempel_rente(3000000, 25, effektiv_rente_prosent)
        payload['fieldData']['sist-oppdatert'] = get_norwegian_date()

        logger.info(f"Updating item {item_id} with payload: {payload}")
        response = requests.patch(f"https://api.webflow.com/v2/collections/6686b2ab64e4a4d49a95b336/items/{item_id}/live", json=payload, headers=headers)
        response.raise_for_status()
        logger.info(f"Successfully updated item with ID {item_id} in Webflow.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to update item with ID {item_id} in Webflow. Error: {str(e)}")
        logger.error(f"Response content: {e.response.content if e.response else 'No response'}")
    except Exception as e:
        logger.error(f"Unexpected error while updating item with ID {item_id}: {str(e)}")
    
    time.sleep(1)  # 1 second delay between API calls
        

def create_webflow_item(title, orgnr, xml_data, numerical_id, bank_id, effektiv_rente_prosent, total_entries, average_interest_rate, is_boliglan):
    headers = {"accept": "application/json", "authorization": f"Bearer {webflow_bearer_token}", "Content-Type": "application/json"}
    field_data = {webflow_field: xml_data.get(xml_field, '') for xml_field, webflow_field in field_mapping.items()}
    
    try:
        field_data.update({
            "eksempel-rente": calculate_eksempel_rente(3000000, 25, effektiv_rente_prosent),
            "name": title,
            "slug": numerical_id,
            "f-leverandor-tekst": orgnr,
            "f-effektiv-rente": f"{effektiv_rente_prosent:.2f}",
            "total-banks": str(total_entries),
            "average-interest-rate": f"{average_interest_rate:.2f}",
            "boliglan": is_boliglan,
            "f-maks-avdragsfrihet": xml_data.get("maks_avdragsfrihet", ""),
            "f-maks-lopetid": xml_data.get("maks_lopetid", ""),
            "f-maks-belaningsgrad": xml_data.get("maks_belaningsgrad", ""),
            "f-produktpakke-tekst": xml_data.get("produktpakke_tekst", ""),
            "f-nominell-rente-1-a": xml_data.get("nominell_rente_1_a", ""),
            "f-termingebyr-1-a": xml_data.get("termingebyr_1_a", ""),
            "f-mellomfinansiering": xml_data.get("mellomfinansiering", "").lower() == "true",
            "f-min-alder": xml_data.get("min_alder", ""),
            "f-forbehold-2": xml_data.get("forbehold", ""),
            "f-boliglan-for-unge": xml_data.get("boliglan_for_unge", "").lower() == "true",
            "f-forstehjemslan": xml_data.get("forstehjemslan", "").lower() == "true",
            "f-rammelan": xml_data.get("rammelan", "").lower() == "true",
            "sist-oppdatert": get_norwegian_date()
        })
        
        if bank_id:
            field_data["bank"] = bank_id

        payload = {"isArchived": False, "isDraft": False, "fieldData": field_data}
        
        logger.info(f"Creating new item for {title} with payload: {payload}")
        response = requests.post("https://api.webflow.com/v2/collections/6686b2ab64e4a4d49a95b336/items/live", json=payload, headers=headers)
        response.raise_for_status()
        logger.info(f"Successfully created item for {title} in Webflow.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to create item for {title} in Webflow. Error: {str(e)}")
        logger.error(f"Response content: {e.response.content if e.response else 'No response'}")
    except Exception as e:
        logger.error(f"Unexpected error while creating item for {title}: {str(e)}")
    
    time.sleep(1)  # 1 second delay between API calls

parse_xml_and_process()

def main():
    # First, update the specific item
    update_specific_item('46135')
    
    # Then proceed with the regular update process
    parse_xml_and_process()

if __name__ == "__main__":
    main()