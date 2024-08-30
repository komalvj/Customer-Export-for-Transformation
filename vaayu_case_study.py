import pandas as pd
import requests
from datetime import datetime
import openpyxl
import json

country_mapping = {
    "Turkey": "TR",
    "Spain": "ES",
    "Germany": "DE",
    "France": "FR",
    "United Kingdom": "GB",
    "United States": "US",
}

destination_mapping = {
    "CZ": "Prague",
    "DE": "Berlin",
    "GB": "London",
    "US": "New York",
}

def transform_row(row):
    try:
        week_com = row['WEEK COM'].isoformat() + 'Z'
        shipment_id = str(int(row['ID']))
        print(f"Transforming Shipment {shipment_id}")
        shipment = {
            'ShipmentId': shipment_id,
            'Type': 'inbound',  # using metadata: sheet_name
            'Filed': week_com,
            'Origin': {
                'City': row['ORIGIN CITY'] if pd.notna(row['ORIGIN CITY']) else None,
                'Country': row['ORIGIN'] if pd.notna(row['ORIGIN']) else None,
                'CountryCode': country_mapping.get(row['ORIGIN'])
            },
            'CustomType': row['TYPE'],  # assumption
            'Destination': {
                'City': destination_mapping.get(row['DESTINATION  (DROP LIST)']),
                'Country': next((country for country, code in country_mapping.items() if code == row['DESTINATION  (DROP LIST)']), None),
                'CountryCode': row['DESTINATION  (DROP LIST)']
            },
            # 'Carrier': { # as we do not have any carrier information like name & tracking
            #     'Service': row['MODE']
            # },
            # 'Departed': week_com,
            'Package': {
                'Mass': {
                    'Value': float(row['WEIGHT']) if pd.notna(row['WEIGHT']) else None,
                    'Unit': 'kg'  # assumption
                },
                'Volume': {
                    'Value': float(row['CBM']) if pd.notna(row['CBM']) else None,
                    'Unit': 'cm3'
                }
            },
            'TransportationModes': [{
                'LegType': 'midleg',  # assuming the package has travelled between consolidation centres or logistics facilities
                'TransportationMode': {
                    'Type': 'hgv' if (row['TYPE'] == 'FTL' and row['MODE'] == 'Road') else {'Road': 'lgv', 'Air': 'plane', 'Sea': 'cargo_ship'}.get(row['MODE'])
                }
            }],
            'LineItems': [{
                'Quantity': int(row['TOTAL UNITS']),
                'OrderId': f"Order_{shipment_id}"  # placeholder
            }],          
        }
        return shipment
    except KeyError as e:
        print(f"KeyError: Missing column {e} in row data.")
        return None
    except ValueError as e:
        print(f"ValueError: {e} in row data.")
        return None
    except Exception as e:
        print(f"Unexpected error in transform_row: {e}")
        return None

def main():
    try:
        # Load the data
        inbound_shipments_df = pd.read_excel("Customer Export for Transformation.xlsx", sheet_name='Inbound Shipments', header=1, parse_dates=['WEEK COM'])
    except Exception as e:
        print(f"Unexpected error while loading the file: {e}")
        return

    inbound_shipments_df.loc[inbound_shipments_df['DESTINATION  (DROP LIST)'] == 'UK', 'DESTINATION  (DROP LIST)'] = 'GB'
    inbound_shipments_df = inbound_shipments_df.applymap(lambda x: x.strip() if isinstance(x, str) else x) # handle unwanted spaces, if any

    shipment_json = []
    for _, row in inbound_shipments_df.iterrows():
        shipment_data = transform_row(row)
        if shipment_data:
            # response = requests.post(
            #   url='https://api.vaayu.tech/shipment/v2/account/{account_uuid}/shipment', 
            #   json=shipment_data
            # )
            # print(f"Shipment {shipment_data['ShipmentId']} uploaded. Status: {response.status_code}")
            print(f"Shipment {shipment_data['ShipmentId']} prepared. Filed date: {shipment_data['Filed']}")
            shipment_json.append(shipment_data)
        else:
            print(f"Skipping row due to transformation error.")

    try:
        with open("shipment_data.json", 'w') as file:
            json.dump(shipment_json, file, indent=4)
    except Exception as e:
        print(f"Unexpected error while writing to file: {e}")

if __name__ == '__main__':
    main()
