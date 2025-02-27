import json
import logging

def process_raw_exchange_rates(raw_data):
    """
        Process and transforms raw_data and insert into processed_exchange_rates
    """
    logging.basicConfig(level=logging.INFO)
    transformed_data=[]
    for res_json, retrieved_at in raw_data:
        try:            
            data = json.loads(res_json)
            if not data.get("success"):
                logging.warning(f"Error API response: {data}")
                continue

            base_curr = data.get("source", "USD")
            quotes = data.get("quotes", {})

            if not base_curr or not quotes:
                logging.warning(f"Missing data at API response: {data}")
                continue

            date = retrieved_at.date()

            for target_curr, rate in quotes.items():
                target_curr = target_curr.replace(base_curr, "")

                if rate is None or rate <= 0:
                    logging.warning(f"Invalid rate found for {base_curr} -> {target_curr}: {rate}")
                    continue
                
            transformed_data.append((date, base_curr, target_curr, rate, retrieved_at)) 

        except json.JSONDecodeError as e:
            logging.error(f"Error with JSON API response: {e}")
            continue
    return transformed_data
        
