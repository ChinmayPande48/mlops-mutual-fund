import asyncio
import aiohttp
import os
import json
import pandas as pd
import time
from extract_scheme_code import get_scheme_codes_async # Assuming this is correctly implemented

OUTPUT_DIR = r"D:\scheme_data"
ALL_SCHEMES_CSV = os.path.join(OUTPUT_DIR, "all_schemes_data.csv")

async def get_schemes_info():
    """
    This is an asynchronous function that fetches scheme codes and then processes them
    to collect all data into a single DataFrame.
    """
    print("Fetching scheme codes...")   
    scheme_url = "https://api.mfapi.in/mf"
    
    # Await the async function to get the scheme codes
    scheme_codes = await get_scheme_codes_async(scheme_url)
    print(f'Total scheme codes to process: {len(scheme_codes)}')
    
    # Create the output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    timeout = aiohttp.ClientTimeout(total=120) # Increased timeout for potentially large number of requests
    CONCURRENT_REQUEST_LIMIT = 50 # Adjust as needed based on API limits and network
    semaphore = asyncio.Semaphore(CONCURRENT_REQUEST_LIMIT)
    
    all_scheme_data = [] # List to store dictionaries of all scheme data

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for code in scheme_codes:
            tasks.append(fetch_scheme_data_for_consolidation(session, code, semaphore, all_scheme_data))
        
        # Run all tasks concurrently and wait for them to complete
        await asyncio.gather(*tasks)
        print("All scheme data fetched.")
    
    if all_scheme_data:
        print(f"Consolidating {len(all_scheme_data)} records into a single DataFrame...")
        # Convert the list of dictionaries to a DataFrame
        final_df = pd.DataFrame(all_scheme_data)
        
        # Ensure scheme_code is numeric if possible, convert date
        if 'date' in final_df.columns:
            final_df['date'] = pd.to_datetime(final_df['date'], errors='coerce')
        if 'nav' in final_df.columns:
            final_df['nav'] = pd.to_numeric(final_df['nav'], errors='coerce')
        if 'scheme_code' in final_df.columns:
            final_df['scheme_code'] = pd.to_numeric(final_df['scheme_code'], errors='coerce')

        unique_scheme_types = final_df['scheme_type'].unique()

        if unique_scheme_types is not None:
            for s_type in unique_scheme_types:
                if pd.isna(s_type): # Handle potential NaN scheme_type if data is messy
                    filename_suffix = "unknown_type"
                else:
                    # Sanitize scheme_type for filename (replace spaces, special chars)
                    filename_suffix = s_type.replace(' ', '_').replace('/', '_').replace('-', '_').replace(';', '_').lower()
                
                filepath = os.path.join(OUTPUT_DIR, f"{filename_suffix}.csv")
                
                type_df = final_df[final_df['scheme_type'] == s_type]
                
                print(f"Saving {len(type_df)} records for scheme type '{s_type}' to {filepath}")
                type_df.to_csv(filepath, index=False)
            print("All scheme type data saved successfully.")
        else:
            print("No valid scheme types found to save.")
    """        
    else:
        print("No data (existing or new) to process or save.")


        # Sort by scheme_code and date for better organization
        final_df = final_df.sort_values(by=['scheme_code', 'date']).reset_index(drop=True)

        print(f"Saving combined data to {ALL_SCHEMES_CSV}")
        final_df.to_csv(ALL_SCHEMES_CSV, index=False)
        print("Data saved successfully.")
    #else:
        #print("No data was fetched to save.")
"""
async def fetch_scheme_data_for_consolidation(session, code, semaphore, all_scheme_data_list):
    """
    Fetches NAV and meta data for a single scheme code and appends it to a shared list.
    """
    try:
        async with semaphore:
            async with session.get(f'https://api.mfapi.in/mf/{code}') as response:
                response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
                data = await response.json()

                if isinstance(data, dict) and "meta" in data and isinstance(data["meta"], dict) and \
                   "data" in data and isinstance(data["data"], list):
                    
                    meta_info = data["meta"]
                    
                    # Iterate through each NAV entry and combine with meta info
                    for nav_entry in data["data"]:
                        if "date" in nav_entry and "nav" in nav_entry:
                            record = {
                                "scheme_code": meta_info.get("scheme_code"),
                                "date": nav_entry["date"],
                                "nav": nav_entry["nav"],
                                "scheme_type": meta_info.get("scheme_type")
                            }
                            all_scheme_data_list.append(record)
                else:
                    print(f"Unexpected data format for scheme {code}: {json.dumps(data)}")
    except aiohttp.ClientError as e:
        print(f"HTTP error fetching data for scheme {code}: {e}")
    except json.JSONDecodeError:
        print(f"JSON decode error for scheme {code} (Response might not be valid JSON)")
    except Exception as e:
        print(f"An unexpected error occurred for scheme {code}: {type(e).__name__}: {e}")
            
if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(get_schemes_info())
    end_time = time.time()

    print(f'Total time to extract and save all data: {end_time-start_time:.2f} seconds')