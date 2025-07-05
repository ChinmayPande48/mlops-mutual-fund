import asyncio
import aiohttp
import os
import json
import pandas as pd
import time
from extract_scheme_codes import get_scheme_codes_async 

OUTPUT_DIR = "D:\data"

async def get_schemes_info():
    """
    This is an asynchronous function that fetches scheme codes and then processes them.
    """
    print("Fetching scheme codes...")
    scheme_url = "https://api.mfapi.in/mf"
    
    # Await the async function to get the scheme codes
    # THIS IS THE CRITICAL LINE TO ENSURE 'await' IS USED
    scheme_codes = await get_scheme_codes_async(scheme_url)
    
    # Create the output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    timeout = aiohttp.ClientTimeout(total=60) # Increased to 60 seconds for robustness

    CONCURRENT_REQUEST_LIMIT = 50 # Start with 50, adjust up or down
    semaphore = asyncio.Semaphore(CONCURRENT_REQUEST_LIMIT)
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Using a list of tasks to fetch data concurrently for all schemes
        tasks = []
        for code in scheme_codes:
            # Create a task for each scheme, but don't await immediately
            tasks.append(fetch_and_save_scheme_data(session, code,semaphore))
        
        # Run all tasks concurrently and wait for them to complete
        await asyncio.gather(*tasks)
        print("All scheme data fetched and saved.")

async def fetch_and_save_scheme_data(session, code,semaphore):
    """
    Fetches NAV data for a single scheme code and saves it to a CSV.
    """
    nav_list = []
    try:
        async with semaphore:

            async with session.get(f'https://api.mfapi.in/mf/{code}') as response:
                response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
                data = await response.json()

                # Ensure 'data' key exists and its value is a list
                if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
                    #if isinstance(data["meta"],dict) and "scheme_code" in data["meta"]:
                        #if data["meta"]["scheme_code"] == code:
                    for item in data["data"]: # Iterate over data["data"] as it's a list of dicts
                        if "date" in item and "nav" in item:
                            nav_tuple = (item["date"], item["nav"])
                            nav_list.append(nav_tuple)
                    
                    if nav_list: # Only create DataFrame and save if there is data
                        columns = ['date', 'nav']
                        df = pd.DataFrame(nav_list, columns=columns)

                        filename = f"{code}.csv"
                        filepath = os.path.join(OUTPUT_DIR, filename)
                        
                        df.to_csv(filepath, index=False) # index=False to avoid writing DataFrame index
                        
                        # print(f"Saved data for scheme {code} to {filepath}") # Uncomment for verbose logging
                    else:
                        print(f"No NAV data found for scheme {code}")
                else:
                    print(f"Unexpected data format for scheme {code}: {json.dumps(data)}") # Print problematic data
                    # Consider logging this as a warning or error to understand API response issues
    except aiohttp.ClientError as e:
        print(f"HTTP error fetching data for scheme {code}: {e}")
    except json.JSONDecodeError:
        print(f"JSON decode error for scheme {code} (Response might not be valid JSON)")
    except Exception as e:
        print(f"An unexpected error occurred for scheme {code}: {type(e).__name__}: {e}")
        #print(f"An unexpected error occurred for scheme {code}: {e}")
            
if __name__ == "__main__":
    # This is the ONLY place asyncio.run() should be called.
    start_time = time.time()
    asyncio.run(get_schemes_info())
    end_time= time.time()

    print(f'Total time to extract: {end_time-start_time}')

    #print(f'Total schemes extracted: {count}')