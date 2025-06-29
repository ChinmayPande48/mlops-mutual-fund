import asyncio
import aiohttp
import json

async def get_scheme_codes_async(api_url):
    """
    Asynchronously fetches scheme codes from a given API URL.

    Args:
        api_url (str): The URL of the API to fetch data from.

    Returns:
        list: A list of scheme codes, or None if an error occurred.
    """
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(api_url) as response:
                response.raise_for_status()  # Raises an aiohttp.ClientResponseError for bad responses (4xx or 5xx)
                data = await response.json()

                scheme_codes = []
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "schemeCode" in item:
                            scheme_codes.append(item["schemeCode"])
                return scheme_codes

        except aiohttp.ClientError as e:
            print(f"Error fetching data from API: {e}")
            return None
        except json.JSONDecodeError:
            print(f"Error decoding JSON response")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

# Example usage (how you would run this asynchronous function)
async def main():
    api_url = "https://api.mfapi.in/mf" # Replace with your actual API URL
    scheme_codes = await get_scheme_codes_async(api_url)
    if scheme_codes:
        print("Scheme Codes:", scheme_codes)

if __name__ == "__main__":
    asyncio.run(main())