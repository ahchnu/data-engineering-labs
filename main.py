import os
import zipfile
import requests
import asyncio
import aiohttp
from aiohttp import ClientResponseError

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

DOWNLOAD_DIRECTORY = 'downloads'


async def download_and_extract_async(session, uri):
    try:
        filename = os.path.join(DOWNLOAD_DIRECTORY, os.path.basename(uri))
        async with session.get(uri) as response:
            response.raise_for_status()

            content = await response.read()
            with open(filename, 'wb') as file:
                file.write(content)

        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIRECTORY)

        os.remove(filename)
        print(f"Downloaded and extracted (async): {uri}")

    except ClientResponseError as e:
        print(f"Error downloading or extracting {uri}: {e}")


async def main():
    if not os.path.exists(DOWNLOAD_DIRECTORY):
        os.makedirs(DOWNLOAD_DIRECTORY)

    async with aiohttp.ClientSession() as session:
        tasks = [download_and_extract_async(session, uri) for uri in download_uris]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())