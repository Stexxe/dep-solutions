import asyncio
import os
import urllib.parse
import aiohttp
import re
import zipfile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]
STORE_DIR = 'downloads'


async def main():
    if not os.path.exists(STORE_DIR):
        os.makedirs(STORE_DIR)

    async with aiohttp.ClientSession() as session:
        for uri in download_uris:
            asyncio.create_task(download_and_extract(session, uri))
        await asyncio.gather(*asyncio.all_tasks() - {asyncio.current_task()})


async def download_and_extract(session, uri):
    url = urllib.parse.urlparse(uri)
    filename = re.sub(r'^/', '', url.path)
    print(f'Downloading {uri}')
    async with session.get(uri) as resp:
        if resp.status != 200:
            return
        zip_file = os.path.join(STORE_DIR, filename)
        with open(zip_file, mode="wb") as file:
            file.write(await resp.read())

        try:
            print(f'Unzipping {zip_file}')
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                for path in zip_ref.namelist():
                    filename = os.path.basename(path)
                    if not filename.startswith('.') and filename.endswith('.csv'):
                        zip_ref.extract(path, STORE_DIR)
        except zipfile.BadZipFile:
            pass
        finally:
            os.remove(zip_file)


if __name__ == "__main__":
    asyncio.run(main())
