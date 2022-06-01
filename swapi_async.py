import asyncio
import aiohttp
import time
from more_itertools import chunked

import migrate
from migrate import Base, engine

URL = 'https://swapi.dev/api/'

MAX = 100
PART = 10
SLEEP_TIME = 1


async def health_check():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(URL) as response:
                    if response.status == 200:
                        print('OK')
                    else:
                        print(response.status)
            except Exception as er:
                print(er)
            await asyncio.sleep(SLEEP_TIME)


async def migrate_db():
    async with aiohttp.ClientSession() as session:
        Base.metadata.create_all(engine)


async def get_person(person_id, session):
    async with session.get(f'{URL}people/{person_id}') as response:
        return await response.json()


async def get_people(all_ids, part, session):
    for chunked_ids in chunked(all_ids, PART):
        yield await asyncio.gather(*[get_person(person_id, session) for person_id in chunked_ids])


async def main():
    start = time.time()
    health_check_task = asyncio.create_task(health_check())
    print(health_check_task)
    migrate_db_task = asyncio.create_task(migrate_db())
    print(migrate_db_task)
    async with aiohttp.ClientSession() as session:
        async for people in get_people(range(1, MAX + 1), PART, session):
            with migrate.Session() as session_db:
                for p in people:
                    if p.get('detail') is None:
                        new_adv = migrate.StartWars(birth_year=p['birth_year'],
                                                    eye_color=p['eye_color'],
                                                    films=p['films'],
                                                    gender=p['gender'],
                                                    hair_color=p['hair_color'],
                                                    height=p['height'],
                                                    homeworld=p['homeworld'],
                                                    mass=p['mass'],
                                                    name=p['name'],
                                                    skin_color=p['skin_color'],
                                                    species =p['species'],
                                                    starships =p['starships'],
                                                    vehicles=p['vehicles'])
                        session_db.add(new_adv)
                        session_db.commit()
    print(time.time() - start)

asyncio.run(main())


# start = time.time()
# asyncio.run(main())
# print(time.time() - start)
