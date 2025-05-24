from typing import Self, List, Dict
from errors import UnableToReachURL
from rolimons_scraper import RolimonsGamePlaceIdsType
from random import random

import requests as r
import pandas as pd
from aiohttp import ClientSession #pyright: ignore
from asyncio import sleep
from tqdm.asyncio import tqdm_asyncio

class RobloxGameDataScraper:
    def __init__(self: Self) -> None:
        self._place_id_to_universe_id_url: str = "https://apis.roblox.com/universes/v1/places/{place_id}/universe"
        self._game_data_url: str = "https://games.roblox.com/v1/games?universeIds={universe_ids}"
        self._game_votes_data_url: str = "https://games.roblox.com/v1/games/votes?universeIds={universe_ids}"
        self._cache: Dict = dict()
        self._universe_ids: List[str] = []

    async def _fetch_universe_id(self: Self, place_id: int, session: ClientSession, ignore_conversion_errors: bool = False) -> int | None:
        sleeping_time: float = random()
        await sleep(sleeping_time)

        place_id_to_universe_id_url: str = self._place_id_to_universe_id_url.format(place_id=place_id)

        async with session.get(place_id_to_universe_id_url) as response:
            json = await response.json()

            if "universeId" not in json:
                if ignore_conversion_errors:
                    print(f"Error reaching {place_id_to_universe_id_url}")
                    return
                else:
                    raise UnableToReachURL("Error: Unable to get universeId information.")

        universe_id: int = json["universeId"]
        self._universe_ids.append(str(universe_id))

    async def get_games(
        self: Self,
        game_place_ids: RolimonsGamePlaceIdsType,
        amount: int | None = None,
        ignore_conversion_errors: bool = False
    ) -> List[Dict]:
        if amount is None:
            amount = len(game_place_ids)
        elif (
            type(amount) != type(int())
            or (amount <= 0 or amount > len(game_place_ids))
        ):
            raise ValueError(f"Parameter 'amount' must be a positive whole number that is less than {len(game_place_ids)}.")

        print("Converting Roblox place_ids to universe_ids...")

        async with ClientSession() as session:
            tasks: List = [
                self._fetch_universe_id(place_id, session, ignore_conversion_errors)
                for place_id, _ in zip(game_place_ids, range(amount))
            ]
            await tqdm_asyncio.gather(*tasks)

        universe_ids: str = ",".join(self._universe_ids)
        universe_id_url: str = self._game_data_url.format(universe_ids=universe_ids)
        universe_votes_id_url: str = self._game_votes_data_url.format(universe_ids=universe_ids)

        print("Currently grabbing Roblox game data from universe ids...")
        games: Dict = r.get(universe_id_url).json()
        votes: Dict = r.get(universe_votes_id_url).json()

        if  "data" not in games or "data" not in votes:
            raise UnableToReachURL("Error: Unable to get game information.")

        results: List[Dict] = []
        for game, vote in zip(games["data"], votes["data"]):
            results.append(game | vote)
        self._cache["game_data"] = results

        print("Successfully got all Roblox game data.")
        return results

    def save_game_data(self: Self, output_file_name: str) -> None:
        if "game_data" not in self._cache:
            print("Nothing to be saved!")

        print("Saving all Roblox game data...!")
        df: pd.DataFrame = pd.DataFrame(self._cache["game_data"])
        df.to_parquet(output_file_name, engine="fastparquet", compression="gzip")
        print("Saved all Roblox game data successfully!")
