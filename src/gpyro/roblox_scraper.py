from typing import Self, List, Dict
from errors import UnableToReachURL
from rolimons_scraper import RolimonsGamePlaceIdsType
from random import random

import requests as r
import pandas as pd
from aiohttp import ClientSession, ClientResponseError  # pyright: ignore
import asyncio
from tqdm.asyncio import tqdm_asyncio


class RobloxGameDataScraper:
    def __init__(
        self: Self,
        max_concurrent_requests: int,
        requests_per_second: int,
    ) -> None:
        self._place_id_to_universe_id_url: str = (
            "https://apis.roblox.com/universes/v1/places/{place_id}/universe"
        )
        self._game_data_url: str = (
            "https://games.roblox.com/v1/games?universeIds={universe_ids}"
        )
        self._game_votes_data_url: str = (
            "https://games.roblox.com/v1/games/votes?universeIds={universe_ids}"
        )

        self._cache: Dict = dict()
        self._universe_ids: List[str] = []

        self._max_concurrent_requests: int = max_concurrent_requests
        self._delay_between_requests: float = 1 / requests_per_second
        self._requests_semaphore: asyncio.Semaphore = asyncio.Semaphore(
            max_concurrent_requests
        )

    async def _fetch_universe_id(
        self: Self,
        place_id: int,
        session: ClientSession,
        ignore_conversion_errors: bool = False,
    ) -> int | None:
        async with self._requests_semaphore:
            sleeping_time: float = self._delay_between_requests + (random() * 0.1)
            await asyncio.sleep(sleeping_time)

            place_id_to_universe_id_url: str = self._place_id_to_universe_id_url.format(
                place_id=place_id
            )

            try:
                async with session.get(place_id_to_universe_id_url) as response:
                    json: Dict = await response.json()
                    response.raise_for_status()

                    if "universeId" not in json:
                        if ignore_conversion_errors:
                            print(
                                f"Error reaching {place_id_to_universe_id_url}. JSON Returned: {json}"
                            )
                            return
                        else:
                            raise UnableToReachURL(
                                f"Error: Unable to get universeId information from {place_id_to_universe_id_url}"
                            )

                universe_id: int = json["universeId"]
                self._universe_ids.append(str(universe_id))
            except asyncio.TimeoutError:
                print(
                    f"Timeout reaching {place_id_to_universe_id_url} for place_id {place_id}."
                )
            except ClientResponseError as e:
                if ignore_conversion_errors:
                    print(f"Unable to reach {place_id_to_universe_id_url} due to {e.status}: {e.message}")
                else:
                    raise UnableToReachURL(
                        f"Error: Unable to get universeId information from {place_id_to_universe_id_url}"
                    )

    async def get_games(
        self: Self,
        game_place_ids: RolimonsGamePlaceIdsType,
        amount: int | None = None,
        ignore_conversion_errors: bool = False,
    ) -> List[Dict]:
        if amount is None:
            amount = len(game_place_ids)
        elif not isinstance(amount, int) or amount <= 0 or amount > len(game_place_ids):
            raise ValueError(
                f"Parameter 'amount' must be a positive whole number that is less than {len(game_place_ids)}."
            )

        print("Converting Roblox place_ids to universe_ids...")

        async with ClientSession() as session:
            tasks: List = [
                self._fetch_universe_id(place_id, session, ignore_conversion_errors)
                for place_id, _ in zip(game_place_ids, range(amount))
            ]
            await tqdm_asyncio.gather(*tasks)

        if not self._universe_ids:
            print("No universe IDs were successfully retrieved. Cannot fetch game data.")
            return []

        universe_ids: str = ",".join(list(set(self._universe_ids)))
        universe_id_url: str = self._game_data_url.format(universe_ids=universe_ids)
        universe_votes_id_url: str = self._game_votes_data_url.format(
            universe_ids=universe_ids
        )

        print("Currently grabbing Roblox game data from universe ids...")
        games: Dict = r.get(universe_id_url).json()
        votes: Dict = r.get(universe_votes_id_url).json()

        if "data" not in games or "data" not in votes:
            raise UnableToReachURL("Error: Unable to get game information.")

        results: List[Dict] = []
        if len(games["data"]) != len(votes["data"]):
            print("Warning: Mismatch in number of games and votes data.")

        for game, vote in zip(games["data"], votes["data"]):
            results.append(game | vote)
        self._cache["game_data"] = results

        print("Successfully got all Roblox game data.")
        return results

    def save_game_data(self: Self, output_file_name: str) -> None:
        if "game_data" not in self._cache:
            print("Nothing to be saved!")
            return

        print("Saving all Roblox game data...!")
        df: pd.DataFrame = pd.DataFrame(self._cache["game_data"])
        df.to_parquet(output_file_name, engine="fastparquet", compression="gzip")
        print("Saved all Roblox game data successfully!")
