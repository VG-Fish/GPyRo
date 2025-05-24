from errors import UnableToReachURL
from typing import Dict, List, Self, NewType, NamedTuple
from enum import Enum
from random import sample
import requests as r
import pandas as pd

RolimonsGameMetadata = NamedTuple(
    "RolimonsGameMetadata",
    [("name", str), ("active_players", int), ("thumbnail_download_url", str)],
)
RolimonsGameInfoType = NewType("RolimonsGameInfoType", Dict[int, RolimonsGameMetadata])
RolimonsGamePlaceIdsType = NewType("RolimonsGamePlaceIdsType", List[int])


class RolimonsAccessTypeOptions(Enum):
    SEQUENTIAL = 1
    RANDOM = 2


class RolimonsScraper:
    def __init__(self: Self) -> None:
        self._rolimons_api_url: str = "https://api.rolimons.com/games/v1/gamelist"

        print("Getting all of Rolimons' game data...")
        self._data: Dict = r.get(self._rolimons_api_url).json()

        if "game_count" not in self._data:
            raise UnableToReachURL(
                "Error: Unable to get the game information (specifically, game data information)."
            )

        self.amount_of_games: int = self._data["game_count"]

        self._game_place_ids: RolimonsGamePlaceIdsType = RolimonsGamePlaceIdsType([])
        for place_id in self._data["games"]:
            self._game_place_ids.append(place_id)
        print("Successfully got all of Rolimons' game data!")

    def get_games(
        self: Self, amount: int, access_type: RolimonsAccessTypeOptions
    ) -> RolimonsGameInfoType:
        if not isinstance(amount, int) or (
            amount <= 0 or amount > self.amount_of_games
        ):
            raise ValueError(
                f"Parameter 'amount' must be a positive whole number that is less than {self.amount_of_games}."
            )

        results: RolimonsGameInfoType = RolimonsGameInfoType(dict())

        match access_type:
            case RolimonsAccessTypeOptions.SEQUENTIAL:
                place_ids: List[int] = self._game_place_ids[:amount]
            case RolimonsAccessTypeOptions.RANDOM:
                place_ids: List[int] = sample(self._game_place_ids, k=amount)

        for place_id in place_ids:
            entry: List[int | str] = self._data["games"][place_id]
            results[place_id] = RolimonsGameMetadata(entry[0], entry[1], entry[2])  # pyright: ignore

        print("Returning all of Rolimons' game data!")
        return results

    def save_all_game_data(self: Self, output_file_name: str) -> None:
        print("Saving all Rolimons' game data...!")
        df: pd.DataFrame = pd.DataFrame(self._data)
        df.to_parquet(output_file_name, engine="fastparquet", compression="gzip")
        print("Saved all Rolimons' game data successfully!")

    def get_game_data(self: Self) -> Dict:
        return self._data

    def get_game_place_ids(self: Self) -> RolimonsGamePlaceIdsType:
        return self._game_place_ids
