from typing import Dict, List, Self, NewType, NamedTuple
from enum import Enum
from random import sample
import requests as r
import pandas as pd

RolimonsGameMetadata = NamedTuple(
    "RolimonsGameMetadata", [
        ("name", str),
        ("active_players", int),
        ("thumbnail_download_url", str)
    ]
)
RolimonsGameInfoType = NewType("RolimonsGameInfoType", Dict[int, RolimonsGameMetadata])

class RolimonsAccessTypeOptions(Enum):
    SEQUENTIAL = 1,
    RANDOM = 2

class RolimonsScraper:
    def __init__(self: Self) -> None:
        self._rolimons_api_url: str = "https://api.rolimons.com/games/v1/gamelist"

        self.data: Dict = r.get(self._rolimons_api_url).json()

        if "game_count" not in self.data:
            raise ValueError("Error: Unable to get the game information (specifically, game data information).")

        self.amount_of_games: int = self.data["game_count"]

        self._game_place_ids: List[int] = []
        for place_id in self.data["games"]:
            self._game_place_ids.append(place_id)

    def get_games(self: Self, amount: int, access_type: RolimonsAccessTypeOptions) -> RolimonsGameInfoType:
        if (
            type(amount) != type(int())
            or (amount <= 0 or amount > self.amount_of_games)
        ):
            raise ValueError(f"Parameter 'amount' must be a positive whole number that is less than {self.amount_of_games}.")

        results: RolimonsGameInfoType = dict() #pyright: ignore

        match access_type:
            case RolimonsAccessTypeOptions.SEQUENTIAL:
                place_ids: List[int] = self._game_place_ids[:amount]
            case RolimonsAccessTypeOptions.RANDOM:
                place_ids: List[int] = sample(self._game_place_ids, k=amount)

        for place_id in place_ids:
            entry: List[int | str] = self.data["games"][place_id]
            results[place_id] = RolimonsGameMetadata(entry[0], entry[1], entry[2]) #pyright: ignore
        return results

    def save_all_game_data(self: Self, output_file_name: str) -> None:
        df = pd.DataFrame(self.data)
        df.to_parquet(output_file_name, compression="gzip")
