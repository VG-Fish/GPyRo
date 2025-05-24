from typing import Self, List, Dict
from errors import UnableToReachURL
from rolimons_scraper import RolimonsGamePlaceIdsType
import requests as r

class RobloxGameDataScraper:
    def __init__(self: Self) -> None:
        self._place_id_to_universe_id_url: str = "https://apis.roblox.com/universes/v1/places/{place_id}/universe"
        self._game_data_url: str = "https://games.roblox.com/v1/games?universeIds={universe_ids}"
        self._game_votes_data_url: str = "https://games.roblox.com/v1/games/votes?universeIds={universe_ids}"

    def get_games(self: Self, game_place_ids: RolimonsGamePlaceIdsType, amount: int) -> List[Dict]:
        if (
            type(amount) != type(int())
            or (amount <= 0 or amount > len(game_place_ids))
        ):
            raise ValueError(f"Parameter 'amount' must be a positive whole number that is less than {len(game_place_ids)}.")

        universe_id_urls: List[str] = []
        for place_id, _ in zip(game_place_ids, range(amount)):
            place_id_to_universe_id_url: str = self._place_id_to_universe_id_url.format(place_id=place_id)
            json = r.get(place_id_to_universe_id_url).json()

            if "universeId" not in json:
                raise UnableToReachURL("Error: Unable to get universeId information.")

            universe_id: int = json["universeId"]
            universe_id_urls.append(str(universe_id))

        universe_ids: str = ",".join(universe_id_urls)
        universe_id_url: str = self._game_data_url.format(universe_ids=universe_ids)
        universe_votes_id_url: str = self._game_votes_data_url.format(universe_ids=universe_ids)

        games = r.get(universe_id_url).json()
        votes = r.get(universe_votes_id_url).json()

        if "data" not in games:
            raise UnableToReachURL("Error: Unable to get game information.")

        results: List[Dict] = []
        for game_data, vote_data in zip(games, votes):
            results.append(game_data | vote_data)
        return results
