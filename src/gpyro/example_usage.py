from roblox_scraper import RobloxGameDataScraper
from rolimons_scraper import RolimonsScraper, RolimonsGamePlaceIdsType
import asyncio


def main() -> None:
    rolimons_scraper: RolimonsScraper = RolimonsScraper()
    game_place_ids: RolimonsGamePlaceIdsType = rolimons_scraper.get_game_place_ids()

    roblox_scraper: RobloxGameDataScraper = RobloxGameDataScraper(
        max_concurrent_requests=1,
        requests_per_second=1
    )
    asyncio.run(roblox_scraper.get_games(game_place_ids, ignore_conversion_errors=True))
    roblox_scraper.save_game_data("rolimons_6180_games_data.parquet")


if __name__ == "__main__":
    main()
