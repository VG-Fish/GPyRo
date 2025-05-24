from roblox_scraper import RobloxGameDataScraper
from rolimons_scraper import RolimonsScraper, RolimonsGamePlaceIdsType

def main() -> None:
    rolimons_scraper: RolimonsScraper = RolimonsScraper()
    game_place_ids: RolimonsGamePlaceIdsType = rolimons_scraper.get_game_place_ids()

    roblox_scraper: RobloxGameDataScraper = RobloxGameDataScraper()
    roblox_scraper.get_games(game_place_ids)
    roblox_scraper.save_game_data("rolimons_6180_games_data.parquet")

if __name__ == "__main__":
    main()
