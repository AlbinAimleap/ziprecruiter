import logging
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)


class Config:
    LOGGER = logging.getLogger("zipRecruiter")
    SKILLS = ["aws", "python"]
    OUTPUTFILE = f"ziprecruiter_{datetime.now().strftime('%d-%m-%Y')}.csv"