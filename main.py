import tls_client
import aiohttp
import httpx
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlencode
import random
import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
import csv
import os

from config import Config

logger = Config.LOGGER


class JobScraper:
    """Base class for job scrapers"""
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        }

    async def fetch(self, url: str) -> str:
        """Abstract method for fetching data"""
        raise NotImplementedError

    def soup_response(self, response: str) -> BeautifulSoup:
        """Convert HTML response to BeautifulSoup object."""
        return BeautifulSoup(response, 'lxml')

class HTTPClient:
    """HTTP client wrapper for different libraries"""
    def __init__(self):
        self.session = tls_client.Session(client_identifier="chrome_108")
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        }

    async def fetch_tls(self, url: str) -> str:
        try:
            response = self.session.get(url, headers=self.headers, allow_redirects=True)
            return response.text
        except Exception as e:
            logger.error(f"TLS Client error: {str(e)}")
            return None

    async def fetch_aiohttp(self, url: str) -> str:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, allow_redirects=True) as response:
                    return await response.text()
        except Exception as e:
            logger.error(f"Aiohttp error: {str(e)}")
            return None

    async def fetch_httpx(self, url: str) -> str:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=self.headers, follow_redirects=True)
                return response.text
        except Exception as e:
            logger.error(f"Httpx error: {str(e)}")
            return None

    async def fetch(self, url: str) -> str:
        """Try different libraries to fetch the page"""
        logger.info(f"Fetching URL: {url.split('/')[-1]}")
        
        for fetch_method in [self.fetch_tls, self.fetch_aiohttp, self.fetch_httpx]:
            try:
                content = await fetch_method(url)
                if content:
                    logger.info(f"Fetched URL: {url.split('/')[-1]}")
                    return content
            except Exception as e:
                logger.error(f"Fetch method failed: {str(e)}")
                continue
            
        raise Exception("All fetch methods failed")

class CSVWriter:
    """Handle CSV file operations"""
    def __init__(self, filename: str, fieldnames: List[str]):
        self.filename = filename
        self.fieldnames = fieldnames
        self.initialize_file()

    def initialize_file(self):
        """Initialize CSV file with headers if it doesn't exist."""
        if not os.path.exists(self.filename):
            with open(self.filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()

    def save_row(self, data: Dict[str, Any]):
        """Save data row to CSV file."""
        with open(self.filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writerow(data)

class ZipRecruiterScraper(JobScraper):
    """A class to scrape job listings from ZipRecruiter."""
    
    def __init__(self, base_url: str = 'https://www.ziprecruiter.in', output_filename: str = 'ziprecruiter_job.csv') -> None:
        super().__init__(base_url)
        self.job_links: List[str] = []
        self.http_client = HTTPClient()
        self.output_filename = output_filename
        self.csv_writer = CSVWriter(self.output_filename, [
            'Domain', 'PostUrl', 'JobID', 'Title', 'City', 'State',
            'Speciality', 'JobType', 'JobDetails', 'Industry', 'Company',
            'PostedOn', 'SalaryFrom', 'SalaryUpto', 'PayoutTerm',
            'IsEstimatedSalary', 'ScrapedOn'
        ])

    async def fetch(self, url: str) -> str:
        return await self.http_client.fetch(url)

    async def get_page(self, page_number: int, query: str) -> str:
        """Get specific page of job listings."""
        params = {
            'page': page_number,
            'q': query
        }
        url = f'{self.base_url}/jobs/search?{urlencode(params)}'
        return await self.fetch(url)

    async def get_details(self, url: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a job posting."""
        logger.info(f"Getting details for job: {url.split('/')[-1]}")
        response = await self.fetch(url)
        
        try:
            soup = self.soup_response(response)
            json_data = soup.find('script', attrs={'type': 'application/ld+json'})
            reference = soup.find('div', class_='job-posting-reference')
            if reference:
                job_id = reference.text.replace("Reference:", "").strip()
            else:
                job_id = ''
            
            if json_data:
                data = json.loads(json_data.text)
                description = self.soup_response(data['description']).text.strip()
                
                return {
                    'Domain': 'ZipRecruiter',
                    'PostUrl': url,
                    'JobID': '',
                    'Title': data['title'],
                    'City': data['jobLocation']['address']['addressLocality'],
                    'State': data['jobLocation']['address']['addressRegion'],
                    'Speciality': '',
                    'JobType': data['employmentType'],
                    'JobDetails': description,
                    'Industry': '',
                    'Company': data['hiringOrganization']['name'],
                    'PostedOn': data['datePosted'],
                    'SalaryFrom': '',
                    'SalaryUpto': '',
                    'PayoutTerm': '',
                    'IsEstimatedSalary': False,
                    'ScrapedOn': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
        except Exception as e:
            logger.error(f"Error getting job details for {url.split('/')[-1]}: {str(e)}")
            return None

    async def load_links(self, query: str) -> List[str]:
        """Load all job links by traversing through pages."""
        logger.info("Starting to load job links")
        
        async def get_links(page_number: int = 1) -> None:
            try:
                response = await self.get_page(page_number, query)
                if not response:
                    logger.error(f"No response received for page {page_number}")
                    return
                
                soup = self.soup_response(response)                
                next_page_link = soup.find('a', attrs={'rel': 'next'})
                links = soup.find_all('a', attrs={'class': 'jobList-title'})
                
                if links:
                    self.job_links.extend([i['href'] for i in links])
                    logger.info(f"Found {len(links)} links on page {page_number}")
                else:
                    logger.warning(f"No links found on page {page_number}")
                    
                if next_page_link:
                    next_page = int(next_page_link.text)
                    await get_links(next_page)
                else:
                    logger.info("No more pages to process")
                    
            except Exception as e:
                logger.error(f"Error parsing page {page_number}: {str(e)}")
                if response:
                    logger.error(f"Response content: {response[:500]}")  # Log first 500 chars
        
        await get_links()
        logger.info(f"Total links found: {len(self.job_links)}")
        return self.job_links

    async def process_link(self, link: str):
        """Process a single job link"""
        details = await self.get_details(link)
        if details:
            logger.info(f"Successfully scraped job: {details['Title']}")
            self.csv_writer.save_row(details)
        else:
            logger.warning(f"No data found for link: {link}")

    async def run(self) -> None:
        """Main execution method to scrape all jobs."""
        logger.info("Starting scraping process")
        queries = Config.SKILLS
        for query in queries:
            try:
                links = await self.load_links(query)
                if not links:
                    logger.error(f"No links were found to process for {query}")
                    continue
                    
                logger.info(f"Processing {len(links)} links for {query}")
                tasks = [self.process_link(link) for link in links]
                await asyncio.gather(*tasks)
            except Exception as e:
                logger.error(f"Error in run method for {query}: {str(e)}")
       

async def main():
    try:
        scraper = ZipRecruiterScraper(output_filename=Config.OUTPUTFILE)
        await scraper.run()
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")

if __name__ == '__main__':
    asyncio.run(main())