import asyncio
import logging
import gzip
import shutil
import glob
import os
from urllib.parse import urlparse
import aiohttp
import cdx_toolkit


logger = logging.getLogger(__name__)


def get_links_from_common_crawl(domain, links_limit):
	""" Метод получает мета-информацию с commoncrawl.org

	Получает информацию о ссылках на файлы для загрузки

	"""
	cdx = cdx_toolkit.CDXFetcher(source='cc')
	links_for_download = []
	url_netloc = 'https://commoncrawl.s3.amazonaws.com/'
	for k, obj in enumerate(cdx.iter(domain, limit=links_limit)):
		temp_list = []
		for item in obj.get('filename').split('/'):
			if item == 'crawldiagnostics' or item == 'robotstxt' \
			   					or item == 'warc' or item == 'wat':
				item = 'wet'
			if 'warc.gz' in item:
				item = item.replace('warc.gz', 'warc.wet.gz')
			temp_list.append(item)
		url_path = '/'.join(temp_list)
		links_for_download.append(url_netloc + url_path)
	if not links_for_download:
		logger.info('links_for_download is empty')
		return None
	return links_for_download


async def write_file(url, session):
	""" Асинхронный метод

	Скачивает wet.gz-файлы и записывает на диск в рабочей директории

	"""
	timeout = aiohttp.ClientTimeout(total=10*60*60)
	async with session.get(url, timeout=timeout) as response:
		filename = urlparse(url).path.split('/')[-1]
		with open(filename, 'wb') as file:
			logger.info('Downloading file {}...'.format(filename))
			while True:
				chunk = await response.content.read(10)
				if not chunk:
					logger.info('File {} downloaded'.format(filename))
					break
				file.write(chunk)


async def download_gz_files(links_for_download):
	""" Асинхронный метод

	Запускает процесс загрузки и записи файлов с commoncrawl.org

	"""
	tasks = []
	async with aiohttp.ClientSession() as session:
		for url in links_for_download:
			task = asyncio.create_task(write_file(url, session))
			tasks.append(task)
		await asyncio.gather(*tasks)


def gunzip_files():
	""" Метод распаковывает скачанные wet.gz-файлы в рабочую директорию"""
	gz_files = glob.glob('*.gz')
	for count, gz_file in enumerate(gz_files):
		with gzip.open(gz_file, 'rb') as source_file:
			dest_file_name = gz_file.replace('.gz', '')
			with open(dest_file_name, 'wb') as destination_file:
				shutil.copyfileobj(source_file, destination_file)
				logger.info('File {} unziped'.format(gz_file))
		os.remove(gz_file)
		logger.info('File {} removed'.format(gz_file))
