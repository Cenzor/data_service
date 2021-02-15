import logging
import os
from typing import List
from find import FindData, FindPredictions
from fastapi import FastAPI, Query
from pydantic import BaseModel


app = FastAPI()

logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)
links_limit = os.getenv('LINKS_LIMIT')
print(links_limit)
domain_regex = '^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$'


class FindPredictionsItem(BaseModel):

	"""	Класс описывает данные, принимаемые по запросу /find_predictions/ """

	domain: List[str] = Query(None, regex=domain_regex)


class FindDataItem(BaseModel):

	""" Класс описывает данные, принимаемые по запросу /find_data/  """

	domain: List[str] = Query(None, regex=domain_regex)


@app.post("/find_predictions/")
async def find_predictions(item: FindPredictionsItem):
	""" Метод принимает запрос от клиента в виде доменного имени.
	В качестве возвращаемого значения - json с результатом из БД
	
	"""
	logger.info("Request /find_predictions/ received")
	fp = FindPredictions(item.domain)
	response = fp.get_data_from_db()
	logger.info("Response sent")
	return response


@app.post("/find_data/")
async def find_data(item: FindDataItem):
	"""Метод принимает запрос от клиента в виде доменного имени.
	В качестве возврщаемого значения - json с результатом из БД

	"""
	logger.info("Request /find_data/ received")
	fd = FindData(item.domain, links_limit)
	response = await fd.get_data_from_db()
	logger.info("Response sent")
	return response

