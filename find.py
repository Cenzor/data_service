import logging
import os
import asyncio
import psycopg2
from crawler import (get_links_from_common_crawl,
					 download_gz_files, gunzip_files)
from prepare_write_data import prepare_write_data


logger = logging.getLogger(__name__)


class Find():
	""" Базовый класс для наследования, реализует работу с БД. """

	def __init__(self, domains):
		""" Конструктор.
	
		Инициализирует переменные экземпляра класса для подключения к БД,
		а так же переменную доменного имени для запроса к БД.

		"""
		self.domain = domains[0]
		self.hostname = os.getenv('DB_HOST')
		self.port = os.getenv('DB_PORT')
		self.username = os.getenv('DB_USERNAME')
		self.password = os.getenv('DB_PASSWORD')
		self.database = os.getenv('DB_NAME')
		self.not_found = {"domain": self.domain, "status": "Not Found"}
		self.result = []

	def get_data_from_db(self):
		""" 

		Метод устанавливает соединение с БД,
		создаёт курсор для выполнения запросов к БД
	
		"""
		self.conn = psycopg2.connect(
			host=self.hostname,
			port=self.port,
			user=self.username,
			password=self.password,
			dbname=self.database
		)
		logger.debug('Connection established')
		self.cursor = self.conn.cursor()
		logger.debug('Cursor received')

	def query_database(self, sql_statement, *args):
		"""

		Метод выполняет запрос к БД,
		возвращает курсор для работы с данными

		"""
		self.cursor.execute(sql_statement, *args)
		logger.debug('Database query completed')
		return self.cursor

	def commit_query(self):
		""" Метод фиксирует выполненную транзакцию в БД """
		return self.cursor.commit()

	def get_result(self, rows):
		""" Метода возвращает список, элементы которого записи из таблицы """
		return self.result

	def __del__(self):
		""" Деструктор.
		
		Закрывает курсор и соединение с БД
		
		"""
		self.cursor.close()
		logger.debug('Cursor closed')
		self.conn.close()
		logger.debug('Connection closed')


class FindPredictions(Find):
	""" Класс наследуется от базового класс для работы с БД.

	Обрабатывает запросы /find_predictions/.
	По доменному имени из запроса ищет информацию в таблице 'domain_preds'.
	Возвращает найденный результат, в противном случае Not Found

	"""
	def __init__(self, domains):
		""" Override Конструктор.

		Запускает конструктор базового (наслудуемого) класса.
		Инициализирует логгер для экземпляра класса.

		"""
		super().__init__(domains)
		self.logger = logging.getLogger(__name__ + '.FindPredictions ')
		self.logger.debug('The instance of FindPredictions created.')

	def get_data_from_db(self):
		""" Override

		Метод получает информацию с БД по запрашиваемому доменному имени.
		В качетве возвращаемого значения список, элементами которого
		являются записи таблицы в виде словаря поле->значение.

		"""
		super().get_data_from_db()
		sql_statement = "SELECT predictions, domain FROM domain_preds WHERE " \
						"domain LIKE '%" + self.domain + "' LIMIT 200"
		cursor = self.query_database(sql_statement)
		rows = cursor.fetchall()
		return self.get_result(rows)

	def get_result(self, rows):
		"""Override
		
		Метод получает список записей из БД.
		В качестве возвращаемого значения - список, элементами которого
		являются записи таблицы в виде словаря поле->значение

		"""
		super().get_result(rows)
		if rows:
			for row in rows:
				self.result.append({
					"domain": row[1],
					"predictions": row[0],
				})
			self.result.append({"status": "OK"})
		else:
			self.result.append(self.not_found)
		return self.result

	def __del__(self):
		""" Override Деструктор"""
		super().__del__()
		self.logger.debug('The instance of FindPredictions deleted')


class FindData(Find):
	""" Класс наследуется от базового класса лдя работы с БД.

	Обрабатывает запросы /find_data/.
	По доменному имени из запроса ищет информацию в таблице 'url'.
	Возвращает найденный результат, иначе
		делает поиск по доменному имени в commoncrawl.org,
		если поиск не дал результатов - возвращает Not Found.
		если поиск дал результат
			загружает wet-файлы, конвертирует в csv, записи из csv вставляет
			в таблицу 'url', повторно делает запрос к БД по зданному
			доменному имени из запроса, результат возвращает клиенту

	"""

	def __init__(self,  domains, links_limit):
		""" Override Конструктор

		Запускает конструктор базового (наследуемого) класса.
		Инициализирует логгер и переменные для экземпляра класса

		"""
		super().__init__(domains)
		self.logger = logging.getLogger(__name__ + '.FindData ')
		self.logger.debug('The instance of FindData created.')
		self.links_limit = links_limit

	async def get_data_from_db(self):
		""" Overrode

		Метод получает информацию с БД по запрашиваемому доменному имени.
		В качестве возвращаемого значения список, элементами которого
		являются записи из таблицы в виде словарей поле->значение.

		"""
		super().get_data_from_db()
		sql_statement = "SELECT * FROM url WHERE domain " \
						"LIKE '%" + self.domain + "' LIMIT 200"
		self.logger.info("SQL statement: {}".format(sql_statement))
		cursor = self.query_database(sql_statement)
		rows = cursor.fetchall()
		if rows:
			self.result = self.get_result(rows)
		else:
			self.logger.info("Domain {} not exist in URL table. Let's make "
							 "request to commoncrawl.org".format(self.domain))
			links_for_download = get_links_from_common_crawl(self.domain,
															 self.links_limit)
			if not links_for_download:
				self.result.append(self.not_found)
				return self.result
			self.logger.info('Download links received.')
			await download_gz_files(links_for_download)
			gunzip_files()
			prepare_write_data(self.domain)
			self.logger.info("Repeated query to the DB...")
			self.logger.info("SQL statement: {}".format(sql_statement))
			cursor = self.query_database(sql_statement)
			rows = cursor.fetchall()
			if rows:
				self.result = self.get_result(rows)
			else:
				self.result.append(self.not_found)
		return self.result

	def get_result(self, rows):
		""" Override

		Метод из запроса к БД формирует результат в виде списка,
		элементами которого являются записи таблицы в виде
		словаря поле->значение. Возвращает список.

		"""
		super().get_result(rows)
		for row in rows:
			self.result.append({
				"domain": row[1],
				"created": row[2],
				"text": row[3],
				"is_accompanying": row[4],
				"url": row[5],
			})
		self.result.append({"status": "OK"})
		return self.result

	def __del__(self):
		""" Override Деструктор """
		super().__del__()
		self.logger.debug('The instance of FindData deleted')
