import glob
import codecs
import csv
import sys
import string
import os
import logging
from contextlib import closing
from datetime import datetime
from urllib.parse import urlparse
import pandas as pd
import numpy as np
from warcio.archiveiterator import ArchiveIterator
import psycopg2
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


punct = string.punctuation.replace(".", "")
logger = logging.getLogger(__name__)


def remove_stopwords(text, path, k):
	""" Метод с помощью библиотеки nltk очищает строку от стоп-слов

	Возвращает очищенную строку.

	"""
	try:
		text_tokens = word_tokenize(text)
	except TypeError:
		logger.error('Номер файла с ошибкой: {}'.format(path))
		logger.error('Номер строки с ошибкой: {}'.format(str(k)))
		logger.error('Текст строки: {}'.format(str(text)))
		return None
	token_without_sw = [word for word in text_tokens if not word in stopwords.words()]
	return ' '.join(token_without_sw)


def remove_punctuation(text):
	""" Метод очищает строку от занков пунктуации, кроме знака точки (.)

	Возвращает очищенную строку.

	"""
	translator = str.maketrans('', '', punct)
	return text.translate(translator)


def insert_to_db(domain, created, text, is_accompanying, url):
	with closing(psycopg2.connect(
			host=os.getenv('DB_HOST'),
			port=os.getenv('DB_PORT'),
			user=os.getenv('DB_USERNAME'),
			password=os.getenv('DB_PASSWORD'),
			dbname=os.getenv('DB_NAME'))) as conn:
		with conn.cursor() as cursor:
			sql_statement = "INSERT INTO url (domain, created, text, is_accompanying, url) VALUES (%s, %s, %s, %s, %s)"
			record_to_insert = (domain, created, text, is_accompanying, url)
			try:
				cursor.execute(sql_statement, record_to_insert)
				conn.commit()
			except psycopg2.errors.UniqueViolation:
				logger.info('duplicate detected, ignore')
				return


def prepare_write_data(requested_domain):
	""" Меод преобразует wet-файлы в csv-файлы

	Записи из csv-файлов очищает (стоп-слова, пунктуация),
	вставляет информацию в БД (вызывает метод insert_to_db())

	"""
	paths = glob.glob("*.warc.wet")
	count_paths = len(paths)
	csv.field_size_limit(sys.maxsize)
	for cp, path in enumerate(paths):
		with open(path, 'rb') as stream:
			list_dicts = []
			for record in ArchiveIterator(stream):
				try:
					list_dicts.append(
						dict([*{'content_type': record.content_type,
								'raw_stream.readline()': record.raw_stream.readline().decode('UTF-8'),
								'rec_type': record.rec_type}.items()] + record.rec_headers.headers))
				except UnicodeDecodeError:
					continue
		df = pd.DataFrame(list_dicts)
		df.fillna(' ', inplace=True)
		df['WARC-Target-URI'] = df['WARC-Target-URI'].apply(
										lambda x: x if x != ' ' else np.nan)
		df.dropna(inplace=True)
		csv_filename = path + '.csv'
		df.to_csv(csv_filename, index=False)
		with codecs.open(csv_filename, 'r', 'utf-8') as f:
			reader = csv.DictReader((l.replace('\0', '') for l in f))
			for k, line in enumerate(reader):
				if line['raw_stream.readline()'] is None:
					continue
				domain = urlparse(line['WARC-Target-URI']).netloc
				created = datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')
				text_without_stopwords = remove_stopwords(
										line['raw_stream.readline()'], path, k)
				text_without_punctuation = remove_punctuation(
										text_without_stopwords)
				text = text_without_punctuation.lower()
				is_accompanying = 'false' if requested_domain in domain else 'true'
				url = line['WARC-Target-URI']
				if k % 100 == 0:
					logger.info('File {}/{} ({}).   Raws processed: {}'
								.format(str(cp+1), count_paths, path, k))
				insert_to_db(domain, created, text, is_accompanying, url)
		os.remove(csv_filename)
		logger.info('File {} removed'.format(csv_filename))
		os.remove(path)
		logger.info('File {} removed'.format(path))
