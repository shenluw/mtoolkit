# -*- coding: utf-8 -*-
import hashlib
import os
import sqlite3
import sys
import zlib

sql_conn = None
sql_cursor = None
is_begin = False

BATCH_COUNT = 1000
TABLE_COUNT_LIMIT = 2 << 24

register_algorithm = {}

register_algorithm_alias = {}

sql_conn_dict = {}

DATA_TABLE_NAME = 'alg'

for name in hashlib.algorithms_available:
	if name == 'shake_128' or name == 'shake_256':
		pass
	else:
		register_algorithm[name] = name


def call_algorithms(name, bs):
	m = hashlib.new(name)
	m.update(bs)
	return m.hexdigest()


for name in hashlib.algorithms_available:
	if name.lower() in register_algorithm and name == name.upper():
		register_algorithm_alias[name] = name + '_up'
	else:
		register_algorithm_alias[name] = name


def update_bitsets(max_limit, bitsets):
	index = len(bitsets) - 1
	while True:
		bitsets[index] += 1
		bitsets[index] %= max_limit
		if bitsets[index] == 0:
			if index == 0:
				return False
			index -= 1
		else:
			break
	return True


def record_bitsets(bitsets):
	f = open(record_path, 'w+', encoding='utf-8')
	f.write(','.join(map(str, bitsets)))
	f.close()


def read_record(record_path):
	try:
		f = open(record_path, encoding='utf-8')
		txt = f.read()
		result = list(map(int, txt.split(',')))
		f.close()
	except:
		result = []
	return result


def create_splite_table(cursor):
	cursor.execute("""
		create table if not exists splite_table (
		  id    INTEGER      not null primary key autoincrement ,
		  key   varchar(255) not null unique,
		  value long         not null );
		""")


# def switch_new_table(key):
# 	conn = sqlite3.connect(db_name)
# 	cursor = conn.cursor()
# 	create_splite_table(cursor)
# 	cursor.execute("select value from splite_table where key = ?;", [key])
# 	count = 0
# 	fetchone = cursor.fetchone()
# 	if fetchone is None:
# 		cursor.execute("insert into splite_table (key, value) values (?,?);", [key, count])
# 	else:
# 		count = fetchone[0] + 1
# 		cursor.execute("update splite_table set value = ? where key = ?;", [count, key])
#
# 	conn.commit()
# 	conn.close()
#
# 	return count

def init_db():
	for algo in register_algorithm.keys():
		conn = sqlite3.connect(db_name + '.' + register_algorithm_alias[algo])
		cursor = conn.cursor()
		create_splite_table(cursor)
		table_name = DATA_TABLE_NAME
		table_index = get_table_index(cursor, table_name)
		create_data_table(cursor, table_name, table_index)
		conn.commit()
		conn.close()


def switch_new_table2(key, cursor):
	cursor.execute("select value from splite_table where key = ?;", [key])
	count = 0
	fetchone = cursor.fetchone()
	if fetchone is None:
		cursor.execute("insert into splite_table (key, value) values (?,?);", [key, count])
	else:
		count = fetchone[0] + 1
		cursor.execute("update splite_table set value = ? where key = ?;", [count, key])

	return count


def get_table_index(cursor, key):
	cursor.execute("select value from splite_table where key = ?;", [key])
	count = 0
	fetchone = cursor.fetchone()
	if fetchone is None:
		cursor.execute("insert into splite_table (key, value) values (?,?);", [key, count])
	else:
		count = fetchone[0]
	return count


def begin_transaction():
	print("begin: ", bitsets)
	for algo in register_algorithm.keys():
		conn = sqlite3.connect(db_name + '.' + register_algorithm_alias[algo])
		cursor = conn.cursor()
		sql_conn_dict[algo] = (conn, cursor)
	global is_begin
	is_begin = True


def end_transaction():
	print("end")
	global is_begin
	for arr in sql_conn_dict.values():
		conn = arr[0]
		conn.commit()
		conn.close()
	sql_conn_dict.clear()
	is_begin = False


def create_data_table(cursor, table_name, index):
	real_table_name = table_name + str(index)
	cursor.execute("""
		create table if not exists {} (
		  id              INTEGER      not null
			primary key autoincrement,
		  algorithm        BLOB			 not null,
		  origin          BLOB          not null unique ,
		  origin_group    varchar(64)  not null,
		  algorithm_group varchar(64)  not null
		);
		""".format(real_table_name))


def insert_template(txt_bytes, origin_id, cursor, table_name, algorithm_bytes, algorithm_group):
	table_index = get_table_index(cursor, table_name)
	real_table_name = table_name + str(table_index)
	origin_group = origin_id[:4]
	try:
		cursor.execute("insert into {} (algorithm, origin, origin_group, algorithm_group) values (? ,? ,? ,?);".format(real_table_name), (algorithm_bytes, txt_bytes, origin_group, algorithm_group))
		lastrowid = cursor.lastrowid
		if lastrowid >= TABLE_COUNT_LIMIT:
			table_index = switch_new_table2(table_name, cursor)
			create_data_table(cursor, table_name, table_index)
	except:
		pass


def process_txt(bitsets):
	txt = ''
	for bit in bitsets:
		txt += source_txt[bit]
	encode_txt = txt.encode(encoding='utf-8')
	sha256str = hashlib.sha256(encode_txt).hexdigest()
	compress_origin_txt = zlib.compress(encode_txt)
	for k, func in register_algorithm.items():
		cursor = sql_conn_dict[k][1]
		algorithms_txt = call_algorithms(k, encode_txt)
		compress_algorithms_txt = zlib.compress(algorithms_txt.encode(encoding='utf-8'))
		insert_template(compress_origin_txt, sha256str, cursor, DATA_TABLE_NAME, compress_algorithms_txt, algorithms_txt[:4])


def generate_txt(src_len, bitsets):
	global count
	count = 0
	while True:
		if count == 0:
			begin_transaction()
		process_txt(bitsets)
		do_next = update_bitsets(src_len, bitsets)
		if not do_next:
			break
		count += 1
		if count > BATCH_COUNT:
			record_bitsets(bitsets)
			end_transaction()
			count = 0


simple_txt_china = """ !"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~·—‘“”…、。《》【】！（），：；？￥"""
simple_txt = """ !"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~"""

db_name = None
record_path = None
source_txt = ''
source_len = 0
if __name__ == '__main__':
	print(sys.argv)
	import argparse

	parser = argparse.ArgumentParser(description='auto config')
	parser.add_argument("--db")
	parser.add_argument("--src")
	parser.add_argument("--srcpath")
	parser.add_argument("--record")

	args = parser.parse_args()
	if not os.path.exists("data"):
		os.mkdir("data")
	db_name = args.db or './data/simple.db'

	if args.src is not None:
		source_txt = args
	elif args.srcpath and os.path.exists(args.srcpath):
		f = open(args.srcpath, encoding='utf-8')
		source_txt = f.read()
		source_len = len(source_txt)
	else:
		source_txt = simple_txt

	source_len = len(source_txt)
	if source_len == 0:
		print("source file is empty")
	else:
		print("source txt")
		print(source_txt)
		source_txt = ''.join(sorted(set(source_txt)))
		print("real source: ", source_txt)
		record_path = args.record or "bitsets.txt"

		bitsets = read_record(record_path)

		print("bitsets: ", bitsets)
		if len(bitsets) == 0:
			bitsets = [0]

		init_db()

		while True:
			generate_txt(source_len, bitsets)
			is_begin and end_transaction()
			bitsets = [0] * (len(bitsets) + 1)
