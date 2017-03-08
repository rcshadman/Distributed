import sys,psycopg2,csv,time,os,re
import Queue
from multiprocessing import cpu_count
from multiprocessing import Pool,Lock, TimeoutError
from itertools import repeat
from functools import partial
sys.path.append('~/Desktop/greytip/Desktop/letter_report/Distributed/')
from query_list import extract_query
# from functools import map, reduce
# import imp
# foo = imp.load_source('module.name', '/path/to/file.py')

debug = True
class Servers():
	def __init__(self,filename):
		self.server_map, self.server_weight = {}, {}
		self.server_map= self.generate_server_map(filename)

	def generate_weight(self,):	
		self.server_weight = {"count":len(self.server_map.keys())}
		for each_server in self.server_map.keys():
			self.server_weight[each_server]={'count':len(self.server_map[each_server].keys())}
			for each_db in self.server_map[each_server].keys():
				self.server_weight[each_server][each_db]={'count':len(self.server_map[each_server][each_db].keys())}
		return self.server_weight

	def generate_server_map(self,filename):
		server_details = {}
		with open(filename, 'rb') as csvfile:
			linereader = csv.reader(csvfile, delimiter=',', quotechar='|')
			for eachline in linereader:
				# import ipdb; ipdb.set_trace()
				if linereader.line_num == 1L:
					pass
				else:
					server_details['dbname'] = eachline[6]
					server_details['schema_name'] = eachline[2]
					server_details['domain'] = eachline[0]
					server_details['schema_username'] = eachline[2]
					server_details['schema_pass'] = eachline[3]
					server_details['connection_url'] = 'http://192.168.3.103'
					# postgresql://pgbouncer.gtkops.in
					server_details['port'] = eachline[8]

					server_name = re.sub(r'\d+$', '', eachline[6])
					if server_name not in self.server_map:
						self.server_map[server_name]={}
					if eachline[6] not in self.server_map[server_name]:
						self.server_map[server_name][eachline[6]]={}
					if eachline[1] not in self.server_map[server_name][eachline[6]]:
						self.server_map[server_name][eachline[6]][eachline[1]]={}
					server_details['server_name'] = server_name
					self.server_map[server_name][eachline[6]][eachline[1]].update(server_details)
					server_details = {} 
			self.generate_weight()
			return self.server_map

	# def get_server_map(self,filename=None):
	# 	if len(self.server_map.keys()) is not 0 and filename is None:
	# 		return self.server_map
	# 	else:
	# 		return self.generate_server_map(filename)
	


	def server_list(self,servers=None):
		return self.server_map.keys() if servers is None else servers.keys()
	
	def db_list(self,server_name):
		return self.server_map[server_name].keys()
	
	def schema_list(self,server_name,dbname):
		server_name = [ each for each in self.server_list() if server_name == each ][0]
		return self.server_map[server_name][dbname].keys()
	
	def schema_info(self,server_name,dbname,schama_name):
		return self.server_map[server_name][dbname][schama_name]

class Connection():
	def __init__(self,server_info):
		self.isSchemaSet=False
		self.connection_info = server_info
		connection_param = (
			server_info['dbname'],
			server_info['schema_username'],
			server_info['schema_pass'],
			server_info['connection_url'],
			server_info['port'])
		# import ipdb; ipdb.set_trace()
		self.connection_str = "dbname={0}, user={1}, password={2}, host={3}, port={4}".format(*connection_param)
		self.conn,self.cur = None,None
	
	def connect(self,):
		self.conn = psycopg2.connect(self.connection_str)
		self.cur = conn.cursor()
		# self.server_info = server_info
		# return conn,cur,server_info


	def set_schema(self,schema_name):
		result = self.cur.execute("set search_path to %s;",server_info['schema_name'])
		return True if result[0] == 'SET' else False

		
	def query_runner(self,query):
		if self.isSchemaSet:
			self.set_schema(self,)
		else:
			self.cur.execute(query)
		result = cur.fetchall()
		return result
	
	def close_connection(self,conn):
		self.conn.close()
		self.conn = None


class Sequencial():
	def __init__(self,servers=None):
		self.servers = servers
		self.task_q = self.load_task_q()
		if servers:
			self.server_q = self.load_server_q()


	def load_task_q(self,):
		task_q = Queue.Queue(len(extract_query))
		for key in sorted(extract_query.keys()):
			task_q.put({key:extract_query[key]["query"]})
		return task_q
	
	def load_schema_q(self,server_name,dbname):
		schema_q = Queue.Queue(len(self.servers.schema_list(server_name,dbname)))
		for schema in self.servers.schema_list(server_name,dbname):
			schema_q.put(schema)
		return schema_q
	
	def load_db_q(self,server_name):
		db_q = Queue.Queue(len(self.servers.db_list(server_name)))
		for db in self.servers.db_list(server_name):
			db_q.put(db)
		return db_q

	def load_server_q(self,):
		server_q = Queue.Queue(len(self.servers.server_list()))
		for server in self.servers.server_list():
			server_q.put(server)
		return server_q
# 4 int , long
# 8 long long
# 9 long, date

	def reduce(self,result_map,neglect_count_reduce):
		# for 2
		count_rest = 0 
		# for 4
		template_count = {}
		letter_template = set()
		
		# for 8
		count_8 = 0
		count_8_user = 0
		# for 9
		if debug:
			for qu in result_map:
				for sch in result_map[qu]:
					if len(result_map[qu][sch]) != 0 and sch[0][0] != None:
						# rest Long
						if qu not in neglect_count_reduce:
							count_rest += result_map[qu][sch][0][0]
						# 2 string , long
						if qu == 2:
							for each_record in result_map[qu][sch]:
								letter_template.add(each_record[0])
								template_count[each_record[0]] = 0
							# letter_template = list(letter_template)
							for each_record in result_map[qu][sch]:
								if each_record[0] in letter_template:
									template_count[each_record[0]] += each_record[1]
						# long, long
						if qu ==8:
							pass

						# int , long 
						# if qu==4:
						# 	user_count = 0
						# 	disabled_count = 0
						# 	for each in result_map[qu][sch]:
						# 		if each in 
						# 		if not user_count:
						# 			user_count = 0
						# 		if not disabled_count:
						# 			disabled_count = 0
						# 		user_count += 1
						# 		disabled_count += 1

						# 9 long date
						if qu == 9:
							pass
							# for each in

				
				extract_query[qu]["result"] = count_rest
				count_rest = 0
			import pprint
			pprint.pprint(template_count)

							# import ipdb; ipdb.set_trace()
		else:
			pass
		# for q in result_map:
		# 	for s in result_map[q]:
		# 			for d in result_map[q][s]:
		# 				for sch in result_map[q][s][d]:
		# 					if q in (4,8):
		# 						pass
		# 					elif q is 9:
		# 						pass
		# 					else:
		# 						count+=result_map[q][s][d][sch][0][0]
		# 	extract_query[q]["result"] = count
		# 	count = 0
class main():
	if  debug:
		string = 'dbname=public_ashwini user=majordomo host=192.168.3.103'
		conn = psycopg2.connect(string)
		cur=conn.cursor()
		cur.execute("SELECT table_schema FROM information_schema.tables WHERE table_name like 'letter_letters';")
		schemas=cur.fetchall()

		result_map={}
		s_temp_map = {}
		par = Sequencial({})
		while par.task_q.unfinished_tasks > 0:
			task = par.task_q.get()
			result_map[task.keys()[0]] = {}
			# for each in schemas:
			schema_q = Queue.Queue(len(schemas))
			for each in schemas:
				schema_q.put({'schema_name':each[0]})
			while schema_q.unfinished_tasks > 0:
				schema = schema_q.get()
				schema_name = schema['schema_name']
				cur.execute("set search_path to {};".format(schema_name))
				cur.execute(task.values()[0])
				result = cur.fetchall()
				# if server_info['dbname'] not in p.servers.db_list(each_server):
				s_temp_map[schema_name] = result
				schema_q.task_done()
			
			result_map[task.keys()[0]] = s_temp_map
			# extract_query[task.keys()[0]]["result"] = s_temp_map
			s_temp_map={}
			par.task_q.task_done()
		import pprint
		pprint.pprint(result_map)
		par.reduce(result_map,(2,4,8,9))
		print " \n"
		import ipdb; ipdb.set_trace()
		conn.close()
	else:


		# Load Server Details, Map, weight from CSV
		filename = 'schema_details.csv'
		servers = Servers(filename)
		
		# Parallely spawn one process for each db connection
		par = Sequencial(servers)

		# result_map = {}
		server_temp = {}
		db_temp = {}
		schema_temp = {}
		while par.task_q.unfinished_tasks > 0:
			task = par.task_q.get()
			# result_map[task.keys()[0]] = {}
			while par.server_q.unfinished_tasks > 0:
				server_name = par.server_q.get()
				db_q = par.load_db_q(server_name)
				while db_q.unfinished_tasks > 0:
					dbname = db_q.get()
					schema_q = par.load_schema_q(server_name,dbname)
					while schema_q.unfinished_tasks > 0:
						schema_name = schema_q.get()
						server_info = par.servers.schema_info(server_name,dbname,schema_name)
						# if server_info['dbname'] not in p.servers.db_list(each_server):
						import ipdb; ipdb.set_trace()
						connObj = Connection(server_info)
						connObj.connect()
						result = connObj.query_runner(par.task_q.queue[0].values())
						connObj.close()
						# par.servers.server_weight[server_name][dbname][schema_name] = result
						s_temp_map[schema_name] = result
						schema_q.task_done()
					db_temp[dbname] = s_temp_map
					db_q.task_done()
				server_temp[server_name] = db_temp
				par.server_q.task_done()
			extract_query[task.keys()[0]]["result"] = server_temp
			par.task_q.task_done()
			
		import pprint
		pprint.pprint(result_map)
		print " \n"
		



if __name__=='__main__':
	main()
		