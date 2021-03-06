import sys,psycopg2,csv,time,os,re, datetime, pprint, Queue
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
		self.server_weight=self.generate_weight()

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
	def write_result_to_csv(self,):
		pprint.pprint(extract_query)
		with open('letter_report.csv', 'wb') as csvfile:
			fieldnames = ['description','','result']
			writer = csv.writer(csvfile)
			writer.writerow(['S.No','Description',' Details','Result'])
			for index,each_result in enumerate(extract_query.values()):
				if isinstance(each_result["result"],dict):
					writer.writerow([])
					for e in each_result["result"]:
						writer.writerow([index+1,each_result['description'],e,each_result['result'][e]])
				elif isinstance(each_result["result"],list):
					writer.writerow([])
					writer.writerow([index+1,each_result['description'],each_result['result'][0],[each_result['result'][1]]])
				else:
					writer.writerow([index+1,each_result['description'],'',each_result['result']])
					writer.writerow([])

	def reduce(self,result_map,neglect_count_reduce):
		

		count = [0]*5
		fields = set()
		field_count = {}
		if debug:
			for qu in result_map:
				for sch in result_map[qu]:
					if len(result_map[qu][sch]) != 0 and result_map[qu][sch][0][0] != None:
						if len(extract_query[qu]['result_format']) ==1:
							if extract_query[qu]['result_format'][0]['action']=='sum':
								count[0] += result_map[qu][sch][0][0]
						elif len(extract_query[qu]['result_format']) ==2:
							if extract_query[qu]['result_format'][0]['action']=='sum' and extract_query[qu]['result_format'][1]['action']=='sum':

								count[0] += result_map[qu][sch][0][0]
								count[1] += result_map[qu][sch][0][1]
							
							elif extract_query[qu]['result_format'][0]['action']=='groupby'  and extract_query[qu]['result_format'][1]['action']=='sum':

								if extract_query[qu]['result_format'][0]['data_type'] == 'date':
									for each_record in result_map[qu][sch]:
										format_for_month = each_record[0].strftime('%m-%Y')
										fields.add(format_for_month)
										field_count[format_for_month] = 0

									for each_record in result_map[qu][sch]:
										if each_record[0].strftime('%m-%Y') in fields:
											field_count[format_for_month] += each_record[1]
								
								if extract_query[qu]['result_format'][0]['data_type'] in 'string':
									for each_record in result_map[qu][sch]:
										fields.add(each_record[0])
										field_count[each_record[0]] = 0

									for each_record in result_map[qu][sch]:
										if each_record[0] in fields:
											field_count[each_record[0]] += each_record[1]

							elif extract_query[qu]['result_format'][0]['action']=='group_outside' and extract_query[qu]['result_format'][1]['action']=='group_outside':
								# import ipdb; ipdb.set_trace()
								if extract_query[qu]['result_format'][1]['data_type']=='string' and extract_query[qu]['result_format'][1]['data_type']=='string':
									for each_record in result_map[qu][sch]:
										fields.add(each_record)
							else:
								pass 
						


				# if qu ==2:
				if len(extract_query[qu]['result_format']) ==1:
					extract_query[qu]["result"] = count[0]
					count[0] = 0
				if len(extract_query[qu]['result_format']) == 2:
					if extract_query[qu]['result_format'][0]['action']=='sum' and extract_query[qu]['result_format'][1]['action']=='sum':
						extract_query[qu]["result"] = [count[0],count[1]]
						count[0] = 0
						count[1] = 0
						
					elif extract_query[qu]['result_format'][0]['action']=='groupby' and extract_query[qu]['result_format'][1]['action']=='sum':
						extract_query[qu]["result"] = field_count
						fields=set()
						field_count={}

					elif extract_query[qu]['result_format'][0]['action']=='group_outside' and extract_query[qu]['result_format'][1]['action']=='group_outside':
						new_dict={}
						for each in list(fields):
							new_dict[each[0]]=each[1]
						extract_query[qu]["result"] = new_dict
						fields = set()
					else:
						pass				

		else:
			for qu in result_map:
				for svr in result_map[qu]:
					for db in result_map[qu][svr]:
						for sch in result_map[qu][svr][db]:
							if len(result_map[qu][svr][db][sch]) != 0 and result_map[qu][svr][db][sch][0][0] != None:
								if len(extract_query[qu]['result_format']) ==1:
									if extract_query[qu]['result_format'][0]['action']=='sum':
										count[0] += result_map[qu][svr][db][sch][0][0]
								elif len(extract_query[qu]['result_format']) ==2:
									if extract_query[qu]['result_format'][0]['action']=='sum' and extract_query[qu]['result_format'][1]['action']=='sum':

										count[0] += result_map[qu][svr][db][sch][0][0]
										count[1] += result_map[qu][svr][db][sch][0][1]
									
									elif extract_query[qu]['result_format'][0]['action']=='groupby' and extract_query[qu]['result_format'][1]['action']=='sum':

										if extract_query[qu]['result_format'][0]['data_type'] == 'date' :
											for each_record in result_map[qu][svr][db][sch]:
												format_for_month = each_record[0].strftime('%m-%Y')
												fields.add(format_for_month)
												field_count[format_for_month] = 0

											for each_record in result_map[qu][svr][db][sch]:
												if each_record[0].strftime('%m-%Y') in fields:
													field_count[format_for_month] += each_record[1]
										
										if extract_query[qu]['result_format'][0]['data_type'] in 'string' :
											for each_record in result_map[qu][svr][db][sch]:
												fields.add(each_record[0])
												field_count[each_record[0]] = 0

											for each_record in result_map[qu][svr][db][sch]:
												if each_record[0] in fields:
													field_count[each_record[0]] += each_record[1]

									elif extract_query[qu]['result_format'][0]['action']=='group_outside' and extract_query[qu]['result_format'][1]['action']=='group_outside':
										if extract_query[qu]['result_format'][1]['data_type']=='string' and extract_query[qu]['result_format'][1]['data_type']=='string':
											for each_record in result_map[qu][svr][db][sch]:
												fields.add(each_record)
									else:
										pass
								
								else:
									pass

				
				if len(extract_query[qu]['result_format']) ==1:
					extract_query[qu]["result"] = count[0]
					count[0] = 0
				elif len(extract_query[qu]['result_format']) == 2:
					if extract_query[qu]['result_format'][0]['action']=='sum' and extract_query[qu]['result_format'][1]['action']=='sum':
						extract_query[qu]["result"] = [count[0],count[1]]
						count[0] = 0
						count[1] = 0
						
					elif extract_query[qu]['result_format'][0]['action']=='groupby' and extract_query[qu]['result_format'][1]['action']=='sum':
						extract_query[qu]["result"] = field_count
						fields=set()
						field_count={}
					elif extract_query[qu]['result_format'][0]['action']=='group_outside' and extract_query[qu]['result_format'][1]['action']=='group_outside':
						new_dict={}
						for each in list(fields):
							new_dict[each[0]]=each[1]
						extract_query[qu]["result"] = new_dict
					else:
						pass
				# else:
				# 	pass

				# if qu ==2:
				# 	extract_query[qu]["result"] = template_count
					
				# elif qu in (4,8):
				# 	extract_query[qu]["result"] = [count_48_user,count_48]
				# 	count_48 = 0
				# 	count_48_user = 0
					
				# elif qu==9:
				# 	extract_query[qu]["result"] = monthly_count
					
				# else:
				# 	extract_query[qu]["result"] = count_rest
				# 	count_rest = 0
			pass

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
			
			schema_q = Queue.Queue(len(schemas))
			for each in schemas:
				schema_q.put({'schema_name':each[0]})
			while schema_q.unfinished_tasks > 0:
				schema = schema_q.get()
				schema_name = schema['schema_name']
				cur.execute("set search_path to {};".format(schema_name))
				cur.execute(task.values()[0])
				result = cur.fetchall()
				
				s_temp_map[schema_name] = result
				schema_q.task_done()
			
			result_map[task.keys()[0]] = s_temp_map
			
			s_temp_map={}
			par.task_q.task_done()
		conn.close()
		# import pprint
		# pprint.pprint(result_map)
		par.reduce(result_map,(2,4,8,9))
		# import ipdb; ipdb.set_traces()
		par.write_result_to_csv()
	else:


		# Load Server Details, Map, weight from CSV
		filename = 'schema_details.csv'
		servers = Servers(filename)
		
		# Parallely spawn one process for each db connection
		par = Sequencial(servers)

		result_map = {}
		server_temp = {}
		db_temp = {}
		schema_temp = {}
		while par.task_q.unfinished_tasks > 0:
			task = par.task_q.get()
			result_map[task.keys()[0]] = {}
			while par.server_q.unfinished_tasks > 0:
				server_name = par.server_q.get()
				db_q = par.load_db_q(server_name)
				while db_q.unfinished_tasks > 0:
					dbname = db_q.get()
					schema_q = par.load_schema_q(server_name,dbname)
					while schema_q.unfinished_tasks > 0:
						schema_name = schema_q.get()
						server_info = par.servers.schema_info(server_name,dbname,schema_name)
						import ipdb; ipdb.set_trace()
						connObj = Connection(server_info)
						connObj.connect()
						result = connObj.query_runner(par.task_q.queue[0].values())
						connObj.close()
						schema_temp[schema_name] = result
						schema_q.task_done()
					db_temp[dbname] = schema_temp
					schema_temp = {}
					db_q.task_done()
				server_temp[server_name] = db_temp
				db_temp={}
				par.server_q.task_done()
			result_map[task.keys()[0]] = server_temp
			server_temp = {}
			par.task_q.task_done()
		par.reduce(result_map,(2,4,8,9))
		par.write_result_to_csv()
		import pprint
		pprint.pprint(result_map)
		print " \n"
		



if __name__=='__main__':
	main()
		