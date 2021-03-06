import sys,psycopg2,csv,time,os,re
import Queue
from multiprocessing import cpu_count
from multiprocessing import Pool,Lock, TimeoutError
from itertools import repeat
from functools import partial
sys.path.append('~/Desktop/greytip/Desktop/letter_report/')
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
					server_details['connection_url'] = 'postgresql://pgbouncer.gtkops.in'
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
		connection_str = "dbname={0}, user={1}, password={2}, host={3}, port={4}".format(*connection_param)
		# self.conn = psycopg2.connect(connection_str)
		# self.cur = conn.cursor()
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


class Parallel():
	def __init__(self,servers):
		self.servers = servers

	def distribute_by_core(self,nitems, nprocs=None):
		if nprocs is None:
			nprocs = cpu_count()
			nitems_per_proc = (nitems+nprocs-1)/nprocs
			return [(i, min(nitems, i+nitems_per_proc)) for i in range(0, nitems, nitems_per_proc)]
			# slices = obj.distribute(obj.server_map)
			# 	results = [pool.apply_async(Server.query_runner, (obj, imin, imax)) for (imin, imax) in slices]
			# 	for r, (imin, imax) in zip(results, slices):
			# 		data[imin:imax] = r.get()	

	def load_balance(self,):
		server_name = 'pap'
		dbname = 'pap00'
		schema_name = self.servers.schema_list(server_name,dbname)[0]
		server_info = self.servers.schema_info(server_name,dbname,schema_name)
		connection = Connection(server_info)
		pool = Pool()
		data = []
	
	def load_tasks_q(self,):
		task_q = Queue.Queue(len(extract_query))
		for key in sorted(extract_query.keys()):
			task_q.put({key:extract_query[key][1]})
		return task_q
	
	def load_schema_q(self,server_name,dbname):
		schema_q = Queue.Queue(len(self.servers.schema_list(server_name,dbname)))
		for schema in self.servers.schema_list():
			schema_q.put(schema)
		return schema_q
	
	def load_db_q(self,server_name):
		db_q = Queue.Queue(len(self.servers.db_list(server_name)))
		for db in self.servers.db_list():
			db_q.put(db)
		return db_q

	def load_server_q(self,):
		server_q = Queue.Queue(len(self.servers.server_list()))
		for server in self.servers.server_list():
			server_q.put(server)
		return server_q
		
	def process(self,):

		pass

	def reduce(self,):
		pass

class main():
	if debug:
		# conn = psycopg2.connect(
		# 	"dbname=public_backups user=majordomo host=192.168.3.103")
		# cur = conn.cursor()

		# Load Server Details, Map, weight from CSV
		filename = 'schema_details.csv'
		servers = Servers(filename)
		
		# Parallely spawn one process for each db connection
		import ipdb; ipdb.set_trace()
		p = Parallel(servers)
		p.process()


	else:
		pass
		# obj.db_list(obj.server_list()[])

		# lock = Lock()
		# pool.apply
		# 1
		# repeat_func = partial(start_query,lock)
		# pool.map(repeat_func,servers_dir)
		# 2
		# pool.map(start_query,zip(db_list,repeat(lock)))
		# pool = Pool(processes=8)
	


if __name__=='__main__':
	main()
		




		# def worker():
		# while True:
		# 	item = q.get()
		# 	do_work(item)
		# 	q.task_done()

		# q = Queue()
		# for i in range(num_worker_threads):
		# 	 t = Thread(target=worker)
		# 	 t.daemon = True
		# 	 t.start()

		# for item in source():
		# 	q.put(item)

		# q.join()       # block until all tasks are done


# pool = mp.Pool(processes=4)
# results = [pool.apply_async(cube, args=(x,)) for x in range(1,7)]
# output = [p.get() for p in results]


		# Process(target=f, args=(lock, num)).start()
	# cur.execute(
	# 	"""SELECT table_schema FROM information_schema.tables WHERE table_name like '%%letter_letters%%';""")


# from multiprocessing import Process, Lock

# def f(l, i):
#     l.acquire()
#     print 'hello world', i
#     l.release()

# if __name__ == '__main__':
#     lock = Lock()

#     for num in range(10):
#         Process(target=f, args=(lock, num)).start()





