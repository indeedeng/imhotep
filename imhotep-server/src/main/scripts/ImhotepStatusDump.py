from imhotep_utils import read_hosts_file, get_status_dump

def main() :
	hosts = read_hosts_file('/var/imhotep/hosts.txt')
	print 'hosts: %s' % hosts
	for host in hosts :
		status_dump = get_status_dump(host)
		
		out_file = open('%s.dump' % host[0], 'w')
		out_file.write(str(status_dump))
		out_file.close()

if __name__ == '__main__' :
	main()