# The Sumstat discipline

DESC_IN

	- stat_A: {'type': 'float', 'default': 1.3, 'unit': '-', 'visibility': 'Shared', 'namespace': 'ns_sum_stat'}
	- stat_B: {'type': 'float', 'default': 1.3, 'unit': '-', 'visibility': 'Shared', 'namespace': 'ns_sum_stat'}
	- stat_C: {'type': 'float', 'default': 1.3, 'unit': '-', 'visibility': 'Shared', 'namespace': 'ns_sum_stat'}

DESC_OUT

	- sum_stat: {'type': 'float', 'unit': '-', 'visibility': 'Shared', 'namespace': 'ns_sum_stat'}
RUN

$sum_stat = stat_A + stat_B + stat_C$