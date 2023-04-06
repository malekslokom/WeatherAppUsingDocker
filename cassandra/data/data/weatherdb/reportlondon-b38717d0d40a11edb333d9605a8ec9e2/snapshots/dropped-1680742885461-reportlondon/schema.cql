CREATE TABLE IF NOT EXISTS weatherdb.reportlondon (
	time text PRIMARY KEY,
	clouds int,
	dt bigint,
	feels_like float,
	humidity int,
	id int,
	lat float,
	lon float,
	name text,
	pressure int,
	rain1h float,
	sys_country text,
	sys_id int,
	sys_sunrise bigint,
	sys_sunset bigint,
	sys_type int,
	temp float,
	temp_max float,
	temp_min float,
	timezone int,
	visibility int,
	weather text,
	weather_description text,
	wind_speed float)
	WITH ID = b38717d0-d40a-11ed-b333-d9605a8ec9e2
	AND bloom_filter_fp_chance = 0.01
	AND dclocal_read_repair_chance = 0.1
	AND crc_check_chance = 1.0
	AND default_time_to_live = 0
	AND gc_grace_seconds = 864000
	AND min_index_interval = 128
	AND max_index_interval = 2048
	AND memtable_flush_period_in_ms = 0
	AND read_repair_chance = 0.0
	AND speculative_retry = '99PERCENTILE'
	AND comment = ''
	AND caching = { 'keys': 'ALL', 'rows_per_partition': 'NONE' }
	AND compaction = { 'max_threshold': '32', 'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy' }
	AND compression = { 'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor' }
	AND cdc = false
	AND extensions = {  };
