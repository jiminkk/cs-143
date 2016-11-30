
LOAD xsmall FROM 'xsmall.del' WITH INDEX
SELECT COUNT(*) FROM xsmall
SELECT * FROM xsmall WHERE key = 2342 AND key <> 2342
SELECT * FROM xsmall WHERE key < 2342 AND key <> 2342
SELECT * FROM xsmall WHERE key > 2342 AND key <> 2342
SELECT * FROM xsmall WHERE key < 2342 AND key = 2342
SELECT * FROM xsmall WHERE key > 2342 AND key = 2342
SELECT * FROM xsmall WHERE key<200 AND key<2000 AND key<20000
SELECT * FROM xsmall WHERE value > 'Baby Take a Bow'
SELECT * FROM xsmall WHERE value <> 'Baby Take a Bow'
SELECT * FROM xsmall WHERE value >= 'Baby Take a Bow' AND value < 'Knot'
SELECT * FROM xsmall WHERE key > 2500 AND value > 'Baby Take a Bow'

LOAD small FROM 'small.del' WITH INDEX
SELECT COUNT(*) FROM small
SELECT * FROM small WHERE key > 100 AND key < 500
SELECT * FROM small WHERE key = 2342 AND key <> 2342
SELECT * FROM small WHERE key < 2342 AND key = 2342
SELECT * FROM small WHERE key<200 AND key<2000 AND key<20000
SELECT * FROM small WHERE value > 'Baby Take a Bow'
SELECT * FROM small WHERE value <> 'Baby Take a Bow'
SELECT * FROM small WHERE value >= 'Baby Take a Bow' AND value < 'Knot'
SELECT * FROM small WHERE key > 2500 AND value > 'Baby Take a Bow'

LOAD medium FROM 'medium.del' WITH INDEX
SELECT COUNT(*) FROM medium
SELECT * FROM medium WHERE key = 489
SELECT * FROM medium WHERE key > 100 AND key < 500
SELECT * FROM medium WHERE key = 2342 AND key <> 2342
SELECT * FROM medium WHERE key < 2342 AND key = 2342
SELECT * FROM medium WHERE key<200 AND key<2000 AND key<20000
SELECT * FROM medium WHERE value > 'Baby Take a Bow'
SELECT * FROM medium WHERE value <> 'Baby Take a Bow'
SELECT * FROM medium WHERE value >= 'Baby Take a Bow' AND value < 'Knot'
SELECT * FROM medium WHERE key < 2500 AND value > 'Baby Take a Bow'

LOAD large FROM 'large.del' WITH INDEX
SELECT COUNT(*) FROM large
SELECT * FROM large WHERE key > 4500
SELECT * FROM large WHERE key > 4500 AND key > 0

LOAD xlarge FROM 'xlarge.del' WITH INDEX
SELECT COUNT(*) FROM xlarge
SELECT * FROM xlarge WHERE key = 4240
SELECT * FROM xlarge WHERE key > 400 AND key < 500 AND key > 100 AND key < 4000000
