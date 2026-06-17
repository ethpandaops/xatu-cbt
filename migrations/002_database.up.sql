-- No-op: the network database is created out-of-band by network setup before
-- migrations run, because golang-migrate cannot bootstrap its own database.
SELECT 1;
