-- Create databases
CREATE DATABASE nimtable;

-- Create Nimtable user and grant access to its DB
CREATE USER nimtable_user WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE nimtable TO nimtable_user;

\connect nimtable

GRANT ALL ON SCHEMA public TO nimtable_user;
ALTER SCHEMA public OWNER TO nimtable_user;
