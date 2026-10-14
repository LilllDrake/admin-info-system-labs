CREATE ROLE repl_user
    WITH LOGIN
    REPLICATION
    PASSWORD 'replica';

CREATE ROLE app_user
    WITH LOGIN
    PASSWORD 'apppppppppp';

GRANT CONNECT ON DATABASE my_database TO app_user;

GRANT USAGE, CREATE ON SCHEMA public TO app_user;

    