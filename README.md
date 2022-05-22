[Install PostgreSQL on macOS](https://gist.github.com/phortuin/2fe698b6c741fd84357cec84219c6667)

Create database `lamden_mainnet`

Retrieve KEY or VALUE from state

```
select state::json->'key'
from current_state

select state::json->'value'
from current_state
```

Install on Ubuntu
`sudo apt-get install postgresql`

Start DB instance
`pg_ctlcluster 12 main start`

Login to psql command line tool
`sudo -u postgres psql`

Create role for application, give login and CREATEDB permissions
```
postgres-# CREATE ROLE myuser WITH LOGIN;
postgres-# ALTER ROLE myuser CREATEDB;
```

Quit psql for postgres user
`\q`

Login as newly created user
`psql postgres -U myuser`

Create database
`CREATE DATABASE lamden_mainnet;`

Grant all privileges to new user
`GRANT ALL PRIVILEGES ON DATABASE mydatabase TO myuser;`

Quit
`\q`
