[Install PostgreSQL on macOS](https://gist.github.com/phortuin/2fe698b6c741fd84357cec84219c6667)

Create database `lamden_mainnet`

Retrieve KEY or VALUE from state

```
select state::json->'key'
from current_state

select state::json->'value'
from current_state
```

Install necessary packages
`sudo apt -y install gnupg2 wget vim`

Add the repository that provides PostgreSQL 14 on Ubuntu 20.04|18.04
`sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'`

Iport the GPG signing key for the repository
`wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -`

Update package list
`sudo apt -y update`

Install PostgreSQL 14
`sudo apt -y install postgresql-14`

Start database server
`sudo systemctl start postgresql@14-main`

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

???
`sudo nano /etc/postgresql/14/main/pg_hba.conf`