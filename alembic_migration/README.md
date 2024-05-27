# Database Migration (Dev mode)

The table models defined in `tdp/core/models` might incur changes over time. Alembic is used to migrate these changes to your database. This section explains how to use Alembic for database migration in development mode (between 2 releases).

The `offline` triggered with the `--sql` argument in the command ahas been removed. The `online` mode enables the user to generate the revision scripts and perform migrations by being connected to a database.

The `alembic_migration` folder contains 3 subfolders corresponding to the different dialects TDP supports:

- `mysql` (also supports MariaDB)
- `postgresql`
- `sqlite`

Table transformations are written differently according to the mentioned dialects.

Alembic revisions are the migration files which are linked to each other with chained revision ids and revisions of each dialect are independant.

A compose file is provided to run the databases in Docker containers. To start the databases, run:

```sh
docker-compose -f dev/docker-compose.yaml up -d
```

To use the databases, set the following environment variables. For example, in the `.env` file:

```sh
export TDP_ALEMBIC_SQLITE_DSN=sqlite:///sqlite.db
export TDP_ALEMBIC_POSGRESQL_DSN=postgresql://postgres:postgres@localhost:5432/tdp
export TDP_ALEMBIC_MYSQL_DSN=mysql+pymysql://mysql:mysql@localhost:3306/tdp
```

Alembic usage:

- (Optional) Set path to `alembic.ini` file if Alembic commands are executed ouside the `tdp-lib` folder:

  ```sh
  export ALEMBIC_CONFIG=<path_to>/tdp-lib/alembic.ini
  ```

- Check if the database is up to date with SQLAlchemy models defined in `tdp/core/models`:

  ```sh
  alembic --name postgresql check
  alembic --name sqlite check
  alembic --name mysql check
  ```

- Generate a migration file in `alembic/versions`:

  ```sh
  alembic --name postgresql revision --autogenerate -m "alembic commit message"
  alembic --name sqlite revision --autogenerate -m "alembic commit message"
  alembic --name mysql revision --autogenerate -m "alembic commit message"
  ```

- Verify the file generated in the `versions` folder, modify it if necessary and test the upgrade with the command:

  ```sh
  alembic --name postgresql upgrade head
  alembic --name sqlite upgrade head
  alembic --name mysql upgrade head
  ```
