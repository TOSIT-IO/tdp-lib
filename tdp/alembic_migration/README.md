# Database Migration

Database migrations are handled by Alembic, a lightweight database migration tool compatible with SQLAlchemy. It is used to generate migration scripts, called "revisions", based on the database schema changes (defined in `tdp/core/models`).

## Dialects

TDP supports multiple databases dialects which require distinct environments (with their own set of revisions). Environments are located in the `alembic_migration` folder:

- `mysql` for MySQL and MariaDB
- `postgresql` for PostgreSQL
- `sqlite` for SQLite

Mapping between the dialects and these environments is done in the `alembic.ini` file.

## Migration Modes

TDP only supports the `online` mode which allows to run the migration scripts directly on the database. It does not support the `offline` (aka `--sql`) mode which generates the SQL scripts to be run manually. Hence, the `run_migrations_offline` function in `env.py` is not implemented.

## Usage (development environment)

Database connections are required by Alembic to generate the revisions. As mentioned previously, revisions need to be generated for each dialect, hence, the corresponding database needs to be running.

### Database Setup (Optional)

For convenience, a compose file is provided to run all supported databases in Docker containers. To start the databases, run:

```sh
docker-compose -f dev/docker-compose.yaml up -d
```

To use the databases, set the following environment variables:

```sh
TDP_ALEMBIC_SQLITE_DSN=sqlite:///sqlite.db
TDP_ALEMBIC_POSTGRESQL_DSN=postgresql://postgres:postgres@localhost:5432/tdp
TDP_ALEMBIC_MYSQL_DSN=mysql+pymysql://mysql:mysql@localhost:3306/tdp
```

### Environment Setup (Optional)

If Alembic commands are executed ouside the `tdp-lib` folder, export the path to the `alembic.ini` file:

```sh
export ALEMBIC_CONFIG=<path_to>/tdp-lib/alembic.ini
```

### Running Alembic Commands

Alembic needs to know which environment to use when running the commands. This is done by specifying the `--name` argument.

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
