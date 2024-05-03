## Table Migration

The table models defined in `tdp/core/models` might incure changes over time. Alembic is used to migrate these changes to your database.

It has been setup to work with 3 DBMSs:`MySQL`,`PostreSQL` and `SqLite`. 

Use Alembic:

- Set path to `alembic.ini` file if alembic commands are executed ouside the `tdp-lib` folder:

    ```sh
    export ALEMBIC_CONFIG=<path_to>/tdp-lib/alembic.ini
    ```

- Check if the database is up to date with SQLAlchemy models in the code:

    ```sh
    alembic --name <BBMS> check
    ```

- Let Alembic generate a migration file in `alambic/versions`:

    ```sh
    alembic --name <BBMS> revision --autogenerate -m "commit message"
    ```

- Verify the file generated in the `versions` folder, modify it if necessary and test the upgrade with the command:

   ```sh
   alembic --name <BBMS> upgrade head
   ```
