# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    POSTGRES_DSN: str
    MYSQL_DSN: str
    MARIADB_DSN: str
    SQLITE_DSN: str


settings = Settings(_env_file=".env", _extra="ignore")  # type: ignore
