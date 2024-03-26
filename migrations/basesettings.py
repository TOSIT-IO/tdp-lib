# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import create_engine


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    TDP_DATABASE_DSN: str


settings = Settings(_env_file=".env", _extra="ignore")

engine = create_engine(settings.TDP_DATABASE_DSN, echo=False, future=True)
