import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import redis
import xarray as xr
from aio_pika.abc import AbstractIncomingMessage

from src.rise.app.core.cache import get_settings
from rise.rnr.app.core.logging_module import setup_logger

settings = get_settings()

r_cache = redis.Redis(host=settings.redis_url, port=6379, decode_responses=True)

log = setup_logger("default", "consumer.log")


class RISE:
    def read_message(self, body: str) -> Dict[str, Any]:
        message_str = body.decode()
        json_start = message_str.find("{")
        json_end = message_str.rfind("}")
        json_string = message_str[json_start : json_end + 1].replace("\\", "")
        json_data = json.loads(json_string)
        return json_data

    async def process_request(self, message: AbstractIncomingMessage):
        json_data = self.read_message(message.body)
        lid = json_data["lid"]
        log.info(f"Consumed message for {lid}")

    async def process_error(self, message: AbstractIncomingMessage):
        log.error("ERROR QUEUE TRIGGERED")