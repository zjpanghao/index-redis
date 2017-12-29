#!/bin/bash
pidof fetch_market_redis|xargs kill -9
nohup ./fetch_market_redis index_redis_test &
