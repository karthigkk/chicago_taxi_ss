#!/usr/bin/env bash
export IS_DEBUG=${DEBUG:-true}
exec gunicorn -b :${PORT:-5000} --workers=4 --access-logfile - --error-logfile - run:application --preload