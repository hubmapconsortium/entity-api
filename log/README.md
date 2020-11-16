# API Logging

All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`. This log file is generated from the container and being mounted to the host system for data persistence.

## Log rotation

On the host system, the log rotation is handled via `logrotate` utility with a daily logging rotation schedule. Copy the configuration file from `src/entity-api.logrotate.conf` to `/etc/logrotate.d/`.