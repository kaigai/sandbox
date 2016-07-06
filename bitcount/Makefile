PG_CONFIG := pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

MODULES = bitcount
EXTENSION = bitcount
DATA = bitcount--1.0.sql

include $(PGXS)
