CREATE OR REPLACE FUNCTION popc(bit)
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION popc_and(bit, bit)
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION popc_or(bit, bit)
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION popc_xor(bit, bit)
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C;
