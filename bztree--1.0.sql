/* contrib/bztree/bztree--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bztree" to load this file. \quit

CREATE FUNCTION bztree_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD bztree TYPE INDEX HANDLER bztree_handler;
COMMENT ON ACCESS METHOD bztree IS 'bztree index access method';

-- Opclasses

CREATE OPERATOR CLASS int2_ops
DEFAULT FOR TYPE int2 USING bztree AS
	OPERATOR	1	=,
	FUNCTION	1	int2eq(int2,int2);

CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING bztree AS
	OPERATOR	1	=,
	FUNCTION	1	int4eq(int4,int4);

CREATE OPERATOR CLASS int8_ops
DEFAULT FOR TYPE int8 USING bztree AS
	OPERATOR	1	=,
	FUNCTION	1	int8eq(int8,int8);

CREATE OPERATOR CLASS text_ops
DEFAULT FOR TYPE text USING bztree AS
	OPERATOR	1	=,
	FUNCTION	1	texteq(text,text);

CREATE OPERATOR CLASS varchar_ops
DEFAULT FOR TYPE varchar USING bztree AS
	OPERATOR	1	=(text,text),
	FUNCTION	1	texteq(text,text);

CREATE OPERATOR CLASS char_ops
DEFAULT FOR TYPE "char" USING bztree AS
	OPERATOR	1	=,
	FUNCTION	1	chareq("char","char");

CREATE OPERATOR CLASS bytea_ops
DEFAULT FOR TYPE bytea USING bztree AS
	OPERATOR	1	=,
	FUNCTION	1	byteaeq(bytea,bytea);

CREATE OPERATOR CLASS bit_ops
DEFAULT FOR TYPE bit USING bztree AS
	OPERATOR	1	=,
	FUNCTION	1	biteq(bit,bit);

CREATE OPERATOR CLASS inet_ops
DEFAULT FOR TYPE inet USING bztree AS
	OPERATOR	1	=,
	FUNCTION	1	network_eq(inet,inet);

CREATE OPERATOR CLASS macaddr_ops
DEFAULT FOR TYPE macaddr USING bztree AS
	OPERATOR	1	=,
	FUNCTION	1	macaddr_eq(macaddr,macaddr);

CREATE OPERATOR CLASS oid_ops
DEFAULT FOR TYPE oid USING bztree AS
	OPERATOR	1	=,
	FUNCTION	1	oideq(oid,oid);

