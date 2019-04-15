/* contrib/test_pglz/test_pglz--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_pglz" to load this file. \quit

CREATE FUNCTION test_pglz()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
