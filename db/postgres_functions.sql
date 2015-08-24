--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

--
-- Name: cassandra_new_node(); Type: FUNCTION; Schema: public; Owner: root
--

CREATE FUNCTION cassandra_new_node() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    total_nodes integer;
BEGIN
    -- Determine anticipated node count from our table name, cassandra_30 = 30 nodes
    total_nodes := split_part(TG_TABLE_NAME, '_', 2)::integer;

    -- Calculate the keyspace column
    IF NEW.keyspace IS NULL THEN
        --                      Start nodes at 0
        NEW.keyspace := to_char((NEW.nodeid-1) * (2::numeric ^ 127) / total_nodes, '9999999999999999999999999999999999999999');
    END IF;

    RETURN NEW;
END;
$$;


ALTER FUNCTION public.cassandra_new_node() OWNER TO root;

--
-- Name: new_node(); Type: FUNCTION; Schema: public; Owner: root
--

CREATE FUNCTION new_node() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- In case we set up listeners later
    NOTIFY newnode;
    RETURN NULL;
END;
$$;


ALTER FUNCTION public.new_node() OWNER TO root;

--
-- PostgreSQL database dump complete
--

