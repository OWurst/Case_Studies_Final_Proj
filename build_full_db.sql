--
-- PostgreSQL database dump
--

\restrict ajQkectL9gMskYmbI0F2cy5JMto7dKKixGrFNInbzaOpxUtndwywyYCr03umRVl

-- Dumped from database version 18.0
-- Dumped by pg_dump version 18.0

-- Started on 2026-04-03 22:19:15

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 12 (class 2615 OID 82445)
-- Name: ml; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA ml;


ALTER SCHEMA ml OWNER TO postgres;

--
-- TOC entry 7 (class 2615 OID 32768)
-- Name: net; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA net;


ALTER SCHEMA net OWNER TO postgres;

--
-- TOC entry 6 (class 2615 OID 24577)
-- Name: wts; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA wts;


ALTER SCHEMA wts OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 259 (class 1259 OID 57833)
-- Name: graph_build_run; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.graph_build_run (
    run_id bigint NOT NULL,
    started_at timestamp without time zone DEFAULT now() NOT NULL,
    finished_at timestamp without time zone,
    status character varying(20) DEFAULT 'RUNNING'::character varying NOT NULL,
    start_date date NOT NULL,
    notes text
);


ALTER TABLE net.graph_build_run OWNER TO postgres;

--
-- TOC entry 258 (class 1259 OID 57832)
-- Name: graph_build_run_run_id_seq; Type: SEQUENCE; Schema: net; Owner: postgres
--

CREATE SEQUENCE net.graph_build_run_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE net.graph_build_run_run_id_seq OWNER TO postgres;

--
-- TOC entry 5239 (class 0 OID 0)
-- Dependencies: 258
-- Name: graph_build_run_run_id_seq; Type: SEQUENCE OWNED BY; Schema: net; Owner: postgres
--

ALTER SEQUENCE net.graph_build_run_run_id_seq OWNED BY net.graph_build_run.run_id;


--
-- TOC entry 260 (class 1259 OID 57847)
-- Name: graph_build_step; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.graph_build_step (
    run_id bigint NOT NULL,
    year_quarter integer NOT NULL,
    step_name character varying(100) NOT NULL,
    started_at timestamp without time zone DEFAULT now() NOT NULL,
    finished_at timestamp without time zone,
    status character varying(20) DEFAULT 'RUNNING'::character varying NOT NULL,
    detail text
);


ALTER TABLE net.graph_build_step OWNER TO postgres;

--
-- TOC entry 255 (class 1259 OID 57757)
-- Name: graph_edge_generator_facility_stream_qtr; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.graph_edge_generator_facility_stream_qtr (
    edge_id bigint NOT NULL,
    year integer NOT NULL,
    quarter integer NOT NULL,
    year_quarter integer NOT NULL,
    generator_node_id bigint CONSTRAINT graph_edge_generator_facility_stream_generator_node_id_not_null NOT NULL,
    facility_node_id bigint CONSTRAINT graph_edge_generator_facility_stream__facility_node_id_not_null NOT NULL,
    waste_stream_id bigint CONSTRAINT graph_edge_generator_facility_stream_q_waste_stream_id_not_null NOT NULL,
    manifest_count bigint,
    waste_line_count bigint,
    total_waste_tons double precision,
    total_waste_kg double precision,
    unique_transporters bigint,
    first_shipped_date timestamp without time zone,
    last_shipped_date timestamp without time zone,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE net.graph_edge_generator_facility_stream_qtr OWNER TO postgres;

--
-- TOC entry 254 (class 1259 OID 57756)
-- Name: graph_edge_generator_facility_stream_qtr_edge_id_seq; Type: SEQUENCE; Schema: net; Owner: postgres
--

CREATE SEQUENCE net.graph_edge_generator_facility_stream_qtr_edge_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE net.graph_edge_generator_facility_stream_qtr_edge_id_seq OWNER TO postgres;

--
-- TOC entry 5240 (class 0 OID 0)
-- Dependencies: 254
-- Name: graph_edge_generator_facility_stream_qtr_edge_id_seq; Type: SEQUENCE OWNED BY; Schema: net; Owner: postgres
--

ALTER SEQUENCE net.graph_edge_generator_facility_stream_qtr_edge_id_seq OWNED BY net.graph_edge_generator_facility_stream_qtr.edge_id;


--
-- TOC entry 243 (class 1259 OID 40979)
-- Name: graph_edge_generator_facility_year; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.graph_edge_generator_facility_year (
    edge_id bigint NOT NULL,
    year integer NOT NULL,
    generator_node_id bigint NOT NULL,
    facility_node_id bigint NOT NULL,
    manifest_count bigint,
    total_waste_tons double precision,
    total_waste_kg double precision,
    unique_transporters bigint,
    unique_waste_codes bigint,
    rejection_manifest_count bigint,
    import_manifest_count bigint,
    first_shipped_date timestamp without time zone,
    last_shipped_date timestamp without time zone,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE net.graph_edge_generator_facility_year OWNER TO postgres;

--
-- TOC entry 242 (class 1259 OID 40978)
-- Name: graph_edge_generator_facility_year_edge_id_seq; Type: SEQUENCE; Schema: net; Owner: postgres
--

CREATE SEQUENCE net.graph_edge_generator_facility_year_edge_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE net.graph_edge_generator_facility_year_edge_id_seq OWNER TO postgres;

--
-- TOC entry 5241 (class 0 OID 0)
-- Dependencies: 242
-- Name: graph_edge_generator_facility_year_edge_id_seq; Type: SEQUENCE OWNED BY; Schema: net; Owner: postgres
--

ALTER SEQUENCE net.graph_edge_generator_facility_year_edge_id_seq OWNED BY net.graph_edge_generator_facility_year.edge_id;


--
-- TOC entry 257 (class 1259 OID 57788)
-- Name: graph_edge_generator_transporter_stream_qtr; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.graph_edge_generator_transporter_stream_qtr (
    edge_id bigint NOT NULL,
    year integer NOT NULL,
    quarter integer NOT NULL,
    year_quarter integer CONSTRAINT graph_edge_generator_transporter_stream_q_year_quarter_not_null NOT NULL,
    generator_node_id bigint CONSTRAINT graph_edge_generator_transporter_str_generator_node_id_not_null NOT NULL,
    transporter_node_id bigint CONSTRAINT graph_edge_generator_transporter_s_transporter_node_id_not_null NOT NULL,
    waste_stream_id bigint CONSTRAINT graph_edge_generator_transporter_strea_waste_stream_id_not_null NOT NULL,
    manifest_count bigint,
    waste_line_count bigint,
    total_waste_tons double precision,
    total_waste_kg double precision,
    unique_facilities bigint,
    first_shipped_date timestamp without time zone,
    last_shipped_date timestamp without time zone,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE net.graph_edge_generator_transporter_stream_qtr OWNER TO postgres;

--
-- TOC entry 256 (class 1259 OID 57787)
-- Name: graph_edge_generator_transporter_stream_qtr_edge_id_seq; Type: SEQUENCE; Schema: net; Owner: postgres
--

CREATE SEQUENCE net.graph_edge_generator_transporter_stream_qtr_edge_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE net.graph_edge_generator_transporter_stream_qtr_edge_id_seq OWNER TO postgres;

--
-- TOC entry 5242 (class 0 OID 0)
-- Dependencies: 256
-- Name: graph_edge_generator_transporter_stream_qtr_edge_id_seq; Type: SEQUENCE OWNED BY; Schema: net; Owner: postgres
--

ALTER SEQUENCE net.graph_edge_generator_transporter_stream_qtr_edge_id_seq OWNED BY net.graph_edge_generator_transporter_stream_qtr.edge_id;


--
-- TOC entry 245 (class 1259 OID 41007)
-- Name: graph_edge_generator_transporter_year; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.graph_edge_generator_transporter_year (
    edge_id bigint NOT NULL,
    year integer NOT NULL,
    generator_node_id bigint CONSTRAINT graph_edge_generator_transporter_yea_generator_node_id_not_null NOT NULL,
    transporter_node_id bigint CONSTRAINT graph_edge_generator_transporter_y_transporter_node_id_not_null NOT NULL,
    manifest_count bigint,
    total_waste_tons double precision,
    total_waste_kg double precision,
    unique_facilities bigint,
    unique_waste_codes bigint,
    first_shipped_date timestamp without time zone,
    last_shipped_date timestamp without time zone,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE net.graph_edge_generator_transporter_year OWNER TO postgres;

--
-- TOC entry 244 (class 1259 OID 41006)
-- Name: graph_edge_generator_transporter_year_edge_id_seq; Type: SEQUENCE; Schema: net; Owner: postgres
--

CREATE SEQUENCE net.graph_edge_generator_transporter_year_edge_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE net.graph_edge_generator_transporter_year_edge_id_seq OWNER TO postgres;

--
-- TOC entry 5243 (class 0 OID 0)
-- Dependencies: 244
-- Name: graph_edge_generator_transporter_year_edge_id_seq; Type: SEQUENCE OWNED BY; Schema: net; Owner: postgres
--

ALTER SEQUENCE net.graph_edge_generator_transporter_year_edge_id_seq OWNED BY net.graph_edge_generator_transporter_year.edge_id;


--
-- TOC entry 251 (class 1259 OID 57367)
-- Name: graph_edge_transporter_facility_stream_qtr; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.graph_edge_transporter_facility_stream_qtr (
    edge_id bigint NOT NULL,
    year integer NOT NULL,
    quarter integer NOT NULL,
    year_quarter integer CONSTRAINT graph_edge_transporter_facility_stream_qt_year_quarter_not_null NOT NULL,
    transporter_node_id bigint CONSTRAINT graph_edge_transporter_facility_st_transporter_node_id_not_null NOT NULL,
    facility_node_id bigint CONSTRAINT graph_edge_transporter_facility_strea_facility_node_id_not_null NOT NULL,
    waste_stream_id bigint CONSTRAINT graph_edge_transporter_facility_stream_waste_stream_id_not_null NOT NULL,
    manifest_count bigint,
    waste_line_count bigint,
    total_waste_tons double precision,
    total_waste_kg double precision,
    unique_generators bigint,
    first_shipped_date timestamp without time zone,
    last_shipped_date timestamp without time zone,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE net.graph_edge_transporter_facility_stream_qtr OWNER TO postgres;

--
-- TOC entry 250 (class 1259 OID 57366)
-- Name: graph_edge_transporter_facility_stream_qtr_edge_id_seq; Type: SEQUENCE; Schema: net; Owner: postgres
--

CREATE SEQUENCE net.graph_edge_transporter_facility_stream_qtr_edge_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE net.graph_edge_transporter_facility_stream_qtr_edge_id_seq OWNER TO postgres;

--
-- TOC entry 5244 (class 0 OID 0)
-- Dependencies: 250
-- Name: graph_edge_transporter_facility_stream_qtr_edge_id_seq; Type: SEQUENCE OWNED BY; Schema: net; Owner: postgres
--

ALTER SEQUENCE net.graph_edge_transporter_facility_stream_qtr_edge_id_seq OWNED BY net.graph_edge_transporter_facility_stream_qtr.edge_id;


--
-- TOC entry 247 (class 1259 OID 41035)
-- Name: graph_edge_transporter_facility_year; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.graph_edge_transporter_facility_year (
    edge_id bigint NOT NULL,
    year integer NOT NULL,
    transporter_node_id bigint CONSTRAINT graph_edge_transporter_facility_ye_transporter_node_id_not_null NOT NULL,
    facility_node_id bigint NOT NULL,
    manifest_count bigint,
    total_waste_tons double precision,
    total_waste_kg double precision,
    unique_generators bigint,
    unique_waste_codes bigint,
    first_shipped_date timestamp without time zone,
    last_shipped_date timestamp without time zone,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE net.graph_edge_transporter_facility_year OWNER TO postgres;

--
-- TOC entry 246 (class 1259 OID 41034)
-- Name: graph_edge_transporter_facility_year_edge_id_seq; Type: SEQUENCE; Schema: net; Owner: postgres
--

CREATE SEQUENCE net.graph_edge_transporter_facility_year_edge_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE net.graph_edge_transporter_facility_year_edge_id_seq OWNER TO postgres;

--
-- TOC entry 5245 (class 0 OID 0)
-- Dependencies: 246
-- Name: graph_edge_transporter_facility_year_edge_id_seq; Type: SEQUENCE OWNED BY; Schema: net; Owner: postgres
--

ALTER SEQUENCE net.graph_edge_transporter_facility_year_edge_id_seq OWNED BY net.graph_edge_transporter_facility_year.edge_id;


--
-- TOC entry 241 (class 1259 OID 40961)
-- Name: graph_node; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.graph_node (
    graph_node_id bigint NOT NULL,
    node_type character varying(30) NOT NULL,
    business_id character varying(30) NOT NULL,
    business_name character varying(255),
    state_code character varying(2),
    source_table character varying(50),
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE net.graph_node OWNER TO postgres;

--
-- TOC entry 240 (class 1259 OID 40960)
-- Name: graph_node_graph_node_id_seq; Type: SEQUENCE; Schema: net; Owner: postgres
--

CREATE SEQUENCE net.graph_node_graph_node_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE net.graph_node_graph_node_id_seq OWNER TO postgres;

--
-- TOC entry 5246 (class 0 OID 0)
-- Dependencies: 240
-- Name: graph_node_graph_node_id_seq; Type: SEQUENCE OWNED BY; Schema: net; Owner: postgres
--

ALTER SEQUENCE net.graph_node_graph_node_id_seq OWNED BY net.graph_node.graph_node_id;


--
-- TOC entry 261 (class 1259 OID 66292)
-- Name: graph_stage_status; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.graph_stage_status (
    year_quarter integer NOT NULL,
    waste_line_fact_ready boolean DEFAULT false NOT NULL,
    manifest_transporter_ready boolean DEFAULT false NOT NULL,
    staged_at timestamp without time zone,
    notes text
);


ALTER TABLE net.graph_stage_status OWNER TO postgres;

--
-- TOC entry 249 (class 1259 OID 57349)
-- Name: graph_waste_stream; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.graph_waste_stream (
    waste_stream_id bigint NOT NULL,
    waste_stream_key text NOT NULL,
    usdot_hazardous_indicator character varying(1),
    primary_description character varying(500),
    usdot_description character varying(500),
    non_hazardous_waste_description character varying(500),
    management_method_code character varying(4),
    management_method_description character varying(100),
    form_code character varying(4),
    form_code_description character varying(125),
    source_code character varying(3),
    source_code_description character varying(125),
    container_type_code character varying(2),
    container_type_description character varying(50),
    display_name text,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE net.graph_waste_stream OWNER TO postgres;

--
-- TOC entry 248 (class 1259 OID 57348)
-- Name: graph_waste_stream_waste_stream_id_seq; Type: SEQUENCE; Schema: net; Owner: postgres
--

CREATE SEQUENCE net.graph_waste_stream_waste_stream_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE net.graph_waste_stream_waste_stream_id_seq OWNER TO postgres;

--
-- TOC entry 5247 (class 0 OID 0)
-- Dependencies: 248
-- Name: graph_waste_stream_waste_stream_id_seq; Type: SEQUENCE OWNED BY; Schema: net; Owner: postgres
--

ALTER SEQUENCE net.graph_waste_stream_waste_stream_id_seq OWNED BY net.graph_waste_stream.waste_stream_id;


--
-- TOC entry 253 (class 1259 OID 57752)
-- Name: stg_manifest_transporter; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.stg_manifest_transporter (
    "ManifestTrackingNumber" character varying(12),
    "TransporterEPAID" character varying(15)
);


ALTER TABLE net.stg_manifest_transporter OWNER TO postgres;

--
-- TOC entry 266 (class 1259 OID 66341)
-- Name: stg_manifest_transporter_qtr; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.stg_manifest_transporter_qtr (
    year_quarter integer,
    "ManifestTrackingNumber" character varying(12),
    "TransporterEPAID" character varying(15)
);


ALTER TABLE net.stg_manifest_transporter_qtr OWNER TO postgres;

--
-- TOC entry 263 (class 1259 OID 66314)
-- Name: stg_top_facilities; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.stg_top_facilities (
    "DesignatedFacilityEPAID" character varying(12) NOT NULL,
    row_count bigint NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE net.stg_top_facilities OWNER TO postgres;

--
-- TOC entry 264 (class 1259 OID 66323)
-- Name: stg_top_generators; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.stg_top_generators (
    "GeneratorEPAID" character varying(15) NOT NULL,
    row_count bigint NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE net.stg_top_generators OWNER TO postgres;

--
-- TOC entry 265 (class 1259 OID 66332)
-- Name: stg_top_transporters; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.stg_top_transporters (
    "TransporterEPAID" character varying(15) NOT NULL,
    row_count bigint NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE net.stg_top_transporters OWNER TO postgres;

--
-- TOC entry 262 (class 1259 OID 66305)
-- Name: stg_top_wasteline_numbers; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.stg_top_wasteline_numbers (
    "WasteLineNumber" integer NOT NULL,
    row_count bigint NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE net.stg_top_wasteline_numbers OWNER TO postgres;

--
-- TOC entry 252 (class 1259 OID 57747)
-- Name: stg_waste_line_fact_qtr; Type: TABLE; Schema: net; Owner: postgres
--

CREATE TABLE net.stg_waste_line_fact_qtr (
    "ManifestTrackingNumber" character varying(12),
    "WasteLineNumber" integer,
    "ShippedDate" timestamp without time zone,
    year integer,
    quarter integer,
    year_quarter integer,
    "GeneratorEPAID" character varying(15),
    "DesignatedFacilityEPAID" character varying(12),
    usdot_hazardous_indicator character varying(1),
    usdot_description character varying(500),
    non_hazardous_waste_description character varying(500),
    management_method_code character varying(4),
    management_method_description character varying(100),
    form_code character varying(4),
    form_code_description character varying(125),
    source_code character varying(3),
    source_code_description character varying(125),
    container_type_code character varying(2),
    container_type_description character varying(50),
    primary_description character varying(500),
    waste_quantity_tons double precision,
    waste_quantity_kg double precision,
    unique_transporters bigint,
    waste_stream_key text
);


ALTER TABLE net.stg_waste_line_fact_qtr OWNER TO postgres;

--
-- TOC entry 226 (class 1259 OID 24578)
-- Name: EM_FEDERAL_WASTE_CODE; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."EM_FEDERAL_WASTE_CODE" (
    "FederalWasteCodeID" bigint NOT NULL,
    "ManifestTrackingNumber" character varying(12),
    "WasteLineNumber" integer,
    "FederalWasteCode" character varying(4),
    "WTSCreateDate" timestamp without time zone NOT NULL
);


ALTER TABLE wts."EM_FEDERAL_WASTE_CODE" OWNER TO postgres;

--
-- TOC entry 227 (class 1259 OID 24583)
-- Name: EM_IMPORT; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."EM_IMPORT" (
    "ImportID" bigint NOT NULL,
    "ManifestTrackingNumber" character varying(12),
    "PortofEntryState" character varying(2),
    "PortofEntryCity" character varying(100),
    "ForeignGeneratorName" character varying(80),
    "ForeignGeneratorAddress" character varying(50),
    "ForeignGeneratorCity" character varying(25),
    "ForeignGeneratorPostalCode" character varying(15),
    "ForeignGeneratorProvince" character varying(50),
    "ForeignGeneratorCountryCode" character varying(2),
    "ForeignGeneratorCountryName" character varying(100),
    "WTSCreateDate" timestamp without time zone NOT NULL
);


ALTER TABLE wts."EM_IMPORT" OWNER TO postgres;

--
-- TOC entry 228 (class 1259 OID 24588)
-- Name: EM_MANIFEST; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."EM_MANIFEST" (
    "EmanifestID" bigint NOT NULL,
    "ManifestTrackingNumber" character varying(12),
    "LastUpdatedDate" timestamp without time zone,
    "ShippedDate" timestamp without time zone,
    "ReceivedDate" timestamp without time zone,
    "ManifestStatus" character varying(20),
    "SubmissionType" character varying(15),
    "OriginType" character varying(10),
    "GeneratorEPAID" character varying(15),
    "GeneratorName" character varying(80),
    "GeneratorMailStreetNumber" character varying(12),
    "GeneratorMailStreet1" character varying(50),
    "GeneratorMailStreet2" character varying(50),
    "GeneratorMailCity" character varying(25),
    "GeneratorMailZip" character varying(14),
    "GeneratorMailState" character varying(2),
    "GeneratorLocationStreetNumber" character varying(12),
    "GeneratorLocationStreet1" character varying(50),
    "GeneratorLocationStreet2" character varying(50),
    "GeneratorLocationCity" character varying(25),
    "GeneratorLocationZip" character varying(14),
    "GeneratorLocationState" character varying(2),
    "GeneratorContactCompanyName" character varying(80),
    "DesignatedFacilityEPAID" character varying(12),
    "DesignatedFacilityName" character varying(80),
    "DesignatedFacilityMailStreetNumber" character varying(12),
    "DesignatedFacilityMailStreet1" character varying(50),
    "DesignatedFacilityMailStreet2" character varying(50),
    "DesignatedFacilityMailCity" character varying(25),
    "DesignatedFacilityMailZip" character varying(14),
    "DesignatedFacilityMailState" character varying(2),
    "DesignatedFacilityLocationStreetNumber" character varying(12),
    "DesignatedFacilityLocationStreet1" character varying(50),
    "DesignatedFacilityLocationStreet2" character varying(50),
    "DesignatedFacilityLocationCity" character varying(25),
    "DesignatedFacilityLocationZip" character varying(14),
    "DesignatedFacilityLocationState" character varying(2),
    "DesignatedFacilityContactCompanyName" character varying(80),
    "ManifestResidueIndicator" character varying(1),
    "RejectionIndicator" character varying(1),
    "TotalAcuteWasteQuantityInKilograms" double precision,
    "TotalAcuteWasteQuantityInTons" double precision,
    "TotalHazardousWasteQuantityInKilograms" double precision,
    "TotalHazardousWasteQuantityInTons" double precision,
    "TotalNonAcuteWasteQuantityInKilograms" double precision,
    "TotalNonAcuteWasteQuantityInTons" double precision,
    "TotalNonHazardousWasteQuantityInKilograms" double precision,
    "TotalNonHazardousWasteQuantityInTons" double precision,
    "TotalWasteQuantityInKilograms" double precision,
    "TotalWasteQuantityInTons" double precision,
    "BrokerID" character varying(15),
    "WTSCreateDate" timestamp without time zone NOT NULL
);


ALTER TABLE wts."EM_MANIFEST" OWNER TO postgres;

--
-- TOC entry 229 (class 1259 OID 24595)
-- Name: EM_PCB_INFO; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."EM_PCB_INFO" (
    "PCBInfoID" bigint NOT NULL,
    "ManifestTrackingNumber" character varying(12),
    "WasteLineNumber" integer,
    "ArticleorContainerID" character varying(255),
    "BulkIdentity" character varying(255),
    "DateofRemoval" timestamp without time zone,
    "LoadType" character varying(100),
    "LoadTypeDescription" character varying(100),
    "WasteType" character varying(100),
    "Weight" double precision,
    "WTSCreateDate" timestamp without time zone NOT NULL
);


ALTER TABLE wts."EM_PCB_INFO" OWNER TO postgres;

--
-- TOC entry 230 (class 1259 OID 24602)
-- Name: EM_REJECTION; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."EM_REJECTION" (
    "RejectionID" bigint NOT NULL,
    "ManifestTrackingNumber" character varying(12),
    "RejectionTypeIndicator" character varying(15),
    "RejectionTransporterOnsiteIndicator" character varying(1),
    "AlternateDestinationFacilityType" character varying(12),
    "AlternateDesignatedFacilityEPAID" character varying(12),
    "AlternateDesignatedFacilityName" character varying(80),
    "AlternateDesignatedFacilityMailStreetNumber" character varying(12),
    "AlternateDesignatedFacilityMailStreet1" character varying(50),
    "AlternateDesignatedFacilityMailStreet2" character varying(50),
    "AlternateDesignatedFacilityMailCity" character varying(25),
    "AlternateDesignatedFacilityMailZip" character varying(14),
    "AlternateDesignatedFacilityMailState" character varying(2),
    "AlternateDesignatedFacilityLocationStreetNumber" character varying(12),
    "AlternateDesignatedFacilityLocationStreet1" character varying(50),
    "AlternateDesignatedFacilityLocationStreet2" character varying(50),
    "AlternateDesignatedFacilityLocationCity" character varying(25),
    "AlternateDesignatedFacilityLocationZip" character varying(14),
    "AlternateDesignatedFacilityLocationState" character varying(2),
    "AlternateDesignatedFacilityContactCompanyName" character varying(80),
    "WTSCreateDate" timestamp without time zone NOT NULL
);


ALTER TABLE wts."EM_REJECTION" OWNER TO postgres;

--
-- TOC entry 231 (class 1259 OID 24609)
-- Name: EM_STAGE; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."EM_STAGE" (
    "ImportData" text NOT NULL
);


ALTER TABLE wts."EM_STAGE" OWNER TO postgres;

--
-- TOC entry 232 (class 1259 OID 24615)
-- Name: EM_STATE_WASTE_CODE; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."EM_STATE_WASTE_CODE" (
    "StateWasteCodeID" bigint NOT NULL,
    "ManifestTrackingNumber" character varying(12),
    "WasteLineNumber" integer,
    "StateWasteCodeOwner" character varying(2),
    "StateWasteCode" character varying(8),
    "WTSCreateDate" timestamp without time zone NOT NULL
);


ALTER TABLE wts."EM_STATE_WASTE_CODE" OWNER TO postgres;

--
-- TOC entry 233 (class 1259 OID 24620)
-- Name: EM_TRANSPORTER; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."EM_TRANSPORTER" (
    "TransporterID" bigint NOT NULL,
    "ManifestTrackingNumber" character varying(12),
    "TransporterLineNumber" bigint,
    "TransporterEPAID" character varying(15),
    "TransporterName" character varying(80),
    "WTSCreateDate" timestamp without time zone NOT NULL
);


ALTER TABLE wts."EM_TRANSPORTER" OWNER TO postgres;

--
-- TOC entry 234 (class 1259 OID 24625)
-- Name: EM_WASTE_LINE; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."EM_WASTE_LINE" (
    "WasteLineID" bigint NOT NULL,
    "ManifestTrackingNumber" character varying(12),
    "WasteLineNumber" integer,
    "USDOTHazardousIndicator" character varying(1),
    "USDOTIDNumber" character varying(6),
    "USDOTDescription" character varying(500),
    "NonHazardousWasteDescription" character varying(500),
    "NumberofContainers" character varying(6),
    "ContainerTypeCode" character varying(2),
    "ContainerTypeDescription" character varying(50),
    "WasteQuantity" bigint,
    "QuantityUnitofMeasureCode" character varying(1),
    "QuantityUnitofMeasureDescription" character varying(40),
    "WasteQuantityTons" double precision,
    "AcuteWasteQuantityTons" double precision,
    "NonAcuteWasteQuantityTons" double precision,
    "WasteQuantityKilograms" double precision,
    "AcuteWasteQuantityKilograms" double precision,
    "NonAcuteWasteQuantityKilorgrams" double precision,
    "ManagementMethodCode" character varying(4),
    "ManagementMethodDescription" character varying(100),
    "WasteResidueIndicator" character varying(1),
    "QuantityDiscrepancyIndicator" character varying(1),
    "WasteTypeDiscrepancyIndicator" character varying(1),
    "WasteDensity" double precision,
    "WasteDensityUnitofMeasureCode" character varying(1),
    "WasteDensityUnitofMeasureDescription" character varying(50),
    "FormCode" character varying(4),
    "FormCodeDescription" character varying(125),
    "SourceCode" character varying(3),
    "SourceCodeDescription" character varying(125),
    "WasteMinimizationCode" character varying(1),
    "WasteMinimizationCodeDescription" character varying(100),
    "ConsentNumber" character varying(12),
    "EPAWasteIndicator" character varying(1),
    "HazardousWasteQuantityInKilograms" double precision,
    "HazardousWasteQuantityInTons" double precision,
    "NonHazardousWasteQuantityInTons" double precision,
    "NonHazardousWasteQuantityInKilograms" double precision,
    "WTSCreateDate" timestamp without time zone NOT NULL
);


ALTER TABLE wts."EM_WASTE_LINE" OWNER TO postgres;

--
-- TOC entry 235 (class 1259 OID 24632)
-- Name: HD_HANDLER; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."HD_HANDLER" (
    "HandlerID" bigint NOT NULL,
    "EPAHandlerID" character varying(12),
    "ActivityLocation" character varying(2),
    "SourceType" character varying(1),
    "SequenceNumber" integer,
    "ReceiveDate" timestamp without time zone,
    "HandlerName" character varying(80),
    "NonNotifier" character varying(1),
    "AcknowledgeFlagDate" timestamp without time zone,
    "AcknowledgeFlag" character varying(1),
    "Accessibility" character varying(1),
    "LocationStreetNumber" character varying(12),
    "LocationStreet1" character varying(50),
    "LocationStreet2" character varying(50),
    "LocationCity" character varying(25),
    "LocationState" character varying(2),
    "LocationZipCode" character varying(14),
    "LocationCountry" character varying(2),
    "CountyCode" character varying(5),
    "StateDistrictOwner" character varying(2),
    "StateDistrict" character varying(10),
    "LandType" character varying(1),
    "MailingStreetNumber" character varying(12),
    "MailingStreet1" character varying(50),
    "MailingStreet2" character varying(50),
    "MailingCity" character varying(25),
    "MailingState" character varying(2),
    "MailingZipCode" character varying(14),
    "MailingCountry" character varying(2),
    "ContactFirstName" character varying(38),
    "ContactMiddleInitial" character varying(1),
    "ContactLastName" character varying(38),
    "ContactStreetNumber" character varying(12),
    "ContactStreet1" character varying(50),
    "ContactStreet2" character varying(50),
    "ContactCity" character varying(25),
    "ContactState" character varying(2),
    "ContactZipCode" character varying(14),
    "ContactCountry" character varying(2),
    "ContactTelephoneNumber" character varying(15),
    "ContactTelephoneExtension" character varying(6),
    "ContactFacsimileNumber" character varying(15),
    "ContactEmailAddress" character varying(80),
    "ContactTitle" character varying(45),
    "FederalWasteGeneratorCodeOwner" character varying(2),
    "FederalWasteGeneratorCode" character varying(1),
    "StateWasteGeneratorCodeOwner" character varying(2),
    "StateWasteGeneratorCode" character varying(1),
    "ShortTermGeneratorActivity" character varying(1),
    "ImporterActivity" character varying(1),
    "MixedWasteGenerator" character varying(1),
    "TransporterActivity" character varying(1),
    "TransferFacilityActivity" character varying(1),
    "TSDActivity" character varying(1),
    "RecyclerActivitywithStorage" character varying(1),
    "SmallQuantityOnsiteBurnerExemption" character varying(1),
    "SmeltingMeltingandRefiningFurnaceExemption" character varying(1),
    "UndergroundInjectionControl" character varying(1),
    "OffsiteWasteReceipt" character varying(1),
    "UniversalWasteDestinationFacility" character varying(1),
    "UsedOilTransporter" character varying(1),
    "UsedOilTransferFacility" character varying(1),
    "UsedOilProcessor" character varying(1),
    "UsedOilRefiner" character varying(1),
    "OffspecificationUsedOilBurner" character varying(1),
    "MarketerWhoDirectsShipmentofOffspecificationUsedOiltoOffspecifi" character varying(1),
    "MarketerWhoFirstClaimstheUsedOilMeetstheSpecifications" character varying(1),
    "SubpartKCollegeorUniversity" character varying(1),
    "SubpartKTeachingHospital" character varying(1),
    "SubpartKNonprofitResearchInstitute" character varying(1),
    "SubpartKWithdrawal" character varying(1),
    "IncludeinNationalReport" character varying(1),
    "BiennialReportCycle" integer,
    "LargeQuantityHandlerofUniversalWaste" character varying(1),
    "RecognizedTraderImporter" character varying(1),
    "RecognizedTraderExporter" character varying(1),
    "SpentLeadAcidBatteryImporter" character varying(1),
    "SpentLeadAcidBatteryExporter" character varying(1),
    "CurrentRecord" character varying(1),
    "NonstorageRecyclerActivity" character varying(1),
    "ElectronicManifestBroker" character varying(1),
    "PublicNotes" character varying(4000),
    "SubpartPHeathcareFacility" character varying(1),
    "SubpartPReverseDistributor" character varying(1),
    "SubpartPWithdrawal" character varying(1),
    "LocationLatitude" double precision,
    "LocationLongitude" double precision,
    "LocationGISPrimary" character varying(1),
    "LocationGISOrigin" character varying(1),
    "BiennialReportExemptIndicator" character varying(1),
    "ContactPreferredLanguage" character varying(2),
    "WTSCreateDate" timestamp without time zone NOT NULL
);


ALTER TABLE wts."HD_HANDLER" OWNER TO postgres;

--
-- TOC entry 236 (class 1259 OID 24639)
-- Name: HD_HANDLER_STAGE; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."HD_HANDLER_STAGE" (
    "ImportData" text NOT NULL
);


ALTER TABLE wts."HD_HANDLER_STAGE" OWNER TO postgres;

--
-- TOC entry 237 (class 1259 OID 24645)
-- Name: HD_NAICS; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."HD_NAICS" (
    "NAICSID" bigint NOT NULL,
    "EPAHandlerID" character varying(12),
    "ActivityLocation" character varying(2),
    "SourceType" character varying(1),
    "SequenceNumber" integer,
    "NAICSSequenceNumber" integer,
    "NAICSOwner" character varying(2),
    "NAICSCode" character varying(6),
    "WTSCreateDate" timestamp without time zone NOT NULL
);


ALTER TABLE wts."HD_NAICS" OWNER TO postgres;

--
-- TOC entry 238 (class 1259 OID 24650)
-- Name: HD_NAICS_STAGE; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."HD_NAICS_STAGE" (
    "ImportData" text NOT NULL
);


ALTER TABLE wts."HD_NAICS_STAGE" OWNER TO postgres;

--
-- TOC entry 239 (class 1259 OID 24656)
-- Name: Logs; Type: TABLE; Schema: wts; Owner: postgres
--

CREATE TABLE wts."Logs" (
    "LogId" integer NOT NULL,
    "Level" text NOT NULL,
    "CallSite" text NOT NULL,
    "Type" text NOT NULL,
    "Message" text NOT NULL,
    "StackTrace" text NOT NULL,
    "InnerException" text NOT NULL,
    "AdditionalInfo" text NOT NULL,
    "LoggedOnDate" timestamp without time zone NOT NULL
);


ALTER TABLE wts."Logs" OWNER TO postgres;

--
-- TOC entry 4965 (class 2604 OID 57836)
-- Name: graph_build_run run_id; Type: DEFAULT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_build_run ALTER COLUMN run_id SET DEFAULT nextval('net.graph_build_run_run_id_seq'::regclass);


--
-- TOC entry 4961 (class 2604 OID 57760)
-- Name: graph_edge_generator_facility_stream_qtr edge_id; Type: DEFAULT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_facility_stream_qtr ALTER COLUMN edge_id SET DEFAULT nextval('net.graph_edge_generator_facility_stream_qtr_edge_id_seq'::regclass);


--
-- TOC entry 4951 (class 2604 OID 40982)
-- Name: graph_edge_generator_facility_year edge_id; Type: DEFAULT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_facility_year ALTER COLUMN edge_id SET DEFAULT nextval('net.graph_edge_generator_facility_year_edge_id_seq'::regclass);


--
-- TOC entry 4963 (class 2604 OID 57791)
-- Name: graph_edge_generator_transporter_stream_qtr edge_id; Type: DEFAULT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_transporter_stream_qtr ALTER COLUMN edge_id SET DEFAULT nextval('net.graph_edge_generator_transporter_stream_qtr_edge_id_seq'::regclass);


--
-- TOC entry 4953 (class 2604 OID 41010)
-- Name: graph_edge_generator_transporter_year edge_id; Type: DEFAULT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_transporter_year ALTER COLUMN edge_id SET DEFAULT nextval('net.graph_edge_generator_transporter_year_edge_id_seq'::regclass);


--
-- TOC entry 4959 (class 2604 OID 57370)
-- Name: graph_edge_transporter_facility_stream_qtr edge_id; Type: DEFAULT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_transporter_facility_stream_qtr ALTER COLUMN edge_id SET DEFAULT nextval('net.graph_edge_transporter_facility_stream_qtr_edge_id_seq'::regclass);


--
-- TOC entry 4955 (class 2604 OID 41038)
-- Name: graph_edge_transporter_facility_year edge_id; Type: DEFAULT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_transporter_facility_year ALTER COLUMN edge_id SET DEFAULT nextval('net.graph_edge_transporter_facility_year_edge_id_seq'::regclass);


--
-- TOC entry 4948 (class 2604 OID 40964)
-- Name: graph_node graph_node_id; Type: DEFAULT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_node ALTER COLUMN graph_node_id SET DEFAULT nextval('net.graph_node_graph_node_id_seq'::regclass);


--
-- TOC entry 4957 (class 2604 OID 57352)
-- Name: graph_waste_stream waste_stream_id; Type: DEFAULT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_waste_stream ALTER COLUMN waste_stream_id SET DEFAULT nextval('net.graph_waste_stream_waste_stream_id_seq'::regclass);


--
-- TOC entry 5054 (class 2606 OID 57846)
-- Name: graph_build_run graph_build_run_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_build_run
    ADD CONSTRAINT graph_build_run_pkey PRIMARY KEY (run_id);


--
-- TOC entry 5056 (class 2606 OID 57860)
-- Name: graph_build_step graph_build_step_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_build_step
    ADD CONSTRAINT graph_build_step_pkey PRIMARY KEY (run_id, year_quarter, step_name);


--
-- TOC entry 5044 (class 2606 OID 57771)
-- Name: graph_edge_generator_facility_stream_qtr graph_edge_generator_facility_stream_qtr_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_facility_stream_qtr
    ADD CONSTRAINT graph_edge_generator_facility_stream_qtr_pkey PRIMARY KEY (edge_id);


--
-- TOC entry 5001 (class 2606 OID 40990)
-- Name: graph_edge_generator_facility_year graph_edge_generator_facility_year_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_facility_year
    ADD CONSTRAINT graph_edge_generator_facility_year_pkey PRIMARY KEY (edge_id);


--
-- TOC entry 5049 (class 2606 OID 57802)
-- Name: graph_edge_generator_transporter_stream_qtr graph_edge_generator_transporter_stream_qtr_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_transporter_stream_qtr
    ADD CONSTRAINT graph_edge_generator_transporter_stream_qtr_pkey PRIMARY KEY (edge_id);


--
-- TOC entry 5008 (class 2606 OID 41018)
-- Name: graph_edge_generator_transporter_year graph_edge_generator_transporter_year_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_transporter_year
    ADD CONSTRAINT graph_edge_generator_transporter_year_pkey PRIMARY KEY (edge_id);


--
-- TOC entry 5029 (class 2606 OID 57381)
-- Name: graph_edge_transporter_facility_stream_qtr graph_edge_transporter_facility_stream_qtr_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_transporter_facility_stream_qtr
    ADD CONSTRAINT graph_edge_transporter_facility_stream_qtr_pkey PRIMARY KEY (edge_id);


--
-- TOC entry 5015 (class 2606 OID 41046)
-- Name: graph_edge_transporter_facility_year graph_edge_transporter_facility_year_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_transporter_facility_year
    ADD CONSTRAINT graph_edge_transporter_facility_year_pkey PRIMARY KEY (edge_id);


--
-- TOC entry 4994 (class 2606 OID 40973)
-- Name: graph_node graph_node_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_node
    ADD CONSTRAINT graph_node_pkey PRIMARY KEY (graph_node_id);


--
-- TOC entry 5058 (class 2606 OID 66303)
-- Name: graph_stage_status graph_stage_status_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_stage_status
    ADD CONSTRAINT graph_stage_status_pkey PRIMARY KEY (year_quarter);


--
-- TOC entry 5022 (class 2606 OID 57360)
-- Name: graph_waste_stream graph_waste_stream_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_waste_stream
    ADD CONSTRAINT graph_waste_stream_pkey PRIMARY KEY (waste_stream_id);


--
-- TOC entry 5024 (class 2606 OID 57362)
-- Name: graph_waste_stream graph_waste_stream_waste_stream_key_key; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_waste_stream
    ADD CONSTRAINT graph_waste_stream_waste_stream_key_key UNIQUE (waste_stream_key);


--
-- TOC entry 5062 (class 2606 OID 66322)
-- Name: stg_top_facilities stg_top_facilities_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.stg_top_facilities
    ADD CONSTRAINT stg_top_facilities_pkey PRIMARY KEY ("DesignatedFacilityEPAID");


--
-- TOC entry 5064 (class 2606 OID 66331)
-- Name: stg_top_generators stg_top_generators_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.stg_top_generators
    ADD CONSTRAINT stg_top_generators_pkey PRIMARY KEY ("GeneratorEPAID");


--
-- TOC entry 5066 (class 2606 OID 66340)
-- Name: stg_top_transporters stg_top_transporters_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.stg_top_transporters
    ADD CONSTRAINT stg_top_transporters_pkey PRIMARY KEY ("TransporterEPAID");


--
-- TOC entry 5060 (class 2606 OID 66313)
-- Name: stg_top_wasteline_numbers stg_top_wasteline_numbers_pkey; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.stg_top_wasteline_numbers
    ADD CONSTRAINT stg_top_wasteline_numbers_pkey PRIMARY KEY ("WasteLineNumber");


--
-- TOC entry 5006 (class 2606 OID 40992)
-- Name: graph_edge_generator_facility_year uq_gef; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_facility_year
    ADD CONSTRAINT uq_gef UNIQUE (year, generator_node_id, facility_node_id);


--
-- TOC entry 5013 (class 2606 OID 41020)
-- Name: graph_edge_generator_transporter_year uq_get; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_transporter_year
    ADD CONSTRAINT uq_get UNIQUE (year, generator_node_id, transporter_node_id);


--
-- TOC entry 4998 (class 2606 OID 40975)
-- Name: graph_node uq_graph_node; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_node
    ADD CONSTRAINT uq_graph_node UNIQUE (node_type, business_id);


--
-- TOC entry 5020 (class 2606 OID 41048)
-- Name: graph_edge_transporter_facility_year uq_gtf; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_transporter_facility_year
    ADD CONSTRAINT uq_gtf UNIQUE (year, transporter_node_id, facility_node_id);


--
-- TOC entry 5034 (class 2606 OID 57383)
-- Name: graph_edge_transporter_facility_stream_qtr uq_gtf_sq; Type: CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_transporter_facility_stream_qtr
    ADD CONSTRAINT uq_gtf_sq UNIQUE (year_quarter, transporter_node_id, facility_node_id, waste_stream_id);


--
-- TOC entry 5045 (class 1259 OID 74507)
-- Name: ix_gef_sq_yearq_facility; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_gef_sq_yearq_facility ON net.graph_edge_generator_facility_stream_qtr USING btree (year_quarter, facility_node_id);


--
-- TOC entry 5046 (class 1259 OID 74506)
-- Name: ix_gef_sq_yearq_generator; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_gef_sq_yearq_generator ON net.graph_edge_generator_facility_stream_qtr USING btree (year_quarter, generator_node_id);


--
-- TOC entry 5047 (class 1259 OID 74508)
-- Name: ix_gef_sq_yearq_stream; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_gef_sq_yearq_stream ON net.graph_edge_generator_facility_stream_qtr USING btree (year_quarter, waste_stream_id);


--
-- TOC entry 5002 (class 1259 OID 41005)
-- Name: ix_gef_year; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_gef_year ON net.graph_edge_generator_facility_year USING btree (year);


--
-- TOC entry 5003 (class 1259 OID 41004)
-- Name: ix_gef_year_facility; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_gef_year_facility ON net.graph_edge_generator_facility_year USING btree (year, facility_node_id);


--
-- TOC entry 5004 (class 1259 OID 41003)
-- Name: ix_gef_year_generator; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_gef_year_generator ON net.graph_edge_generator_facility_year USING btree (year, generator_node_id);


--
-- TOC entry 5050 (class 1259 OID 74509)
-- Name: ix_get_sq_yearq_generator; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_get_sq_yearq_generator ON net.graph_edge_generator_transporter_stream_qtr USING btree (year_quarter, generator_node_id);


--
-- TOC entry 5051 (class 1259 OID 74511)
-- Name: ix_get_sq_yearq_stream; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_get_sq_yearq_stream ON net.graph_edge_generator_transporter_stream_qtr USING btree (year_quarter, waste_stream_id);


--
-- TOC entry 5052 (class 1259 OID 74510)
-- Name: ix_get_sq_yearq_transporter; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_get_sq_yearq_transporter ON net.graph_edge_generator_transporter_stream_qtr USING btree (year_quarter, transporter_node_id);


--
-- TOC entry 5009 (class 1259 OID 41033)
-- Name: ix_get_year; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_get_year ON net.graph_edge_generator_transporter_year USING btree (year);


--
-- TOC entry 5010 (class 1259 OID 41031)
-- Name: ix_get_year_generator; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_get_year_generator ON net.graph_edge_generator_transporter_year USING btree (year, generator_node_id);


--
-- TOC entry 5011 (class 1259 OID 41032)
-- Name: ix_get_year_transporter; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_get_year_transporter ON net.graph_edge_generator_transporter_year USING btree (year, transporter_node_id);


--
-- TOC entry 4995 (class 1259 OID 40977)
-- Name: ix_graph_node_business_id; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_graph_node_business_id ON net.graph_node USING btree (business_id);


--
-- TOC entry 4996 (class 1259 OID 40976)
-- Name: ix_graph_node_type_business_id; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_graph_node_type_business_id ON net.graph_node USING btree (node_type, business_id);


--
-- TOC entry 5025 (class 1259 OID 57365)
-- Name: ix_graph_waste_stream_mgmt_form_source; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_graph_waste_stream_mgmt_form_source ON net.graph_waste_stream USING btree (management_method_code, form_code, source_code);


--
-- TOC entry 5026 (class 1259 OID 57364)
-- Name: ix_graph_waste_stream_primary_description; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_graph_waste_stream_primary_description ON net.graph_waste_stream USING btree (primary_description);


--
-- TOC entry 5030 (class 1259 OID 74513)
-- Name: ix_gtf_sq_yearq_facility; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_gtf_sq_yearq_facility ON net.graph_edge_transporter_facility_stream_qtr USING btree (year_quarter, facility_node_id);


--
-- TOC entry 5031 (class 1259 OID 74514)
-- Name: ix_gtf_sq_yearq_stream; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_gtf_sq_yearq_stream ON net.graph_edge_transporter_facility_stream_qtr USING btree (year_quarter, waste_stream_id);


--
-- TOC entry 5032 (class 1259 OID 74512)
-- Name: ix_gtf_sq_yearq_transporter; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_gtf_sq_yearq_transporter ON net.graph_edge_transporter_facility_stream_qtr USING btree (year_quarter, transporter_node_id);


--
-- TOC entry 5016 (class 1259 OID 41061)
-- Name: ix_gtf_year; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_gtf_year ON net.graph_edge_transporter_facility_year USING btree (year);


--
-- TOC entry 5017 (class 1259 OID 41060)
-- Name: ix_gtf_year_facility; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_gtf_year_facility ON net.graph_edge_transporter_facility_year USING btree (year, facility_node_id);


--
-- TOC entry 5018 (class 1259 OID 41059)
-- Name: ix_gtf_year_transporter; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_gtf_year_transporter ON net.graph_edge_transporter_facility_year USING btree (year, transporter_node_id);


--
-- TOC entry 5067 (class 1259 OID 66360)
-- Name: ix_stg_mtq_track; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_stg_mtq_track ON net.stg_manifest_transporter_qtr USING btree ("ManifestTrackingNumber");


--
-- TOC entry 5068 (class 1259 OID 66361)
-- Name: ix_stg_mtq_transporter; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_stg_mtq_transporter ON net.stg_manifest_transporter_qtr USING btree ("TransporterEPAID");


--
-- TOC entry 5069 (class 1259 OID 66359)
-- Name: ix_stg_mtq_yearq; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_stg_mtq_yearq ON net.stg_manifest_transporter_qtr USING btree (year_quarter);


--
-- TOC entry 5070 (class 1259 OID 66362)
-- Name: ix_stg_mtq_yearq_track; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_stg_mtq_yearq_track ON net.stg_manifest_transporter_qtr USING btree (year_quarter, "ManifestTrackingNumber");


--
-- TOC entry 5035 (class 1259 OID 66354)
-- Name: ix_stg_wlfq_facility; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_stg_wlfq_facility ON net.stg_waste_line_fact_qtr USING btree ("DesignatedFacilityEPAID");


--
-- TOC entry 5036 (class 1259 OID 66353)
-- Name: ix_stg_wlfq_generator; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_stg_wlfq_generator ON net.stg_waste_line_fact_qtr USING btree ("GeneratorEPAID");


--
-- TOC entry 5037 (class 1259 OID 66355)
-- Name: ix_stg_wlfq_stream; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_stg_wlfq_stream ON net.stg_waste_line_fact_qtr USING btree (waste_stream_key);


--
-- TOC entry 5038 (class 1259 OID 66352)
-- Name: ix_stg_wlfq_track; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_stg_wlfq_track ON net.stg_waste_line_fact_qtr USING btree ("ManifestTrackingNumber");


--
-- TOC entry 5039 (class 1259 OID 66351)
-- Name: ix_stg_wlfq_yearq; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_stg_wlfq_yearq ON net.stg_waste_line_fact_qtr USING btree (year_quarter);


--
-- TOC entry 5040 (class 1259 OID 66357)
-- Name: ix_stg_wlfq_yearq_facility; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_stg_wlfq_yearq_facility ON net.stg_waste_line_fact_qtr USING btree (year_quarter, "DesignatedFacilityEPAID");


--
-- TOC entry 5041 (class 1259 OID 66356)
-- Name: ix_stg_wlfq_yearq_gen_fac_stream; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_stg_wlfq_yearq_gen_fac_stream ON net.stg_waste_line_fact_qtr USING btree (year_quarter, "GeneratorEPAID", "DesignatedFacilityEPAID", waste_stream_key);


--
-- TOC entry 5042 (class 1259 OID 66358)
-- Name: ix_stg_wlfq_yearq_wasteline; Type: INDEX; Schema: net; Owner: postgres
--

CREATE INDEX ix_stg_wlfq_yearq_wasteline ON net.stg_waste_line_fact_qtr USING btree (year_quarter, "WasteLineNumber");


--
-- TOC entry 4999 (class 1259 OID 57755)
-- Name: ux_graph_node_type_business_id; Type: INDEX; Schema: net; Owner: postgres
--

CREATE UNIQUE INDEX ux_graph_node_type_business_id ON net.graph_node USING btree (node_type, business_id);


--
-- TOC entry 5027 (class 1259 OID 57363)
-- Name: ux_graph_waste_stream_key; Type: INDEX; Schema: net; Owner: postgres
--

CREATE UNIQUE INDEX ux_graph_waste_stream_key ON net.graph_waste_stream USING btree (waste_stream_key);


--
-- TOC entry 4976 (class 1259 OID 41071)
-- Name: ix_em_federal_waste_code_tracking; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_federal_waste_code_tracking ON wts."EM_FEDERAL_WASTE_CODE" USING btree ("ManifestTrackingNumber");


--
-- TOC entry 4977 (class 1259 OID 41067)
-- Name: ix_em_import_tracking; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_import_tracking ON wts."EM_IMPORT" USING btree ("ManifestTrackingNumber");


--
-- TOC entry 4978 (class 1259 OID 41066)
-- Name: ix_em_manifest_facility; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_manifest_facility ON wts."EM_MANIFEST" USING btree ("DesignatedFacilityEPAID");


--
-- TOC entry 4979 (class 1259 OID 41065)
-- Name: ix_em_manifest_generator; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_manifest_generator ON wts."EM_MANIFEST" USING btree ("GeneratorEPAID");


--
-- TOC entry 4980 (class 1259 OID 57400)
-- Name: ix_em_manifest_ship_generator_facility; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_manifest_ship_generator_facility ON wts."EM_MANIFEST" USING btree ("ShippedDate", "GeneratorEPAID", "DesignatedFacilityEPAID", "ManifestTrackingNumber");


--
-- TOC entry 4981 (class 1259 OID 57869)
-- Name: ix_em_manifest_ship_track; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_manifest_ship_track ON wts."EM_MANIFEST" USING btree ("ShippedDate", "ManifestTrackingNumber");


--
-- TOC entry 4982 (class 1259 OID 41064)
-- Name: ix_em_manifest_shipped_date; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_manifest_shipped_date ON wts."EM_MANIFEST" USING btree ("ShippedDate");


--
-- TOC entry 4983 (class 1259 OID 41063)
-- Name: ix_em_manifest_tracking; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_manifest_tracking ON wts."EM_MANIFEST" USING btree ("ManifestTrackingNumber");


--
-- TOC entry 4984 (class 1259 OID 41072)
-- Name: ix_em_state_waste_code_tracking; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_state_waste_code_tracking ON wts."EM_STATE_WASTE_CODE" USING btree ("ManifestTrackingNumber");


--
-- TOC entry 4985 (class 1259 OID 66350)
-- Name: ix_em_transporter_epa; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_transporter_epa ON wts."EM_TRANSPORTER" USING btree ("TransporterEPAID");


--
-- TOC entry 4986 (class 1259 OID 41070)
-- Name: ix_em_transporter_epaid; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_transporter_epaid ON wts."EM_TRANSPORTER" USING btree ("TransporterEPAID");


--
-- TOC entry 4987 (class 1259 OID 57871)
-- Name: ix_em_transporter_track; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_transporter_track ON wts."EM_TRANSPORTER" USING btree ("ManifestTrackingNumber");


--
-- TOC entry 4988 (class 1259 OID 57399)
-- Name: ix_em_transporter_track_epa; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_transporter_track_epa ON wts."EM_TRANSPORTER" USING btree ("ManifestTrackingNumber", "TransporterEPAID");


--
-- TOC entry 4989 (class 1259 OID 41069)
-- Name: ix_em_transporter_tracking; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_transporter_tracking ON wts."EM_TRANSPORTER" USING btree ("ManifestTrackingNumber");


--
-- TOC entry 4990 (class 1259 OID 66344)
-- Name: ix_em_waste_line_number; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_waste_line_number ON wts."EM_WASTE_LINE" USING btree ("WasteLineNumber");


--
-- TOC entry 4991 (class 1259 OID 57870)
-- Name: ix_em_waste_line_track; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_waste_line_track ON wts."EM_WASTE_LINE" USING btree ("ManifestTrackingNumber");


--
-- TOC entry 4992 (class 1259 OID 41068)
-- Name: ix_em_waste_line_tracking; Type: INDEX; Schema: wts; Owner: postgres
--

CREATE INDEX ix_em_waste_line_tracking ON wts."EM_WASTE_LINE" USING btree ("ManifestTrackingNumber");


--
-- TOC entry 5086 (class 2606 OID 57861)
-- Name: graph_build_step graph_build_step_run_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_build_step
    ADD CONSTRAINT graph_build_step_run_id_fkey FOREIGN KEY (run_id) REFERENCES net.graph_build_run(run_id);


--
-- TOC entry 5080 (class 2606 OID 57777)
-- Name: graph_edge_generator_facility_stream_qtr graph_edge_generator_facility_stream_qtr_facility_node_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_facility_stream_qtr
    ADD CONSTRAINT graph_edge_generator_facility_stream_qtr_facility_node_id_fkey FOREIGN KEY (facility_node_id) REFERENCES net.graph_node(graph_node_id);


--
-- TOC entry 5081 (class 2606 OID 57772)
-- Name: graph_edge_generator_facility_stream_qtr graph_edge_generator_facility_stream_qtr_generator_node_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_facility_stream_qtr
    ADD CONSTRAINT graph_edge_generator_facility_stream_qtr_generator_node_id_fkey FOREIGN KEY (generator_node_id) REFERENCES net.graph_node(graph_node_id);


--
-- TOC entry 5082 (class 2606 OID 57782)
-- Name: graph_edge_generator_facility_stream_qtr graph_edge_generator_facility_stream_qtr_waste_stream_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_facility_stream_qtr
    ADD CONSTRAINT graph_edge_generator_facility_stream_qtr_waste_stream_id_fkey FOREIGN KEY (waste_stream_id) REFERENCES net.graph_waste_stream(waste_stream_id);


--
-- TOC entry 5071 (class 2606 OID 40998)
-- Name: graph_edge_generator_facility_year graph_edge_generator_facility_year_facility_node_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_facility_year
    ADD CONSTRAINT graph_edge_generator_facility_year_facility_node_id_fkey FOREIGN KEY (facility_node_id) REFERENCES net.graph_node(graph_node_id);


--
-- TOC entry 5072 (class 2606 OID 40993)
-- Name: graph_edge_generator_facility_year graph_edge_generator_facility_year_generator_node_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_facility_year
    ADD CONSTRAINT graph_edge_generator_facility_year_generator_node_id_fkey FOREIGN KEY (generator_node_id) REFERENCES net.graph_node(graph_node_id);


--
-- TOC entry 5083 (class 2606 OID 57808)
-- Name: graph_edge_generator_transporter_stream_qtr graph_edge_generator_transporter_strea_transporter_node_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_transporter_stream_qtr
    ADD CONSTRAINT graph_edge_generator_transporter_strea_transporter_node_id_fkey FOREIGN KEY (transporter_node_id) REFERENCES net.graph_node(graph_node_id);


--
-- TOC entry 5084 (class 2606 OID 57803)
-- Name: graph_edge_generator_transporter_stream_qtr graph_edge_generator_transporter_stream__generator_node_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_transporter_stream_qtr
    ADD CONSTRAINT graph_edge_generator_transporter_stream__generator_node_id_fkey FOREIGN KEY (generator_node_id) REFERENCES net.graph_node(graph_node_id);


--
-- TOC entry 5085 (class 2606 OID 57813)
-- Name: graph_edge_generator_transporter_stream_qtr graph_edge_generator_transporter_stream_qt_waste_stream_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_transporter_stream_qtr
    ADD CONSTRAINT graph_edge_generator_transporter_stream_qt_waste_stream_id_fkey FOREIGN KEY (waste_stream_id) REFERENCES net.graph_waste_stream(waste_stream_id);


--
-- TOC entry 5073 (class 2606 OID 41021)
-- Name: graph_edge_generator_transporter_year graph_edge_generator_transporter_year_generator_node_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_transporter_year
    ADD CONSTRAINT graph_edge_generator_transporter_year_generator_node_id_fkey FOREIGN KEY (generator_node_id) REFERENCES net.graph_node(graph_node_id);


--
-- TOC entry 5074 (class 2606 OID 41026)
-- Name: graph_edge_generator_transporter_year graph_edge_generator_transporter_year_transporter_node_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_generator_transporter_year
    ADD CONSTRAINT graph_edge_generator_transporter_year_transporter_node_id_fkey FOREIGN KEY (transporter_node_id) REFERENCES net.graph_node(graph_node_id);


--
-- TOC entry 5077 (class 2606 OID 57389)
-- Name: graph_edge_transporter_facility_stream_qtr graph_edge_transporter_facility_stream_qt_facility_node_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_transporter_facility_stream_qtr
    ADD CONSTRAINT graph_edge_transporter_facility_stream_qt_facility_node_id_fkey FOREIGN KEY (facility_node_id) REFERENCES net.graph_node(graph_node_id);


--
-- TOC entry 5078 (class 2606 OID 57394)
-- Name: graph_edge_transporter_facility_stream_qtr graph_edge_transporter_facility_stream_qtr_waste_stream_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_transporter_facility_stream_qtr
    ADD CONSTRAINT graph_edge_transporter_facility_stream_qtr_waste_stream_id_fkey FOREIGN KEY (waste_stream_id) REFERENCES net.graph_waste_stream(waste_stream_id);


--
-- TOC entry 5079 (class 2606 OID 57384)
-- Name: graph_edge_transporter_facility_stream_qtr graph_edge_transporter_facility_stream_transporter_node_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_transporter_facility_stream_qtr
    ADD CONSTRAINT graph_edge_transporter_facility_stream_transporter_node_id_fkey FOREIGN KEY (transporter_node_id) REFERENCES net.graph_node(graph_node_id);


--
-- TOC entry 5075 (class 2606 OID 41054)
-- Name: graph_edge_transporter_facility_year graph_edge_transporter_facility_year_facility_node_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_transporter_facility_year
    ADD CONSTRAINT graph_edge_transporter_facility_year_facility_node_id_fkey FOREIGN KEY (facility_node_id) REFERENCES net.graph_node(graph_node_id);


--
-- TOC entry 5076 (class 2606 OID 41049)
-- Name: graph_edge_transporter_facility_year graph_edge_transporter_facility_year_transporter_node_id_fkey; Type: FK CONSTRAINT; Schema: net; Owner: postgres
--

ALTER TABLE ONLY net.graph_edge_transporter_facility_year
    ADD CONSTRAINT graph_edge_transporter_facility_year_transporter_node_id_fkey FOREIGN KEY (transporter_node_id) REFERENCES net.graph_node(graph_node_id);


-- Completed on 2026-04-03 22:19:19

--
-- PostgreSQL database dump complete
--

\unrestrict ajQkectL9gMskYmbI0F2cy5JMto7dKKixGrFNInbzaOpxUtndwywyYCr03umRVl

