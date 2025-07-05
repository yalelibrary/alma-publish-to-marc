
DROP VIEW IF EXISTS public.item_info;
DROP TABLE IF EXISTS public.bib_brief  CASCADE;
DROP TABLE IF EXISTS public.bib_marc  CASCADE;
DROP TABLE IF EXISTS public.bib_marc_xml  CASCADE;
DROP TABLE IF EXISTS public.bib_part  CASCADE;
DROP TABLE IF EXISTS public.circ_desk  CASCADE;
DROP TABLE IF EXISTS public.code_table_value  CASCADE;
DROP TABLE IF EXISTS public.deleted_record  CASCADE;
DROP TABLE IF EXISTS public.holding_brief  CASCADE;
DROP TABLE IF EXISTS public.holding_marc  CASCADE;
DROP TABLE IF EXISTS public.item  CASCADE;
DROP TABLE IF EXISTS public.item_base_status  CASCADE;
DROP TABLE IF EXISTS public.item_data  CASCADE;
DROP TABLE IF EXISTS public.library  CASCADE;
DROP TABLE IF EXISTS public.location  CASCADE;
DROP TABLE IF EXISTS public.location_circ_desk  CASCADE;
DROP TABLE IF EXISTS public.record_set CASCADE;
DROP TABLE IF EXISTS public.user_details  CASCADE;
DROP TABLE IF EXISTS public.external_id  CASCADE;
DROP TABLE IF EXISTS public.request_event CASCADE;

# FULL
DROP TABLE IF EXISTS public.record_update  CASCADE;
DROP SEQUENCE IF EXISTS public.circ_desk_id_se CASCADE;
DROP SEQUENCE IF EXISTS public.code_table_value_id_se CASCADE;
DROP SEQUENCE IF EXISTS public.deleted_record_id_se CASCADE;
DROP SEQUENCE IF EXISTS public.location_id_se CASCADE;
DROP SEQUENCE IF EXISTS public.record_set_id_se CASCADE;
DROP SEQUENCE IF EXISTS public.user_details_id_seq CASCADE;
DROP SEQUENCE IF EXISTS public.external_id_seq CASCADE;
# FULL
DROP SEQUENCE IF EXISTS public.record_update_id_seq CASCADE;
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;
SET default_tablespace = '';
SET default_table_access_method = heap;
CREATE TABLE public.bib_brief (
    create_date_time timestamp(6) without time zone,
    system_create_date_time timestamp(6) without time zone,
    system_update_date_time timestamp(6) without time zone,
    update_date_time timestamp(6) without time zone,
    version bigint,
    author text,
    extent text,
    field008 character varying(255),
    isbn text,
    issn text,
    leader character varying(255),
    material text,
    mms_id character varying(255) NOT NULL,
    oclc_number text,
    publication_date character varying(255),
    publication_place text,
    publisher text,
    suppress character varying(255),
    title text,
    voyager_bib_id character varying(255)
);
CREATE TABLE public.bib_marc (
    create_date_time timestamp(6) without time zone,
    update_date_time timestamp(6) without time zone,
    version bigint,
    raw_marc bytea,
    mms_id character varying(255) NOT NULL
);
CREATE TABLE public.bib_marc_xml (
    create_date_time timestamp(6) without time zone,
    update_date_time timestamp(6) without time zone,
    version bigint,
    marc_xml text,
    mms_id character varying(255) NOT NULL
);
CREATE TABLE public.bib_part (
    mms_id character varying(255) NOT NULL,
    part_mms_id character varying(255) NOT NULL
);
CREATE TABLE public.circ_desk (
    primary_desk boolean NOT NULL,
    reading_room_desk boolean NOT NULL,
    create_date_time timestamp(6) without time zone,
    id bigint NOT NULL,
    update_date_time timestamp(6) without time zone,
    version bigint,
    code character varying(255),
    library_code character varying(255),
    name character varying(255)
);
CREATE SEQUENCE public.circ_desk_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.circ_desk_id_seq OWNED BY public.circ_desk.id;
CREATE TABLE public.code_table_value (
    create_date_time timestamp(6) without time zone,
    id bigint NOT NULL,
    update_date_time timestamp(6) without time zone,
    version bigint,
    code character varying(255),
    code_table character varying(255),
    description character varying(255)
);
CREATE SEQUENCE public.code_table_value_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.code_table_value_id_seq OWNED BY public.code_table_value.id;
CREATE TABLE public.deleted_record (
    create_date_time timestamp(6) without time zone,
    id bigint NOT NULL,
    update_date_time timestamp(6) without time zone,
    version bigint,
    mms_id character varying(255),
    record_id character varying(255),
    record_type character varying(255)
);
CREATE SEQUENCE public.deleted_record_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.deleted_record_id_seq OWNED BY public.deleted_record.id;
CREATE TABLE public.holding_brief (
    create_date_time timestamp(6) without time zone,
    location_id bigint,
    system_create_date_time timestamp(6) without time zone,
    system_update_date_time timestamp(6) without time zone,
    update_date_time timestamp(6) without time zone,
    version bigint,
    call_number_type character varying(255),
    display_call_number character varying(255),
    encoding_level character varying(255),
    field007 character varying(255),
    field008 character varying(255),
    holding_id character varying(255) NOT NULL,
    mms_id character varying(255),
    record_status character varying(255),
    record_type character varying(255),
    suppress character varying(255),
    voyager_holding_id character varying(255)
);
CREATE TABLE public.holding_marc (
    create_date_time timestamp(6) without time zone,
    update_date_time timestamp(6) without time zone,
    version bigint,
    raw_marc bytea,
    holding_id character varying(255) NOT NULL
);
CREATE TABLE public.item (
    sequence_number integer NOT NULL,
    create_date_time timestamp(6) without time zone,
    inventory_date_time timestamp(6) without time zone,
    perm_location_id bigint,
    system_create_date_time timestamp(6) without time zone,
    system_update_date_time timestamp(6) without time zone,
    temp_location_id bigint,
    update_date_time timestamp(6) without time zone,
    version bigint,
    barcode character varying(255),
    chron character varying(255),
    holding_id character varying(255),
    item_enum character varying(255),
    material_type character varying(255),
    pid character varying(255) NOT NULL,
    pieces character varying(255),
    voyager_item_id character varying(255),
    copy_id character varying(255),
    policy character varying(255),
    description character varying(255)
);
CREATE TABLE public.item_base_status (
    create_date_time timestamp(6) without time zone,
    due_date timestamp(6) without time zone,
    loan_date timestamp(6) without time zone,
    renewal_date timestamp(6) without time zone,
    update_date_time timestamp(6) without time zone,
    version bigint,
    pid character varying(255) NOT NULL,
    process_status character varying(255),
    process_type character varying(255),
    status_code character varying(255)
);
CREATE TABLE public.item_data (
    create_date_time timestamp(6) without time zone,
    update_date_time timestamp(6) without time zone,
    version bigint,
    data text,
    pid character varying(255) NOT NULL
);
CREATE TABLE public.library (
    create_date_time timestamp(6) without time zone,
    update_date_time timestamp(6) without time zone,
    version bigint,
    alma_id character varying(255),
    campus character varying(255),
    campus_description character varying(255),
    code character varying(255) NOT NULL,
    description character varying(255),
    name character varying(255),
    path character varying(255)
);
CREATE TABLE public.location (
    create_date_time timestamp(6) without time zone,
    id bigint NOT NULL,
    update_date_time timestamp(6) without time zone,
    version bigint,
    code character varying(255),
    external_name character varying(255),
    library_code character varying(255),
    name character varying(255),
    suppress character varying(255)
);
CREATE TABLE public.location_circ_desk (
    circ_desk_id bigint NOT NULL,
    location_id bigint NOT NULL
);
CREATE TABLE public.record_set (
    id bigint NOT NULL,
    alma_id character varying(255),
    set_json text,
    name character varying(255)
);
CREATE SEQUENCE public.record_set_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.record_set_id_seq OWNED BY public.record_set.id;
ALTER TABLE ONLY public.record_set
    ADD CONSTRAINT record_set_pkey PRIMARY KEY (id);
CREATE INDEX record_set_alma_id_ix ON public.record_set USING btree (alma_id);
# FULL
CREATE TABLE public.record_update (
    id bigint NOT NULL,
    create_date_time timestamp(6) without time zone,
    notification_date_time timestamp(6) without time zone,
    record_id character varying(255),
    record_type character varying(255),
    modified_by character varying(255)
);
# FULL
CREATE SEQUENCE public.record_update_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
# FULL
ALTER SEQUENCE public.record_update_id_seq OWNED BY public.record_update.id;
# FULL
ALTER TABLE ONLY public.record_update ALTER COLUMN id SET DEFAULT nextval('public.record_update_id_seq'::regclass);
# FULL
ALTER TABLE ONLY public.record_update
    ADD CONSTRAINT record_update_pkey PRIMARY KEY (id);
# FULL
CREATE INDEX record_update_record_id_ix ON public.record_update USING btree (record_id);
# FULL
CREATE INDEX notification_date_time_record_id_ix ON public.record_update USING btree (notification_date_time);
CREATE TABLE public.external_id (
    id bigint NOT NULL,
    create_date_time timestamp(6) without time zone,
    record_id character varying(255),
    record_type character varying(255),
    external_id_type character varying(255),
    external_value character varying(255)
);
CREATE SEQUENCE public.external_id_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.external_id_id_seq OWNED BY public.external_id.id;
ALTER TABLE ONLY public.external_id ALTER COLUMN id SET DEFAULT nextval('public.external_id_id_seq'::regclass);
ALTER TABLE ONLY public.external_id
    ADD CONSTRAINT external_id_pkey PRIMARY KEY (id);
CREATE INDEX external_id_record_id_ix ON public.external_id USING btree (record_id);
CREATE INDEX external_id_external_id_type_ix ON public.external_id USING btree (external_id_type, external_value);
CREATE INDEX external_id_external_value_ix ON public.external_id USING btree (external_value);
CREATE TABLE public.request_event (
    request_id character varying(255),
    create_date_time timestamp(6) without time zone,
    notification_date_time timestamp(6) without time zone,
    mms_id character varying(255),
    holding_id character varying(255),
    pid character varying(255),
    barcode character varying(255),
    volume character varying(255),
    part character varying(255),
    material_type character varying(255),
    issue character varying(255),
    request_event character varying(255),
    request_type character varying(255),
    request_sub_type character varying(255),
    request_status character varying(255),
    comment text,
    managed_by_library_code character varying(255),
    managed_by_circulation_desk_code character varying(255),
    place_in_queue integer,
    task_name character varying(255),
    pickup_location character varying(255),
    pickup_location_type character varying(255),
    pickup_location_library character varying(255),
    request_date timestamp(6) without time zone,
    expiry_date timestamp(6) without time zone,
    last_interest_date timestamp(6) without time zone
);

ALTER TABLE ONLY public.request_event ADD CONSTRAINT request_event_pkey PRIMARY KEY (request_id);
CREATE INDEX request_event_mmsid_id_ix ON public.request_event USING btree (mms_id);
CREATE INDEX request_event_request_id_ix ON public.request_event USING btree (request_id);
CREATE INDEX request_event_request_status_ix ON public.request_event USING btree (request_status);
CREATE INDEX request_event_barcode_ix ON public.request_event USING btree (barcode);
CREATE INDEX request_event_pid_ix ON public.request_event USING btree (pid);

create sequence public.user_details_id_seq;
create table public.user_details (
                                     account_non_expired boolean not null,
                                     account_non_locked boolean not null,
                                     credentials_non_expired boolean not null,
                                     enabled boolean not null,
                                     create_date_time timestamp(6) without time zone,
                                     id bigint primary key not null default nextval('public.user_details_id_seq'::regclass),
                                     update_date_time timestamp(6) without time zone,
                                     version bigint,
                                     password character varying(255),
                                     role character varying(255),
                                     username character varying(255)
);
create unique index user_username_ix on public.user_details using btree (username);
alter sequence public.user_details_id_seq owned by public.user_details.id;
CREATE SEQUENCE public.location_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.location_id_seq OWNED BY public.location.id;
ALTER TABLE ONLY public.circ_desk ALTER COLUMN id SET DEFAULT nextval('public.circ_desk_id_seq'::regclass);
ALTER TABLE ONLY public.code_table_value ALTER COLUMN id SET DEFAULT nextval('public.code_table_value_id_seq'::regclass);
ALTER TABLE ONLY public.deleted_record ALTER COLUMN id SET DEFAULT nextval('public.deleted_record_id_seq'::regclass);
ALTER TABLE ONLY public.location ALTER COLUMN id SET DEFAULT nextval('public.location_id_seq'::regclass);
ALTER TABLE ONLY public.bib_brief
    ADD CONSTRAINT bib_brief_pkey PRIMARY KEY (mms_id);
ALTER TABLE ONLY public.bib_marc
    ADD CONSTRAINT bib_marc_pkey PRIMARY KEY (mms_id);
ALTER TABLE ONLY public.bib_marc_xml
    ADD CONSTRAINT bib_marc_xml_pkey PRIMARY KEY (mms_id);
ALTER TABLE ONLY public.bib_part
    ADD CONSTRAINT bib_part_pkey PRIMARY KEY (mms_id, part_mms_id);
ALTER TABLE ONLY public.circ_desk
    ADD CONSTRAINT circ_desk_codes_ix UNIQUE (code, library_code);
ALTER TABLE ONLY public.location_circ_desk
    ADD CONSTRAINT circ_desk_location_unique_ix UNIQUE (circ_desk_id, location_id);
ALTER TABLE ONLY public.circ_desk
    ADD CONSTRAINT circ_desk_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.code_table_value
    ADD CONSTRAINT code_table_code_ix UNIQUE (code, code_table);
ALTER TABLE ONLY public.code_table_value
    ADD CONSTRAINT code_table_value_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.deleted_record
    ADD CONSTRAINT deleted_record_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.holding_brief
    ADD CONSTRAINT holding_brief_pkey PRIMARY KEY (holding_id);
ALTER TABLE ONLY public.holding_marc
    ADD CONSTRAINT holding_marc_pkey PRIMARY KEY (holding_id);
ALTER TABLE ONLY public.item_base_status
    ADD CONSTRAINT item_base_status_pkey PRIMARY KEY (pid);
ALTER TABLE ONLY public.item_data
    ADD CONSTRAINT item_data_pkey PRIMARY KEY (pid);
ALTER TABLE ONLY public.item
    ADD CONSTRAINT item_pkey PRIMARY KEY (pid);
ALTER TABLE ONLY public.library
    ADD CONSTRAINT library_pkey PRIMARY KEY (code);
ALTER TABLE ONLY public.location
    ADD CONSTRAINT location_code_library_code_ix UNIQUE (code, library_code);
ALTER TABLE ONLY public.location
    ADD CONSTRAINT location_pkey PRIMARY KEY (id);
CREATE INDEX bib_brief_isbn_ix ON public.bib_brief USING btree (isbn);
CREATE INDEX bib_brief_issn_ix ON public.bib_brief USING btree (issn);
CREATE INDEX bib_brief_suppress_ix ON public.bib_brief USING btree (suppress);
CREATE INDEX bib_brief_update_date_time_ix ON public.bib_brief USING btree (update_date_time);
CREATE INDEX bib_brief_voyager_id_ix ON public.bib_brief USING btree (voyager_bib_id);
CREATE INDEX bib_part_mms_id_ix ON public.bib_part USING btree (mms_id);
CREATE INDEX bib_part_part_mms_id_ix ON public.bib_part USING btree (part_mms_id);
CREATE INDEX circ_desk_id_ix ON public.location_circ_desk USING btree (circ_desk_id);
CREATE INDEX circ_desk_location_ix ON public.location_circ_desk USING btree (location_id);
CREATE INDEX circ_desk_name_ix ON public.circ_desk USING btree (name);
CREATE INDEX deleted_record_mms_id_ix ON public.deleted_record USING btree (mms_id);
CREATE INDEX deleted_record_record_type_id_ix ON public.deleted_record USING btree (record_type, record_id);
CREATE INDEX holding_brief_mms_id_ix ON public.holding_brief USING btree (mms_id);
CREATE INDEX holding_brief_suppress_ix ON public.holding_brief USING btree (suppress);
CREATE INDEX holding_brief_update_date_time_ix ON public.holding_brief USING btree (update_date_time);
CREATE INDEX holding_brief_voyager_holding_id_ix ON public.holding_brief USING btree (voyager_holding_id);
CREATE INDEX item_barcode_ix ON public.item USING btree (barcode);
CREATE INDEX item_holding_id_ix ON public.item USING btree (holding_id);
CREATE INDEX item_status_update_date_time_ix ON public.item_base_status USING btree (update_date_time);
CREATE INDEX item_update_date_time_ix ON public.item USING btree (update_date_time);
CREATE INDEX item_voyager_item_id_ix ON public.item USING btree (voyager_item_id);
CREATE INDEX library_name_ix ON public.library USING btree (name);
CREATE INDEX location_name_ix ON public.location USING btree (name);
ALTER TABLE ONLY public.circ_desk
    ADD CONSTRAINT circ_desk_library_fk FOREIGN KEY (library_code) REFERENCES public.library(code);
ALTER TABLE ONLY public.location_circ_desk
    ADD CONSTRAINT circ_desk_location_circ_desk_fk FOREIGN KEY (circ_desk_id) REFERENCES public.circ_desk(id);
ALTER TABLE ONLY public.location_circ_desk
    ADD CONSTRAINT circ_desk_location_location_fk FOREIGN KEY (location_id) REFERENCES public.location(id);
ALTER TABLE ONLY public.holding_brief
    ADD CONSTRAINT holding_brief_bib_brief_fk FOREIGN KEY (mms_id) REFERENCES public.bib_brief(mms_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.holding_brief
    ADD CONSTRAINT holding_brief_location_fk FOREIGN KEY (location_id) REFERENCES public.location(id);
ALTER TABLE ONLY public.item
    ADD CONSTRAINT item_holding_id_fk FOREIGN KEY (holding_id) REFERENCES public.holding_brief(holding_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.item
    ADD CONSTRAINT item_perm_location_fk FOREIGN KEY (perm_location_id) REFERENCES public.location(id);
ALTER TABLE ONLY public.item
    ADD CONSTRAINT item_temp_location_fk FOREIGN KEY (temp_location_id) REFERENCES public.location(id);
ALTER TABLE ONLY public.location
    ADD CONSTRAINT location_library_fk FOREIGN KEY (library_code) REFERENCES public.library(code);

ALTER TABLE ONLY public.item_data
    ADD CONSTRAINT item_data_id_fk FOREIGN KEY (pid) REFERENCES public.item(pid) ON DELETE CASCADE;
ALTER TABLE ONLY public.holding_marc
    ADD CONSTRAINT holding_marc_id_fk FOREIGN KEY (holding_id) REFERENCES public.holding_brief(holding_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.bib_marc
    ADD CONSTRAINT bib_marc_id_fk FOREIGN KEY (mms_id) REFERENCES public.bib_brief(mms_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.bib_marc_xml
    ADD CONSTRAINT bib_marc_xml_id_fk FOREIGN KEY (mms_id) REFERENCES public.bib_brief(mms_id) ON DELETE CASCADE;


CREATE or REPLACE VIEW public.item_info AS
SELECT
 it.create_date_time,
 it.update_date_time,
 it.system_create_date_time,
 it.system_update_date_time,
 it.version,
 perm_location_id,
 pl.library_code perm_library_code,
 pl.code perm_location_code,
 temp_location_id,
 tl.library_code temp_library_code,
 tl.code temp_location_code,
 hb.mms_id,
 it.holding_id,
 it.pid,
 display_call_number,
 title,
 author,
 it.barcode,
 it.description,
 item_enum,
 chron,
 voyager_item_id,
 inventory_date_time,
 it.material_type,
 pieces,
 copy_id,
 policy,
 sequence_number,
  bs.update_date_time as status_update_date_time,
  status_code,
  status_ct.description as status,
  process_type,
  process_status,
  renewal_date,
  loan_date,
  due_date,
  data,
  rq.request_id as request_id,
  rq.notification_date_time as request_update_date_time,
  rq.request_status,
  rq.request_type,
  rq.request_sub_type,
  rq.request_date,
  rq.request_event,
  rq.pickup_location_library as request_pickup_location,
  (select count(*) from public.request_event rqc where rqc.pid = it.pid and rqc.request_status != 'HISTORY') as request_count,
  (select count(*) from public.request_event rqc where rqc.holding_id = it.holding_id and rqc.request_status != 'HISTORY' and rqc.pid is NULL) as holding_level_request_count,
  (select count(*) from public.request_event rqc where rqc.holding_id is NULL and rqc.mms_id = hb.mms_id and rqc.request_status != 'HISTORY' and rqc.pid is NULL) as bib_level_request_count
FROM
  public.item it
  JOIN public.item_base_status bs USING (pid)
  JOIN public.item_data USING (pid)
  JOIN public.location pl ON (perm_location_id = pl.id)
  JOIN public.holding_brief hb USING(holding_id)
  JOIN public.bib_brief bb USING(mms_id)
  JOIN public.code_table_value status_ct ON (status_ct.code_table = 'BaseStatus' and status_ct.code = bs.status_code)
  LEFT OUTER JOIN public.location tl ON (temp_location_id = tl.id)
  LEFT OUTER JOIN public.request_event rq ON (rq.pid = it.pid AND rq.notification_date_time =
        (SELECT max(notification_date_time)
        FROM public.request_event r1
        WHERE rq.pid = r1.pid) AND rq.request_status != 'HISTORY');



CREATE OR REPLACE FUNCTION public.bib_brief_delete() RETURNS trigger AS $bib_brief_delete$
BEGIN
INSERT INTO public.deleted_record (
    create_date_time, update_date_time, version, record_type, mms_id, record_id
) values (
             now(), now(), 1, 'bib', OLD.mms_id, OLD.mms_id
         ) |
RETURN OLD |
END |
$bib_brief_delete$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER bib_brief_delete BEFORE DELETE ON public.bib_brief
    FOR EACH ROW EXECUTE FUNCTION public.bib_brief_delete();


CREATE OR REPLACE FUNCTION public.holding_brief_delete() RETURNS trigger AS $holding_brief_delete$
BEGIN
    INSERT INTO public.deleted_record (
        create_date_time, update_date_time, version, record_type, mms_id, record_id
    ) values (
                now(), now(), 1, 'holding', OLD.mms_id, OLD.holding_id
             ) |

    INSERT INTO public.deleted_record (
        create_date_time, update_date_time, version, record_type, mms_id, record_id
    )  select
                 now(), now(), 1, 'item', OLD.mms_id, item.pid from item where item.holding_id = OLD.holding_id
     |
    RETURN OLD |
END |
$holding_brief_delete$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER holding_brief_delete BEFORE DELETE ON public.holding_brief
    FOR EACH ROW EXECUTE FUNCTION public.holding_brief_delete();

CREATE OR REPLACE FUNCTION public.item_delete() RETURNS trigger AS $item_delete$
BEGIN
    IF (EXISTS (SELECT mms_id FROM holding_brief WHERE public.holding_brief.holding_id = OLD.holding_id)) THEN
    INSERT INTO public.deleted_record (
        create_date_time, update_date_time, version, record_type, mms_id, record_id
    ) values (
                 now(), now(), 2, 'item', (SELECT mms_id FROM public.holding_brief WHERE public.holding_brief.holding_id = OLD.holding_id), OLD.pid
             )|
    END IF|
    RETURN OLD|
END |
$item_delete$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER item_delete BEFORE DELETE ON public.item
    FOR EACH ROW EXECUTE FUNCTION public.item_delete();




