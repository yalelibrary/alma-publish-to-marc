
from pathlib import Path

# create ddl with
#    run spring boot app to create the database with `ddl-auto: create`
#    docker exec -it d4 bash (where d4 is the alma database container id)
#    pg_dump -s data_sync_db -U alma_sync_user -h localhost
#    then run this script from the ddl directory to clean up the ddl

def load_ddl():
    p = Path(__file__).with_name('data_sync_db.sql')
    with p.open('r') as f:
        return f.readlines()

def write_ddl(ddl):
    p = Path(__file__).with_name('data_sync_db.sql')
    with p.open('w') as f:
        f.writelines(ddl)


def include_line(line):
    line = line.strip()
    if not line:
        return False
    if line.startswith('--'):
        return False
    if line.startswith('ALTER TABLE') and ' OWNER TO ' in line:
        return False
    return True

def add_cascade(line):
    if 'ADD CONSTRAINT item_holding_id_fk' in line or 'ADD CONSTRAINT holding_brief_bib_brief_fk' in line:
        if not 'ON DELETE CASCADE;' in line:
            line = line.replace(';', ' ON DELETE CASCADE;')
    return line


def main():
    ddl = load_ddl()
    ddl = [add_cascade(line) for line in ddl if include_line(line)]
    table_drops = [f'DROP TABLE IF EXISTS {line.strip()[len('CREATE TABLE '):-1]} CASCADE;\n' for line in ddl if line.startswith('CREATE TABLE')]
    seq_drops = [f'DROP SEQUENCE IF EXISTS {line.strip()[len('CREATE SEQUENCE '):-1]} CASCADE;\n' for line in ddl if line.startswith('CREATE SEQUENCE')]
    ddl = table_drops + seq_drops + ddl
    ddl += '''
ALTER TABLE ONLY public.item_data
    ADD CONSTRAINT item_data_id_fk FOREIGN KEY (pid) REFERENCES public.item(pid) ON DELETE CASCADE;
ALTER TABLE ONLY public.holding_marc
    ADD CONSTRAINT holding_marc_id_fk FOREIGN KEY (holding_id) REFERENCES public.holding_brief(holding_id) ON DELETE CASCADE;
ALTER TABLE ONLY public.bib_marc
    ADD CONSTRAINT bib_marc_id_fk FOREIGN KEY (mms_id) REFERENCES public.bib_brief(mms_id) ON DELETE CASCADE;

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
           '''
    write_ddl(ddl)


if __name__ == '__main__':
    main()