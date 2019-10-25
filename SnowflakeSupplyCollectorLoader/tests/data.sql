--- Test data
use testdb;

create schema if not exists test;

drop table if exists test.test_data_types;

create table test.test_data_types (
   id INTEGER autoincrement(1,1) not null primary key,
   char_field char(40),
   varchar_field varchar(100),
   string_field string,
   boolean_field boolean,
   number_field number,
   decimal_field decimal,
   double_field double,
   date_field date,
   timestamp_field timestamp
);

insert into test.test_data_types(char_field, varchar_field, string_field, boolean_field, number_field, decimal_field, double_field, date_field, timestamp_field)
values('char!', 'varchar!', 'string!', true, 6.02214076, 3.141592653, 2.54, to_date('2019-08-13'), current_timestamp);

drop table if exists test.test_array_types;

create table test.test_array_types (
   id integer autoincrement(1,1) not null primary key,
   variant_field variant,
   object_field object,
   array_field array
);

insert into test_array_types (variant_field, object_field, array_field)
values('test', object_construct('field1', 1, 'field2', 'value'), array_construct(1,2,3));

drop table if exists test.test_field_names;

create table test.test_field_names (
   id integer autoincrement(1,1) not null primary key,
   low_case integer,
   UPCASE integer,
   CamelCase integer,
   "Table" integer,
   "array" integer,
   "SELECT" integer,
   constraint test_field_names_pk primary key(id)
);

insert into test.test_field_names(low_case, upcase, camelcase, "Table", "array", "SELECT")
values(0,0,0,0,0,0);

drop table if exists test.test_index;

create table test.test_index (
   id integer not null primary key,
   name varchar(100) NOT NULL unique
);

insert into test.test_index(id, name)
values(1, 'Sunday');
insert into test.test_index(id, name)
values(2, 'Monday');
insert into test.test_index(id, name)
values(3, 'Tuesday');
insert into test.test_index(id, name)
values(4, 'Wednesday');
insert into test.test_index(id, name)
values(5, 'Thursday');
insert into test.test_index(id, name)
values(6, 'Friday');
insert into test.test_index(id, name)
values(7, 'Saturday');

drop table if exists test.test_index_ref;

create table test.test_index_ref (
   id integer autoincrement(1,1) not null primary key,
   index_id integer REFERENCES test_index(id)
);

insert into test.test_index_ref(index_id)
values(1);
insert into test.test_index_ref(index_id)
values(5);

COMMIT;

