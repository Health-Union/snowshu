/* Emulates as GREATEST (ie MAX value) */


create or replace function any_value_mock (anynonarray, anynonarray) returns anynonarray

    language sql as $$

select greatest($1,$2);

$$;

CREATE or replace AGGREGATE any_value (anyelement)

(

    sfunc = any_value_mock,

    stype = anyelement

);
