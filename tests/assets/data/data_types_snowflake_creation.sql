CREATE SCHEMA IF NOT EXISTS "SNOWSHU_DEVELOPMENT"."TESTS_DATA";

CREATE TABLE IF NOT EXISTS "SNOWSHU_DEVELOPMENT"."TESTS_DATA"."DATA_TYPES" (
array_col ARRAY,
bigint_col BIGINT,
binary_col BINARY,
boolean_col BOOLEAN,
char_col CHAR,
character_col CHARACTER,
date_col DATE,
datetime_col DATETIME,
decimal_col DECIMAL,
double_col DOUBLE,
doubleprecision_col DOUBLE PRECISION,
float_col FLOAT,
float4_col FLOAT4,
float8_col FLOAT8,
int_col INT,
integer_col INTEGER,
number_col NUMBER,
numeric_col NUMERIC,
object_col OBJECT,
real_col REAL,
smallint_col SMALLINT,
string_col STRING,
text_col TEXT,
time_col TIME,
timestamp_col TIMESTAMP,
timestamp_ntz_col TIMESTAMP_NTZ,
timestamp_ltz_col TIMESTAMP_LTZ,
timestamp_tz_col TIMESTAMP_TZ,
varbinary_col VARBINARY,
varchar_col VARCHAR,
variant_col VARIANT);



INSERT INTO "SNOWSHU_DEVELOPMENT"."TESTS_DATA"."DATA_TYPES" (
array_col,
bigint_col,
binary_col,
boolean_col,
char_col,
character_col,
date_col,
datetime_col,
decimal_col,
double_col,
doubleprecision_col,
float_col,
float4_col,
float8_col,
int_col,
integer_col,
number_col,
numeric_col,
object_col,
real_col,
smallint_col,
string_col,
text_col,
time_col,
timestamp_col,
timestamp_ntz_col,
timestamp_ltz_col,
timestamp_tz_col,
varbinary_col,
varchar_col,
variant_col)

SELECT
array_construct(1, 2, 3), 
1000000000000000::BIGINT,
to_binary(hex_encode('Hello World'),'HEX'),
TRUE,
1::CHAR,
1::CHARACTER,
CURRENT_DATE(),
CURRENT_DATE(),
1.45E2,
-1.45,
+1.45,
1.45,
1.45,
1.45,
1,
1,
1,
1.45,
    parse_json(' { "outer_key1": { "inner_key1A": "1a", "inner_key1B": "1b" }, '
              ||
               '   "outer_key2": { "inner_key2": 2 } } '),
1,
1,
'Hello World',
'Hello World',
CURRENT_TIME(),
CURRENT_DATE(),
CURRENT_DATE(),
CURRENT_DATE(),
CURRENT_DATE(),
to_binary(hex_encode('Hello World'),'HEX'),
'Hello World',
    parse_json(' { "key1": "value1", "key2": NULL } ');



CREATE TABLE IF NOT EXISTS "SNOWSHU_DEVELOPMENT"."TESTS_DATA"."CASE_TESTING" (
    UPPER_COL VARCHAR,
    "QUOTED_UPPER_COL" VARCHAR,
    lower_col VARCHAR,
    CamelCasedCol VARCHAR,
    Snake_Case_Camel_Col VARCHAR,
    "Spaces Col" VARCHAR,
    "UNIFORM SPACE" VARCHAR,
    "uniform lower" VARCHAR,
    "1" VARCHAR
);


INSERT INTO "SNOWSHU_DEVELOPMENT"."TESTS_DATA"."CASE_TESTING" (
    UPPER_COL,
    "QUOTED_UPPER_COL",
    lower_col,
    CamelCasedCol,
    Snake_Case_Camel_Col,
    "Spaces Col",
    "UNIFORM SPACE",
    "uniform lower",
    "1"
)
VALUES('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i');