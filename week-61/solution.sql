-- 事前準備
use role SYSADMIN;
use warehouse COMPUTE_WH;
create or replace database FROSTY_FRIDAY_DB;
create or replace schema FROSTY_FRIDAY_DB.WEEK_61_SCHEMA;
create or replace stage FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.EXSTG_TELECOM_PRODUCTS
  url = 's3://frostyfridaychallenges/challenge_61/';

list @FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.EXSTG_TELECOM_PRODUCTS;

-- テーブル生成
create or replace table FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.TBL_TELECOM_PRODUCTS as
with raw as (
  -- ステージから取込
  select
    metadata$file_row_number - 1 as RN -- ヘッダーを除いた行番号
    ,$1::VARCHAR as BRAND
    ,$2::VARCHAR as URL
    ,$3::VARCHAR as PRODUCT_NAME
    ,$4::VARCHAR as CATEGORY
    ,$5::VARCHAR as FRIENDLY_URL
  from @FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.EXSTG_TELECOM_PRODUCTS
  where metadata$file_row_number > 1
    and metadata$filename = 'challenge_61/Telecom Products - Sheet1.csv'
)
-- クリーンアップ
select
  -- BRAND の前方フィル
  last_value(BRAND) ignore nulls
    over (order by RN
          rows between unbounded preceding and current row)
    as BRAND
  ,PRODUCT_NAME
  ,CATEGORY
  -- FRIENDLY_URL が NULL の場合は URL を使う
  ,coalesce(FRIENDLY_URL, URL) as FRIENDLY_URL
from raw
-- CATEGROY が　NULLの場合は削除
where CATEGORY is not null;

select * from FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.TBL_TELECOM_PRODUCTS;

-- JSONファイル生成
create or replace stage FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.INSTG_TELECOM_PRODUCTS
  encryption = (type = 'SNOWFLAKE_SSE'); --これを入れないと署名付きURLからダウンロードした場合、ファイルが破損
create or replace file format FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.FFMT_JSON_TELECOM_PRODUCTS
  type = 'JSON'
  compression = 'NONE'; --宣言しないと圧縮されてしまう

copy into @FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.INSTG_TELECOM_PRODUCTS/telecom_products.json
from (
with pa as (
  select
    CATEGORY
    ,BRAND
    -- object_construct(A, B) --> {"A":"B"}　単一のキーと値のペアのオブジェクトを作る
    -- array_agg(A) --> [a1, a2, a3...]
    ,array_agg(object_construct(PRODUCT_NAME, FRIENDLY_URL)) as PRODUCT_ARRAY
  from TBL_TELECOM_PRODUCTS
  group by 1, 2
),
bg as (
  select
    CATEGORY
    -- object_agg(A, B) --> {"a1":"b1", "a2":"b2", "a3":"b3"...} 複数のキーと値のペアをまとめる
    ,object_agg(BRAND, PRODUCT_ARRAY) as BRAND_GROUP
  from pa
  group by 1
)
select
  object_agg(CATEGORY, BRAND_GROUP) as json
from bg
)
file_format = FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.FFMT_JSON_TELECOM_PRODUCTS
overwrite = true
single = true;

select
  json.$1
from
  @FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.INSTG_TELECOM_PRODUCTS/telecom_products.json
  (file_format => FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.FFMT_JSON_TELECOM_PRODUCTS) as json;

-- 署名付きURL生成
-- デフォルトの期限は3600秒
select get_presigned_url(@FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.INSTG_TELECOM_PRODUCTS, 'telecom_products.json') as PRESIGNED_URL;


-- 補足(Categoryの入力が統一されていなかった場合)
create or replace function FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.FUNC_CLEANSE_CATEGORY(input string)
returns string
as
$$
  initcap(trim(input))
$$;

select
  '  mobile DEVices  ' as raw_value,
  FROSTY_FRIDAY_DB.WEEK_61_SCHEMA.FUNC_CLEANSE_CATEGORY('  mobile DEVices  ') as cleaned_value;
