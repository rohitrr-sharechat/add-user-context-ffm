with
posts as (
  select distinct CONCAT('1_post_', postId) feature_name
  from {q1table}
),

users as (
  select distinct CONCAT('2_user_', userId) feature_name
  from {q1table}
),

-- tags as (
--   select distinct CONCAT('3_tag_', tagId) feature_name
--   from {q1table}
-- ),

prices as (
  select distinct CONCAT('3_priceBucket_', priceBucket) feature_name
  from {q1table}
),

all_features as (
  (select * from users)
  union all
  (select * from posts)
  union all
  (select * from prices)
)

select feature_name, ROW_NUMBER() OVER (ORDER BY feature_name) as mapping
from all_features
order by mapping