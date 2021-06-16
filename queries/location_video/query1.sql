WITH
full_data as (
  select userId, postId, tagId, locationBucket, if({rating_def}, 1, 0) as score
  from `{q0table}`
),

posts as (
  SELECT postId
  FROM full_data
  group by 1
  having count(*) >= 5000 and sum(score) >= 100
),

users as (
  SELECT userId
  FROM full_data
  where postId in (select * from posts)
  group by 1
  having count(*) >= 10 and sum(score) >= 2
),

locations as (
  SELECT locationBucket
  FROM full_data
  where postId in (select * from posts) and userId in (select * from users)
  group by 1
  having count(*) >= 10 and sum(score) >= 2
)


select *
from full_data
where postId in (select * from posts) and userId in (select * from users)
and locationBucket in (select * from locations)