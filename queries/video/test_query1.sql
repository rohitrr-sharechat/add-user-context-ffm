WITH
full_data as (
  select userId, postId, tagId, referrer, if({rating_def}, 1, 0) as score
  from {q0table}
)

select *
from full_data
where postId in (select postId from {train_q1table})
and userId in (select userId from {train_q1table})
and referrer in (select referrer from {train_q1table})