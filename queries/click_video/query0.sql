WITH
ugc as (
  SELECT
    postId,
    APPROX_TOP_COUNT(tagId, 1)[ORDINAL(1)].value as tagId,
  FROM `maximal-furnace-783.sc_analytics.all_posts2`
  WHERE
    time BETWEEN TIMESTAMP_SUB(TIMESTAMP('{common_posts_end_time}'), interval {common_posts_days} day) AND TIMESTAMP('{common_posts_end_time}') AND
    language = '{language}' AND composeType = 'video'
  group by 1
),

vid_view as (
  SELECT vc.userId, ugc.postId, ugc.tagId 
  FROM `maximal-furnace-783.sc_analytics.view_v3` vc inner join ugc on vc.postId = ugc.postId
  WHERE
    time BETWEEN TIMESTAMP_SUB(TIMESTAMP('{end_time}'), interval {days} day) AND TIMESTAMP('{end_time}') AND 
    lang = '{language}' AND contentType = 'video' AND
    lower(referrer) NOT LIKE "%sugg%" AND
    lower(referrer) NOT LIKE "%morefeed%"
),

vp_temp as (
  select
    SAFE_CAST(postId as STRING) as postId, SAFE_CAST(userId as STRING) as userId,
    TRUE as is_vp_click,
  from `maximal-furnace-783.sc_analytics.video_play`
  where 
    time BETWEEN TIMESTAMP_SUB(TIMESTAMP('{end_time}'), interval {days} day) AND TIMESTAMP('{end_time}') AND
    (percentageFloat IS NULL OR percentageFloat BETWEEN 0 AND 100) AND
    (duration IS NULL OR duration BETWEEN 0 AND 1e5) AND
    (repeatCount IS NULL OR repeatCount BETWEEN 0 AND 1e5) AND
    (sourcePostId IS NULL)
)

-- eng as (
--   select
--     userId, postId, ANY_VALUE(ugc.tagId) as tagId,
--     LOGICAL_OR(name = 'Post Like') AS is_like,
--     LOGICAL_OR(name = 'Post Shared-V2') AS is_share,
--     LOGICAL_OR(name = 'Favourites') AS is_fav,
--   from `maximal-furnace-783.sc_analytics.eng_view` inner join ugc using (postId)
--   where time BETWEEN TIMESTAMP_SUB(TIMESTAMP('2021-03-20 09:41:13.897920'), interval 1 day) AND TIMESTAMP('{end_time}')
--   group by userId, postId
-- )

-- select *
-- from vid_view left join eng using (userId, postId, tagId)

select userId, postId, tagId, 
IFNULL(vp_temp.is_vp_click, FALSE) as is_vp_click
from vid_view left join vp_temp using (postId, userId)