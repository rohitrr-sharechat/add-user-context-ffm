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

vp_temp as (
  select
    * except (postId, userId, tagId, repeatCount, percentageFloat),
    ugc.postId, ugc.tagId, SAFE_CAST(userId as STRING) as userId,
    IFNULL(repeatCount, 0) as repeatCount, IFNULL(percentageFloat, 0) as percentageFloat,
  from `maximal-furnace-783.sc_analytics.video_play` vp inner join ugc on CAST(vp.postId as STRING) = ugc.postId
  where 
    time BETWEEN TIMESTAMP_SUB(TIMESTAMP('{end_time}'), interval {days} day) AND TIMESTAMP('{end_time}') AND
    (percentageFloat IS NULL OR percentageFloat BETWEEN 0 AND 100) AND
    (duration IS NULL OR duration BETWEEN 0 AND 1e5) AND
    (repeatCount IS NULL OR repeatCount BETWEEN 0 AND 1e5)
)


SELECT postId, (APPROX_TOP_COUNT(duration, 1))[ORDINAL(1)].value as duration, ANY_VALUE(tagId) as tagId
  FROM vp_temp
  GROUP BY postId


