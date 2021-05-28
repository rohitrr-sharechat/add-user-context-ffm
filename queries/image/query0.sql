CREATE TEMP FUNCTION getCollectiveReferrer(x string)
RETURNS STRING
LANGUAGE js
AS """
    if(x == null)
        return "NULL_STR"
    s = x.toLowerCase();
    if(s.includes("trendingfeed") && s.includes("suggested"))
        return "trendingfeed_suggested";
    else if(s.includes("trendingfeed"))
        return "trendingfeed";
    else if(s.includes("videofeed") && s.includes("suggested"))
        return "videofeed_suggested";
    else if(s.includes("videofeed"))
        return "videofeed";
    else if(s.includes("tagfeed"))
        return "tagfeed";
    else if(s.includes("bucket"))
        return "bucket";
    else if(s.includes("profilepost"))
        return "profilepost";
    else
        return "others";
""";

WITH
ugc as (
  SELECT
    postId,
    APPROX_TOP_COUNT(tagId, 1)[ORDINAL(1)].value as tagId,
  FROM `maximal-furnace-783.sc_analytics.all_posts2`
  WHERE
    time BETWEEN TIMESTAMP_SUB(TIMESTAMP('{common_posts_end_time}'), interval {common_posts_days} day) AND TIMESTAMP('{common_posts_end_time}') AND
    language = '{language}' AND composeType = 'image'
  group by 1
),

view_data as (
  select userId, postId, ANY_VALUE(ugc.tagId) as tagId, getCollectiveReferrer(ANY_VALUE(referrer)) as referrer
  from `maximal-furnace-783.sc_analytics.view_v3` inner join ugc using (postId)
  where
    time BETWEEN TIMESTAMP_SUB(TIMESTAMP('{end_time}'), interval {days} day) AND TIMESTAMP('{end_time}') AND
    lang = '{language}'
    AND referrer is not NULL
  group by userId, postId
),

eng as (
  select
    userId, postId, ANY_VALUE(ugc.tagId) as tagId,
    getCollectiveReferrer(ANY_VALUE(referrer)) as referrer,
    LOGICAL_OR(name = 'Post Like') AS is_like,
    LOGICAL_OR(name = 'Post Shared-V2') AS is_share,
    LOGICAL_OR(name = 'Favourites') AS is_fav,
  from `maximal-furnace-783.sc_analytics.eng_view` inner join ugc using (postId)
  where time BETWEEN TIMESTAMP_SUB(TIMESTAMP('{end_time}'), interval {days} day) AND TIMESTAMP('{end_time}')
  group by userId, postId
),

lpo as (
  select CAST(distinct_id as string) as userId, ugc.postId, ANY_VALUE(ugc.tagId) as tagId, getCollectiveReferrer(ANY_VALUE(referrer)) as referrer, true as is_lpo
  from `maximal-furnace-783.sc_analytics.likers_popup_opened` x inner join ugc on CAST(x.postId as string) = ugc.postId
  where time BETWEEN TIMESTAMP_SUB(TIMESTAMP('{end_time}'), interval {days} day) AND TIMESTAMP('{end_time}')
  group by userId, postId
)

select userId, postId, COALESCE(view_data.tagId, eng.tagId, lpo.tagId) as tagId,
COALESCE(view_data.referrer, eng.referrer, lpo.referrer) as referrer, is_like, is_share, is_fav, is_lpo
from view_data full join eng using (userId, postId) full join lpo using (userId, postId)