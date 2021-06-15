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
    IFNULL(repeatCount, 0) as repeatCount, IFNULL(percentageFloat, 0) as percentageFloat
  from `maximal-furnace-783.sc_analytics.video_play` vp inner join ugc on CAST(vp.postId as STRING) = ugc.postId
  where 
    time BETWEEN TIMESTAMP_SUB(TIMESTAMP('{end_time}'), interval {days} day) AND TIMESTAMP('{end_time}') AND
    (percentageFloat IS NULL OR percentageFloat BETWEEN 0 AND 100) AND
    (duration IS NULL OR duration BETWEEN 0 AND 1e5) AND
    (repeatCount IS NULL OR repeatCount BETWEEN 0 AND 1e5)
),

vid_dur as (
  SELECT postId, (APPROX_TOP_COUNT(duration, 1))[ORDINAL(1)].value as duration, ANY_VALUE(tagId) as tagId
  FROM vp_temp
  GROUP BY postId
),

vp_succ as (
  select 
    userId, postId, ANY_VALUE(vid_dur.tagId) as tagId, 
    extract(hour from MIN(time) at TIME ZONE "Asia/Kolkata") as hour_of_day,
    extract(dayofweek from MIN(time) at TIME ZONE "Asia/Kolkata") as day_of_week,
    LOGICAL_OR(repeatCount > 1 OR (repeatCount > 0 AND vid_dur.duration*(percentageFloat/100) > 1)) as is_vp_succ,
    sum(if(vid_dur.duration between 5 and 19 and vid_dur.duration*(repeatCount+percentageFloat/100) > (((vid_dur.duration - 5) * 3.7189182474712084) / 15) + 10.95394866571609, 1,
          if(vid_dur.duration between 20 and 34 and vid_dur.duration*(repeatCount+percentageFloat/100) > (((vid_dur.duration - 20) * 7.005242627404698) / 15) + 20.989031673191437, 1,
            if(vid_dur.duration between 35 and 49 and vid_dur.duration*(repeatCount+percentageFloat/100) > (((vid_dur.duration - 35) * 9.664034032498712) / 15) + 30.722644851502906, 1,
              if(vid_dur.duration between 50 and 63 and vid_dur.duration*(repeatCount+percentageFloat/100) > (((vid_dur.duration - 50) * 17.073812758946673) / 13) + 34.34993065881188, 1,
                if(vid_dur.duration > 63 and vid_dur.duration*(repeatCount+percentageFloat/100) > 50,1,0
                )
              )
            )
          )
        )
    )=1 as is_vp_succ2,
    LOGICAL_OR(vid_dur.duration*(repeatCount+percentageFloat/100) < 5) as is_vp_skip,
  from vp_temp inner join vid_dur using (postId)
  where vid_dur.duration >= 5
  group by userId, postId
),

eng as (
  select
    userId, postId, ANY_VALUE(ugc.tagId) as tagId,
    LOGICAL_OR(name = 'Post Like') AS is_like,
    LOGICAL_OR(name = 'Post Shared-V2') AS is_share,
    LOGICAL_OR(name = 'Favourites') AS is_fav,
  from `maximal-furnace-783.sc_analytics.eng_view` inner join ugc using (postId)
  where time BETWEEN TIMESTAMP_SUB(TIMESTAMP('{end_time}'), interval {days} day) AND TIMESTAMP('{end_time}')
  group by userId, postId
),

user_info as (
    select id as userId, userCity, userState, CASE
                                                when price_1 is not null and price_2 is not null then CAST((price_1+price_2)/2 as INT64) 
                                                when price_1 is not null then price_1
                                                when price_2 is not null then price_2
                                                else null
                                                end
                                                as averagePhonePrice
    from `maximal-furnace-783.sc_analytics.user` user
    join `maximal-furnace-783.moj_analytics.phone_price_updated` phone
    on user.phoneModel = phone.mobile_model_name
)

select * except (hour_of_day, day_of_week, userCity, userState, averagePhonePrice), concat(lower(IFNULL(userCity, "noCityLocation")), "_", lower(IFNULL(userState, "noStateLocation"))) as locationBucket, 
          concat(cast(RANGE_BUCKET(hour_of_day, [7, 9, 11, 14, 16, 19, 21]) as STRING), "_", cast(day_of_week as STRING)) as actionTimeBucket,
          cast(IFNULL(RANGE_BUCKET(averagePhonePrice, [7499, 8999, 10999 ,14000]), -1) as STRING) as priceBucket
from vp_succ left join eng using (userId, postId, tagId)
left join user_info using (userId);