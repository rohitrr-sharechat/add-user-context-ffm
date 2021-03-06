with
allmap as (SELECT * FROM {q2table})

select concat(cast(score as int64), " ", "0:", user.mapping, ":1 ", 
"1:", post.mapping, ":1 ",
"2:", priceBucket.mapping, ":1 ") data
from {q1table} tr
       join allmap user on (concat('2_user_', tr.userId) = user.feature_name)
       join allmap post on (concat('1_post_', tr.postId) = post.feature_name)
       join allmap priceBucket on (concat('3_priceBucket_', tr.priceBucket) = priceBucket.feature_name)