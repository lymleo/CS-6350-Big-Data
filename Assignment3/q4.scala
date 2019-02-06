//Q4. List the user_id , and name of the top 10 users who have written the mostreviews.

//"business_id","full_address","categories"
//"review_id","user_id","business_id","stars"
//"user_id","name","url"

val reviewfile = sc.textFile("/yelp/review/review.csv")

val review = reviewfile.map(a=>a.split('^')).map(a=>(a(1),a(1)))
val userfile = sc.textFile("/yelp/user/user.csv")
val user = userfile.map(a=>a.split('^')).map(a=>(a(0),a(1)))

val afterjoin = review.join(user)

val result = afterjoin.map(x=>(x._2, 1))
val topTen = result.reduceByKey(_+_).takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2)).foreach(println)
