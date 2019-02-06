//Q3. List the 'user id' and 'stars' of users that reviewed businesses located in Stanford.

//"business_id","full_address","categories"
//"review_id","user_id","business_id","stars"
//"user_id","name","url"

val businessfile = sc.textFile("/yelp/business/business.csv")
val business = businessfile.map(a=>a.split('^')).filter(a=>a(1).contains("Stanford")).map(a=>(a(0),""))

val reviewfile = sc.textFile("/yelp/review/review.csv")
val review = reviewfile.map(a=>a.split('^')).map(a=>(a(2),(a(1),a(3))))

val afterjoin = business.join(review)
val result = afterjoin.map { case (left, (a, b)) => b}

result.collect().foreach(println)