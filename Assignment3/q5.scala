/* Q5. List the business_id, and count of each business's ratings for the businesses that
are located in the state of TX */

//"business_id","full_address","categories"
//"review_id","user_id","business_id","stars"
//"user_id","name","url"

val businessfile = sc.textFile("/yelp/business/business.csv")

val business = businessfile.map(a=>a.split('^')).filter(a=>a(1).contains("TX")).map(a=>(a(0),a(0)))

val reviewfile = sc.textFile("/yelp/review/review.csv")

val review = reviewfile.map(a=>a.split('^')).map(a=>(a(2),a(2)))

val afterjoin = review.join(business)
val result = afterjoin.map(x=>(x._1, 1))

val count = result.reduceByKey(_+_)

count.collect().foreach(println)