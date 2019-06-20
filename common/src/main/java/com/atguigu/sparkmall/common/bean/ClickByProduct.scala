package com.atguigu.sparkmall.common.bean

/**
  *
  *
  * select
	count(uv.click_product_id) click_num ,
	uv.click_product_id,
	pi.product_name,
	uv.city_id \
  */
 case class ClickByProduct(aa:Long, area:String, city_id:Long, city_name:String,
																	 click_product_id:Long, countall:Long, rate:Double, rate100:String,ranks:Long)  extends  Ordering[ClickByProduct]{
	override def compare(x: ClickByProduct, y: ClickByProduct): Int =( x.aa-y.aa).toInt
}
