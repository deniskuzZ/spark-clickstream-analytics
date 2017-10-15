package clickstream

/**
  * Created by kuzmende on 10/14/17.
  */
package object domain {

  case class EventRecord(adjustedTimestamp: Long, referrer: String, action: String, prevPage: String,
                         visitor: String, page: String, product: String) {
    override def toString = {
      s"$adjustedTimestamp\t$referrer\t$action\t$prevPage\t$visitor\t$page\t$product\n"
    }
  }

}
