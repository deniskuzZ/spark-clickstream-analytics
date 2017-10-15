package utils.serde

import java.util

import org.apache.kafka.common.serialization.Deserializer
import utils.KryoUtils

/**
  * Created by kuzmende on 10/14/17.
  */
class KryoDeserializer extends Deserializer[Object] {

  override def deserialize(topic: String, data: Array[Byte]): Object = {
    KryoUtils.deserialize(data)
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    /*nothing to be done here*/
  }

  override def close(): Unit = {
    /*nothing to be done here*/
  }
}
