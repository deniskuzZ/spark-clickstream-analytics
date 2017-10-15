package utils.serde

import java.util

import org.apache.kafka.common.serialization.Serializer
import utils.KryoUtils

/**
  * Created by kuzmende on 10/14/17.
  */
class KryoSerializer extends Serializer[Object] {

  override def serialize(topic: String, data: Object): Array[Byte] = {
    KryoUtils.serialize(data)
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    /*nothing to be done here*/
  }

  override def close(): Unit = {
    /*nothing to be done here*/
  }
}