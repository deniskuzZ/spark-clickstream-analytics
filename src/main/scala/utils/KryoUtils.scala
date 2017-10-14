package utils

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.pool.{KryoCallback, KryoFactory, KryoPool}
import org.objenesis.strategy.StdInstantiatorStrategy
import org.slf4j.LoggerFactory

/**
  * Created by kuzmende on 10/14/17.
  */
object KryoUtils {

  val factory: KryoFactory = new KryoFactory() {
    override def create(): Kryo = {
      val kryo: Kryo = new Kryo()
      kryo.setRegistrationRequired(false)
      kryo.getInstantiatorStrategy.asInstanceOf[Kryo.DefaultInstantiatorStrategy].setFallbackInstantiatorStrategy(new StdInstantiatorStrategy())

      kryo
    }
  }

  val pool: KryoPool = new KryoPool.Builder(factory).softReferences().build()

  def serialize[T](obj: T): Array[Byte] = {
    pool.run(new KryoCallback[Array[Byte]]() {
      override def execute(kryo: Kryo): Array[Byte] = {
        val stream = new ByteArrayOutputStream
        val output = new Output(stream)

        kryo.writeClassAndObject(output, obj)
        output.close()
        stream.toByteArray
      }
    })
  }

  def deserialize[T](obj: Array[Byte]): T = {
    pool.run(new KryoCallback[T] {
      override def execute(kryo: Kryo): T = {
        val input: Input = new Input(obj)
        kryo.readClassAndObject(input).asInstanceOf[T]
      }
    })
  }

}
