package org.dist.kvstore

import com.fasterxml.jackson.annotation.{JsonAutoDetect, PropertyAccessor}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object JsonSerDes {

  def serialize(obj:Any):String = {
    val objectMapper = new ObjectMapper()
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.writeValueAsString(obj)
  }

  def deserialize[T](json:String, clazz:Class[T]):T = {
    deserialize(json.getBytes(), clazz)
  }

  def deserialize[T](json:Array[Byte], clazz:Class[T]):T = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val module = new SimpleModule()
    module.addKeyDeserializer(classOf[InetAddressAndPort], new InetAddressAndPortKeyDeserializer())
    objectMapper.registerModule(module)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.readValue(json, clazz)
  }


  class InetAddressAndPortKeyDeserializer extends KeyDeserializer {
    override def deserializeKey(key: String, ctxt: DeserializationContext): AnyRef = {
      if (key.startsWith("[") && key.endsWith("]")) {
        val parts = key.substring(1, key.length - 1).split(',')
        InetAddressAndPort.create(parts(0), parts(1).toInt)
      } else
        throw new IllegalArgumentException(s"${key} is not valid InetAddressAndPort")
    }
  }


  def deserialize[T](json:String, typeRef:TypeReference[T]):T = {
    deserialize(json.getBytes(), typeRef)
  }
  def deserialize[T](json:Array[Byte], typeRef:TypeReference[T]):T = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.readValue(json, typeRef)
  }

}
