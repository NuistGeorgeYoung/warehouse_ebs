package core

import java.util.Properties

import core.MysqlConfigFactory.{Getter, Setter}
import core.Validator.check

class MysqlConfigFactory {
  def build():Setter={
    new Setter with Serializable {
      val conf = new Properties()
      override def setDriver(driverClass: String): Setter = {
        check("name_of_mysql_driver_class",driverClass)
        conf.setProperty("driver",driverClass)
        this
      }

      override def setUrl(url: String): Setter = {
        check("url_to_conn_mysql",url)
        conf.setProperty("url",url)
        this
      }

      override def setUser(user: String): Setter = {
        check("user_of_mysql",user)
        conf.setProperty("user",user)
        this
      }

      override def setPassword(password: String): Setter = {
        check("password_to_conn_mysql;",password)
        conf.setProperty("password",password)
        this
      }

      override def finish(): Getter = {
        new Getter {
          override def getUrl: String = conf.getProperty("url")
          override def getConf: Properties = conf
        }

      }
    }
  }
}

object MysqlConfigFactory{
  def apply(): MysqlConfigFactory = new MysqlConfigFactory()
  trait Getter{
    def getUrl:String
    def getConf:Properties
  }
  
  trait Setter{
    def setDriver(driverClass:String):Setter
    def setUrl(url:String):Setter
    def setUser(user:String):Setter
    def setPassword(password:String):Setter

    def finish():Getter
  }
}
