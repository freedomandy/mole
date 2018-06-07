package org.freedomandy.mole.commons.exceptions

/**
  * @author Andy Huang on 2018/5/30
  */
case class BaseException(errorMsg: String, t: Option[Throwable] = None) extends RuntimeException(errorMsg) {
  def errorCode: String = "00-000"
}
