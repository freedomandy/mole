package org.freedomandy.mole.commons.exceptions

/**
  * @author Andy Huang on 2018/8/21
  */
class UnsupportedException(errorMsg: String, t: Option[Throwable] = None) extends BaseException(errorMsg, t) {
  override def errorCode: String = "00-006"
}
