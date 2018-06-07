package org.freedomandy.mole.commons.exceptions

/**
  * @author Andy Huang on 2018/5/30
  */
class InvalidInputException(errorMsg: String, t: Option[Throwable] = None) extends BaseException(errorMsg, t) {
  override def errorCode: String = "00-003"
}
