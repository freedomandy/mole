package org.freedomandy.mole.commons.exceptions

/**
  * @author Andy Huang on 2018/7/25
  */
class UnsatisfiedPropException(errorMsg: String, t: Option[Throwable] = None) extends BaseException(errorMsg, t) {
  override def errorCode: String = "00-005"
}
