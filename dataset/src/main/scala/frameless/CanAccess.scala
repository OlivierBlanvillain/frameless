package frameless

/**
 * Can column of table `T` be accessed in this scope?
 */
// @annotation.implicitNotFound("TODO")
trait CanAccess[T]

object CanAccess {
  private[frameless] val instance: CanAccess[Any] = new CanAccess[Any] {}
  private[frameless] def theToken[T]: CanAccess[T] = instance.asInstanceOf[CanAccess[T]]

  implicit def canAccessL[L, R](c: CanAccess[(L, R)]): CanAccess[L] = theToken[L]
  implicit def canAccessR[L, R](c: CanAccess[(L, R)]): CanAccess[R] = theToken[R]
}

package object unsafeColmunAccess {
  implicit def unrestrictedColumnAccess[X]: CanAccess[X] = CanAccess.theToken[X]
}
