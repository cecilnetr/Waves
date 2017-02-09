package scorex.transaction.state.database.state.extension

import scorex.transaction.Transaction

trait StateValidator {
  def isValid(tx: Transaction): Boolean

}

trait StateProcessor {
  def process(tx: Transaction, blockTs: Long, height: Int): Unit

}
