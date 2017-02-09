package scorex.transaction.state.database.state.extension

import scorex.settings.ChainParameters
import scorex.transaction.state.database.state.storage.StateStorageI
import scorex.transaction.{PaymentTransaction, Transaction}

class IncludedStateProcessor(storage: StateStorageI, settings: ChainParameters) extends StateProcessor {
  override def process(tx: Transaction, blockTs: Long, height: Int): Unit = {
    storage.putTransaction(tx, height)
  }
}

object IncludedValidator {
  def isValid(storage: StateStorageI, settings: ChainParameters)(tx: Transaction): Boolean = tx match {
    case tx: PaymentTransaction if tx.timestamp < settings.requirePaymentUniqueId => true
    case tx: Transaction => storage.included(tx.id).isEmpty
  }
}
