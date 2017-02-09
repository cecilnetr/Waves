package scorex.transaction.state.database.state.extension

import scorex.transaction.state.database.state.storage.StateStorageI
import scorex.transaction.{GenesisTransaction, Transaction}

object GenesisValidator {

  def isValid(storage: StateStorageI)(tx: Transaction): Boolean = tx match {
    case gtx: GenesisTransaction => storage.stateHeight == 0
    case _ => true
  }

}