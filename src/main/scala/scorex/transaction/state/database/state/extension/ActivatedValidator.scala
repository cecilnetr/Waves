package scorex.transaction.state.database.state.extension

import scorex.settings.ChainParameters
import scorex.transaction.assets.exchange.ExchangeTransaction
import scorex.transaction.assets.{BurnTransaction, IssueTransaction, ReissueTransaction, TransferTransaction}
import scorex.transaction.{GenesisTransaction, PaymentTransaction, Transaction}

class ActivatedValidator(settings: ChainParameters) extends StateValidator {


  override def isValid(tx: Transaction): Boolean = tx match {
    case tx: PaymentTransaction => true
    case gtx: GenesisTransaction => true
    case tx: TransferTransaction => true
    case tx: IssueTransaction => true
    case tx: ReissueTransaction => true
    case tx: BurnTransaction => tx.timestamp > settings.allowBurnTransactionAfterTimestamp
    case tx: ExchangeTransaction => true
    case _ => false
  }

}