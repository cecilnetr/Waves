package scorex.transaction

import scorex.settings.ChainParameters
import scorex.transaction.state.database.state.storage.TheStorage

import scala.language.implicitConversions

trait ValidatorService {

  def validate(trans: Seq[Transaction], blockTime: Long): Seq[Transaction]
}

object ValidatorService {

  implicit class RichState(s: ValidatorService) {

    def validateOne(trans: Transaction, blockTime: Long): Option[Transaction] = ??? //s.validate(Seq(trans), blockTime).headOption

    def allValid(txs: Seq[Transaction], blockTime: Long): Boolean = ??? // s.validate(txs, blockTime).size == txs.size

    def isValid(tx: Transaction, blockTime: Long): Boolean = ??? /// s.validateOne(tx, blockTime).isDefined
  }

}
