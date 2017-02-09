package scorex.transaction

import play.api.libs.json.JsObject
import scorex.account.Account
import scorex.block.Block
import scorex.transaction.assets.IssueTransaction
import scorex.transaction.state.database.state.{AccState, Reasons}
import scorex.transaction.state.database.state.storage.TheStorage
import scorex.utils.NTP

import scala.util.Try
import scala.language.implicitConversions

trait State {


  // --- READ

  def getAccountBalance(account: Account): Map[AssetId, (Long, Boolean, Long, IssueTransaction)]

  def assetBalance(acc: AssetAcc): Long

  def assetDistribution(byteArray: Array[Byte]): Map[String, Long]

  def validate(txs: Seq[Transaction], blockTime: Long): Seq[Transaction] // Seq[Either[ValidationError,Transaction]]

  def included(signature: Array[Byte]): Option[Int]

  def balance(account: Account, height: Int): Long

  def balanceWithConfirmations(account: Account, confirmations: Int, heightOpt: Option[Int] = None): Long

  val DefaultLimit = 50
  def accountTransactions(account: Account, limit: Int = DefaultLimit): Seq[_ <: Transaction]

  // --- DEBUG READ

  def stateHeight: Int

  def hash: Int

  def toWavesJson(height: Int): JsObject

  def toJson(heightOpt: Option[Int]): JsObject

  // --- SYNCHRONIZED WRITES

  def applyChanges(changes: Map[AssetAcc, (AccState, Reasons)], blockTs: Long = NTP.correctedTime()): Unit

  def rollbackTo(height: Int): State

  // TEST-ONLY

  def isTValid(transaction: Transaction): Boolean

  def calcNewBalances(trans: Seq[Transaction], fees: Map[AssetAcc, (AccState, Reasons)], allowTemporaryNegative: Boolean):
  Map[AssetAcc, (AccState, Reasons)]

  def totalAssetQuantity(assetId: AssetId): Long

  def totalBalance: Long

  def applyBlock(block: Block): Try[State]

  // --- RAW
  def storage: TheStorage
}

object State {
  implicit def richState(s: State): RichState = new RichState(s)

  class RichState(s: State) {

    def validateOne(trans: Transaction, blockTime: Long): Option[Transaction] = s.validate(Seq(trans), blockTime).headOption

    def allValid(txs: Seq[Transaction], blockTime: Long): Boolean = s.validate(txs, blockTime).size == txs.size

    def isValid(tx: Transaction, blockTime: Long): Boolean = s.validateOne(tx, blockTime).isDefined
  }

}
