package scorex.transaction.state.database.blockchain

import com.google.common.base.Charsets
import scorex.crypto.encode.Base58
import scorex.transaction._
import scorex.transaction.assets.{AssetIssuance, BurnTransaction, IssueTransaction, ReissueTransaction}
import scorex.transaction.state.database.state.extension.StateProcessor
import scorex.transaction.state.database.state.storage.{AssetsExtendedStateStorageI, StateStorageI}
import scorex.utils.ScorexLogging

import scala.util.{Failure, Success}

//TODO move to state.extension package
class AssetsExtendedState(storage: StateStorageI with AssetsExtendedStateStorageI) extends ScorexLogging with StateProcessor {

  override def process(tx: Transaction, blockTs: Long, height: Int): Unit = tx match {
    case tx: AssetIssuance =>
      AssetsExtendedState.addAsset(storage)(tx.assetId, height, tx.id, tx.quantity, tx.reissuable)
    case tx: BurnTransaction =>
      AssetsExtendedState.burnAsset(storage)(tx.assetId, height, tx.id, -tx.amount)
    case _ =>
  }
}

object AssetsExtendedState extends ScorexLogging {

  def isValid(storage: StateStorageI with AssetsExtendedStateStorageI) (tx: Transaction): Boolean = tx match {
    case tx: ReissueTransaction =>
      val reissueValid: Boolean = {
        val sameSender = isIssuerAddress(storage)(tx.assetId, tx.sender.address)
        val reissuable = isReissuable(storage)(tx.assetId)
        sameSender && reissuable
      }
      reissueValid
    case tx: BurnTransaction =>
      isIssuerAddress(storage)(tx.assetId, tx.sender.address)
    case _ => true
  }


  private def isIssuerAddress(storage: StateStorageI)(assetId: Array[Byte], address: String): Boolean = {
    storage.getTransactionBytes(assetId).exists(b =>
      IssueTransaction.parseBytes(b) match {
        case Success(issue) =>
          issue.sender.address == address
        case Failure(f) =>
          log.debug(s"Can't deserialise issue tx", f)
          false
      })
  }


  private[blockchain] def addAsset(storage: AssetsExtendedStateStorageI)(assetId: AssetId, height: Int, transactionId: Array[Byte], quantity: Long, reissuable: Boolean): Unit = {
    val asset = Base58.encode(assetId)
    val transaction = Base58.encode(transactionId)
    val assetAtHeight = s"$asset@$height"
    val assetAtTransaction = s"$asset@$transaction"

    storage.addHeight(asset, height)
    storage.addTransaction(assetAtHeight, transaction)
    storage.setQuantity(assetAtTransaction, quantity)
    storage.setReissuable(assetAtTransaction, reissuable)
  }

  private[blockchain] def burnAsset(storage: AssetsExtendedStateStorageI)(assetId: AssetId, height: Int, transactionId: Array[Byte], quantity: Long): Unit = {
    require(quantity <= 0, "Quantity of burned asset should be negative")

    val asset = Base58.encode(assetId)
    val transaction = Base58.encode(transactionId)
    val assetAtHeight = s"$asset@$height"
    val assetAtTransaction = s"$asset@$transaction"

    storage.addHeight(asset, height)
    storage.addTransaction(assetAtHeight, transaction)
    storage.setQuantity(assetAtTransaction, quantity)
  }

  def rollbackTo(storage: AssetsExtendedStateStorageI)(assetId: AssetId, height: Int): Unit = {
    val asset = Base58.encode(assetId)

    val heights = storage.getHeights(asset)
    val heightsToRemove = heights.filter(h => h > height)
    storage.setHeight(asset, heights -- heightsToRemove)

    val transactionsToRemove: Seq[String] = heightsToRemove.foldLeft(Seq.empty[String]) { (result, h) =>
      result ++ storage.getTransactions(s"$asset@$h")
    }

    val keysToRemove = transactionsToRemove.map(t => s"$asset@$t")

    keysToRemove.foreach { key =>
      storage.removeKey(key)
    }
  }

  def getAssetQuantity(storage: AssetsExtendedStateStorageI)(assetId: AssetId): Long = {
    val asset = Base58.encode(assetId)
    val heights = storage.getHeights(asset)

    val sortedHeights = heights.toSeq.sorted
    val transactions: Seq[String] = sortedHeights.foldLeft(Seq.empty[String]) { (result, h) =>
      result ++ storage.getTransactions(s"$asset@$h")
    }

    transactions.foldLeft(0L) { (result, transaction) =>
      result + storage.getQuantity(s"$asset@$transaction")
    }
  }

  def isReissuable(storage: AssetsExtendedStateStorageI)(assetId: AssetId): Boolean = {
    val asset = Base58.encode(assetId)
    val heights = storage.getHeights(asset)

    val reverseSortedHeight = heights.toSeq.reverse
    if (reverseSortedHeight.nonEmpty) {
      val lastHeight = reverseSortedHeight.head
      val transactions = storage.getTransactions(s"$asset@$lastHeight")
      if (transactions.nonEmpty) {
        val transaction = transactions.toSeq.reverse.head
        storage.isReissuable(s"$asset@$transaction")
      } else false
    } else false
  }

  def getAssetName(storage: StateStorageI)(assetId: AssetId): String = {
    storage.getTransaction(assetId).flatMap {
      case tx: IssueTransaction => Some(tx.asInstanceOf[IssueTransaction])
      case _ => None
    }.map(tx => new String(tx.name, Charsets.UTF_8)).getOrElse("Unknown")
  }
}
