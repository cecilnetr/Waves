package scorex.transaction

import scorex.block.{Block, BlockField, BlockProcessingModule}

import scala.util.Try

trait TransactionModule {

  def genesisData: Seq[Transaction]

  val blockStorage: BlockStorage

  def isValid(block: Block): Boolean

  def isValid(tx: Transaction, blockTime: Long): Boolean

  def unconfirmedTxs: Seq[Transaction]

  def putUnconfirmedIfNew(tx: Transaction): Boolean

  def packUnconfirmed(): Seq[Transaction]

  def clearFromUnconfirmed(data: Seq[Transaction]): Unit

  def onNewOffchainTransaction(transaction: Transaction): Unit

}
