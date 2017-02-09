package scorex.consensus

import scorex.account.{Account, PrivateKeyAccount, PublicKeyAccount}
import scorex.block.{Block, BlockField}
import scorex.consensus.nxt.NxtLikeConsensusBlockData
import scorex.settings.ChainParameters
import scorex.transaction.TransactionModule

trait ConsensusModule {

  def forksConfig: ChainParameters

  def generatingBalance(account: Account, atHeight: Option[Int] = None)
                       (implicit transactionModule: TransactionModule): Long =
    transactionModule.blockStorage.state
      .balanceWithConfirmations(account, if (atHeight.exists(h => h >= forksConfig.generatingBalanceDepthFrom50To1000AfterHeight)) 1000 else 50, atHeight)

  def isValid(block: Block)(implicit transactionModule: TransactionModule): Boolean

  def blockOrdering(implicit transactionModule: TransactionModule): Ordering[(Block)] =
    Ordering.by {
      block =>
        val parent = transactionModule.blockStorage.history.blockById(block.referenceField.value).get
        val blockCreationTime = nextBlockGenerationTime(parent, block.signerDataField.value.generator)
          .getOrElse(block.timestampField.value)

        (block.blockScore, -blockCreationTime)
    }

  def generateNextBlock(account: PrivateKeyAccount)
                       (implicit transactionModule: TransactionModule): Option[Block]

  def generateNextBlocks(accounts: Seq[PrivateKeyAccount])
                        (implicit transactionModule: TransactionModule): Seq[Block] =
    accounts.flatMap(generateNextBlock(_))

  def nextBlockGenerationTime(lastBlock: Block, account: PublicKeyAccount)
                             (implicit transactionModule: TransactionModule): Option[Long]

}
