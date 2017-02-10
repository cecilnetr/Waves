package scorex.transaction.state.database.blockchain

import com.google.common.base.Charsets
import play.api.libs.json.{JsNumber, JsObject}
import scorex.account.Account
import scorex.block.Block
import scorex.crypto.encode.Base58
import scorex.crypto.hash.FastCryptographicHash
import scorex.settings.ChainParameters
import scorex.transaction._
import scorex.transaction.assets._
import scorex.transaction.assets.exchange.{ExchangeTransaction, Order}
import scorex.transaction.state.database.state._
import scorex.transaction.state.database.state.storage._
import scorex.utils.NTP

import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object StoredState {
  def included(storage: StateStorageI)(id: Array[Byte]): Option[Int] = storage.included(id)

  def stateHeight(storage: StateStorageI): Int = storage.stateHeight

  def getAccountBalance(storage: StateStorageI with AssetsStateStorageI)(account: Account): Map[AssetId, (Long, Boolean, Long, IssueTransaction)] = {
    val address = account.address
    storage.accountAssets(address).foldLeft(Map.empty[AssetId, (Long, Boolean, Long, IssueTransaction)]) { (result, asset) =>
      val triedAssetId = Base58.decode(asset)
      val balance = currentBalanceByKey(storage)(address + asset)

      if (triedAssetId.isSuccess) {
        val assetId = triedAssetId.get
        val maybeIssueTransaction = getIssueTransaction(storage)(assetId)
        if (maybeIssueTransaction.isDefined)
          result.updated(assetId, (balance, isReissuable(storage)(assetId), totalAssetQuantity(storage)(assetId),
            maybeIssueTransaction.get))
        else result
      } else result
    }
  }

  def rollbackTo(storage: StateStorageI with AssetsStateStorageI)(height: Int): Unit = {
    def deleteNewer(key: Address): Unit = {
      val currentHeight = storage.getLastStates(key).getOrElse(0)
      if (currentHeight > height) {
        val changes = storage.removeAccountChanges(key, currentHeight)
        changes.reason.foreach(id => {
          storage.removeTransaction(id)
          storage.getTransaction(id) match {
            case Some(t: AssetIssuance) =>
              rollbackAssets(storage)(t.assetId, currentHeight)
            case Some(t: BurnTransaction) =>
              rollbackAssets(storage)(t.assetId, currentHeight)
            case _ =>
          }
        })
        val prevHeight = changes.lastRowHeight
        storage.putLastStates(key, prevHeight)
        deleteNewer(key)
      }
    }

    storage.lastStatesKeys.foreach { key =>
      deleteNewer(key)
    }
    storage.setStateHeight(height)
  }

  def applyBlock(storage: TheStorage, settings: ChainParameters)(block: Block) = Try {
    val fees: Map[AssetAcc, (AccState, List[FeesStateChange])] =
      block.transactionData
        .map(_.assetFee)
        .map(a => AssetAcc(block.signerData.generator, a._1) -> a._2)
        .groupBy(a => a._1)
        .mapValues(_.map(_._2).sum)
        .map(m => m._1 -> (AccState(assetBalance(storage)(m._1) + m._2), List(FeesStateChange(m._2))))

    val newBalances: Map[AssetAcc, (AccState, Reasons)] = calcNewBalances(storage: StateStorageI)(block.transactionData, fees, block.timestamp < settings.allowTemporaryNegativeUntil)
    newBalances.foreach(nb => require(nb._2._1.balance >= 0))

    applyChanges(storage, settings)(newBalances, block.timestamp)
  }

  def balance(storage: StateStorageI)(account: Account, atHeight: Int): Long =
    balanceByKeyAtHeight(storage)(AssetAcc(account, None).key, atHeight)

  def assetBalance(storage: StateStorageI)(account: AssetAcc): Long = {
    currentBalanceByKey(storage)(account.key)
  }

  def balanceWithConfirmations(storage: StateStorageI)(account: Account, confirmations: Int, heightOpt: Option[Int]): Long =
    balance(storage)(account, Math.max(1, heightOpt.getOrElse(storage.stateHeight) - confirmations))

  def accountTransactions(storage: StateStorageI)(account: Account, limit: Int = 50): Seq[Transaction] = {
    val accountAssets = storage.accountAssets(account.address)
    val keys = account.address :: accountAssets.map(account.address + _).toList

    def getTxSize(m: SortedMap[Int, Set[Transaction]]) = m.foldLeft(0)((size, txs) => size + txs._2.size)

    def getRowTxs(row: Row): Set[Transaction] = row.reason.flatMap(id => storage.getTransaction(id)).toSet

    keys.foldLeft(SortedMap.empty[Int, Set[Transaction]]) { (result, key) =>

      storage.getLastStates(key) match {
        case Some(accHeight) if getTxSize(result) < limit || accHeight > result.firstKey =>
          @tailrec
          def loop(h: Int, acc: SortedMap[Int, Set[Transaction]]): SortedMap[Int, Set[Transaction]] = {
            storage.getAccountChanges(key, h) match {
              case Some(row) =>
                val rowTxs = getRowTxs(row)
                val resAcc = acc + (h -> (rowTxs ++ acc.getOrElse(h, Set.empty[Transaction])))
                if (getTxSize(resAcc) < limit) {
                  loop(row.lastRowHeight, resAcc)
                } else {
                  if (row.lastRowHeight > resAcc.firstKey) loop(row.lastRowHeight, resAcc.tail)
                  else resAcc
                }
              case _ => acc
            }
          }

          loop(accHeight, result)
        case _ => result
      }

    }.values.flatten.toList.sortWith(_.timestamp > _.timestamp).take(limit)
  }

  def validate(storage: TheStorage, settings: ChainParameters)(trans: Seq[Transaction], blockTime: Long): Seq[Transaction] = {

    val txs = trans.filter(t => isTValid(storage, settings)(t))

    val allowInvalidPaymentTransactionsByTimestamp = txs.nonEmpty && txs.map(_.timestamp).max < settings.allowInvalidPaymentTransactionsByTimestamp
    val validTransactions = if (allowInvalidPaymentTransactionsByTimestamp) {
      txs
    } else {
      val invalidPaymentTransactionsByTimestamp = invalidatePaymentTransactionsByTimestamp(storage)(txs)
      excludeTransactions(txs, invalidPaymentTransactionsByTimestamp)
    }

    val allowTransactionsFromFutureByTimestamp = validTransactions.nonEmpty && validTransactions.map(_.timestamp).max < settings.allowTransactionsFromFutureUntil
    val filteredFromFuture = if (allowTransactionsFromFutureByTimestamp) {
      validTransactions
    } else {
      filterTransactionsFromFuture(validTransactions, blockTime)
    }

    val allowUnissuedAssets = filteredFromFuture.nonEmpty && txs.map(_.timestamp).max < settings.allowUnissuedAssetsUntil


    def filterValidTransactionsByState(trans: Seq[Transaction]): Seq[Transaction] = {
      val (state, validTxs) = trans.foldLeft((Map.empty[AssetAcc, (AccState, ReasonIds)], Seq.empty[Transaction])) {
        case ((currentState, seq), tx) =>
          try {
            val changes = if (allowUnissuedAssets) tx.balanceChanges() else tx.balanceChanges().sortBy(_.delta)
            val newState = changes.foldLeft(currentState) { case (iChanges, bc) =>
              //update balances sheet
              def safeSum(first: Long, second: Long): Long = {
                try {
                  Math.addExact(first, second)
                } catch {
                  case e: ArithmeticException =>
                    throw new Error(s"Transaction leads to overflow balance: $first + $second = ${first + second}")
                }
              }

              val currentChange = iChanges.getOrElse(bc.assetAcc, (AccState(assetBalance(storage)(bc.assetAcc)), List.empty))
              val newBalance = safeSum(currentChange._1.balance, bc.delta)
              if (newBalance >= 0 || tx.timestamp < settings.allowTemporaryNegativeUntil) {
                iChanges.updated(bc.assetAcc, (AccState(newBalance), tx.id +: currentChange._2))
              } else {
                throw new Error(s"Transaction leads to negative state: ${currentChange._1.balance} + ${bc.delta} = ${currentChange._1.balance + bc.delta}")
              }
            }
            (newState, seq :+ tx)
          } catch {
            case NonFatal(e) =>
              (currentState, seq)
          }
      }
      validTxs
    }

    filterValidTransactionsByState(filteredFromFuture)
  }

  def calcNewBalances(storage: StateStorageI)(trans: Seq[Transaction], fees: Map[AssetAcc, (AccState, Reasons)], allowTemporaryNegative: Boolean):
  Map[AssetAcc, (AccState, Reasons)] = {
    val newBalances: Map[AssetAcc, (AccState, Reasons)] = trans.foldLeft(fees) { case (changes, tx) =>
      tx.balanceChanges().foldLeft(changes) { case (iChanges, bc) =>
        //update balances sheet
        val currentChange = iChanges.getOrElse(bc.assetAcc, (AccState(assetBalance(storage)(bc.assetAcc)), List.empty))
        val newBalance = if (currentChange._1.balance == Long.MinValue) Long.MinValue
        else Try(Math.addExact(currentChange._1.balance, bc.delta)).getOrElse(Long.MinValue)

        if (newBalance < 0 && !allowTemporaryNegative) {
          throw new Error(s"Transaction leads to negative balance ($newBalance): ${tx.json}")
        }

        iChanges.updated(bc.assetAcc, (AccState(newBalance), tx +: currentChange._2))
      }
    }
    newBalances
  }

  def totalAssetQuantity(storage: AssetsStateStorageI)(assetId: AssetId): Long = getAssetQuantity(storage)(assetId)

  def applyChanges(storage: StateStorageI with AssetsStateStorageI with OrderMatchStorageI, settings: ChainParameters)
                  (changes: Map[AssetAcc, (AccState, Reasons)], blockTs: Long = NTP.correctedTime()): Unit = {
    storage.setStateHeight(storage.stateHeight + 1)
    val h = storage.stateHeight
    changes.foreach { ch =>
      val change = Row(ch._2._1, ch._2._2.map(_.id), storage.getLastStates(ch._1.key).getOrElse(0))
      storage.putAccountChanges(ch._1.key, h, change)
      storage.putLastStates(ch._1.key, h)
      ch._2._2.foreach {
        case tx: Transaction =>
          val functions: Seq[(Transaction, Long, Int) => Unit] = Seq(
            applyAssetTranscations(storage),
            applyExchangeTransaction(storage),
            putTransaction(storage, settings))
          functions.foreach(_.apply(tx, blockTs, h))
        case _ =>
      }
      ch._1.assetId.foreach(storage.updateAccountAssets(ch._1.account.address, _))
    }
  }

  private[blockchain] def filterValidTransactions(storage: StateStorageI, settings: ChainParameters)(trans: Seq[Transaction]): Seq[Transaction] = {
    trans.foldLeft((Map.empty[AssetAcc, (AccState, ReasonIds)], Seq.empty[Transaction])) {
      case ((currentState, validTxs), tx) =>
        try {
          val newState = tx.balanceChanges().foldLeft(currentState) { case (iChanges, bc) =>
            //update balances sheet
            val currentChange = iChanges.getOrElse(bc.assetAcc, (AccState(assetBalance(storage)(bc.assetAcc)), List.empty))
            val newBalance = if (currentChange._1.balance == Long.MinValue) Long.MinValue
            else Try(Math.addExact(currentChange._1.balance, bc.delta)).getOrElse(Long.MinValue)

            if (newBalance < 0 && tx.timestamp >= settings.allowTemporaryNegativeUntil) {
              throw new Error(s"Transaction leads to negative balance ($newBalance): ${tx.json}")
            }

            iChanges.updated(bc.assetAcc, (AccState(newBalance), tx.id +: currentChange._2))
          }
          (newState, validTxs :+ tx)
        } catch {
          case NonFatal(e) =>
            (currentState, validTxs)
        }
    }._2
  }

  private def currentBalanceByKey(storage: StateStorageI)(key: String): Long
  = balanceByKeyAtHeight(storage: StateStorageI)(key, storage.stateHeight)

  private def balanceByKeyAtHeight(storage: StateStorageI)(key: String, atHeight: Int): Long = {
    storage.getLastStates(key) match {
      case Some(h) if h > 0 =>
        require(atHeight >= 0, s"Height should not be negative, $atHeight given")

        def loop(hh: Int, min: Long = Long.MaxValue): Long = {
          val rowOpt = storage.getAccountChanges(key, hh)
          require(rowOpt.isDefined, s"accountChanges($key).get($hh) is null. lastStates.get(address)=$h")
          val row = rowOpt.get
          if (hh <= atHeight) Math.min(row.state.balance, min)
          else if (row.lastRowHeight == 0) 0L
          else loop(row.lastRowHeight, Math.min(row.state.balance, min))
        }

        loop(h)
      case _ =>
        0L
    }
  }

  private def excludeTransactions(transactions: Seq[Transaction], exclude: Iterable[Transaction]) =
    transactions.filter(t1 => !exclude.exists(t2 => t2.id sameElements t1.id))


  private def filterTransactionsFromFuture(transactions: Seq[Transaction], blockTime: Long): Seq[Transaction] = {
    transactions.filter {
      tx => (tx.timestamp - blockTime).millis <= SimpleTransactionModule.MaxTimeForUnconfirmed
    }
  }

  def genesisTransactionOnlyInGenesisBlockValidation(stateHeight: Int)(tx: Transaction): Boolean = tx match {
    case gtx: GenesisTransaction => stateHeight == 0
    case _ => true
  }

  def transactionAlreadyActivatedValidation(settings: ChainParameters)(tx: Transaction): Boolean = tx match {
    case tx: PaymentTransaction => true
    case gtx: GenesisTransaction => true
    case tx: TransferTransaction => true
    case tx: IssueTransaction => true
    case tx: ReissueTransaction => true
    case tx: BurnTransaction => tx.timestamp > settings.allowBurnTransactionAfterTimestamp
    case tx: ExchangeTransaction => true
    case _ => false
  }

  def isTValid(storage: TheStorage, settings: ChainParameters)(transaction: Transaction): Boolean = {

    val validators: Seq[(Transaction) => Boolean] = Seq(
      assetIssueReissueBurnValidation(storage),
      previousPaymentTransactionValidation(storage, settings),
      genesisTransactionOnlyInGenesisBlockValidation(storage.stateHeight),
      isExchangeTransactionSanctioned(storage),
      uniquePaymentTransactionIdValidation(storage, settings),
      transactionAlreadyActivatedValidation(settings)
    )

    validators.forall(_.apply(transaction))
  }

  private def getIssueTransaction(storage: StateStorageI)(assetId: AssetId): Option[IssueTransaction] =
    storage.getTransactionBytes(assetId).flatMap(b => IssueTransaction.parseBytes(b).toOption)


  //for debugging purposes only
  def totalBalance(storage: StateStorageI): Long = storage.lastStatesKeys.map(address => currentBalanceByKey(storage)(address)).sum

  //for debugging purposes only
  def toJson(storage: StateStorageI)(heightOpt: Option[Int]): JsObject = {
    val ls = storage.lastStatesKeys.map(add => add -> (heightOpt match {
      case Some(h) => balanceByKeyAtHeight(storage)(add, h);
      case None => currentBalanceByKey(storage)(add)
    }))
      .filter(b => b._2 != 0).sortBy(_._1)
    JsObject(ls.map(a => a._1 -> JsNumber(a._2)).toMap)
  }

  //for debugging purposes only
  def toWavesJson(storage: StateStorageI)(heightOpt: Int): JsObject = {
    val ls = storage.lastStatesKeys.map(add => add -> balanceAtHeight(storage)(add, heightOpt))
      .filter(b => b._1.length == 35 && b._2 != 0).sortBy(_._1).map(b => b._1 -> JsNumber(b._2))
    JsObject(ls)
  }

  //for debugging purposes only
  private def balanceAtHeight(storage: StateStorageI)(key: String, atHeight: Int): Long = {
    storage.getLastStates(key) match {
      case Some(h) if h > 0 =>

        def loop(hh: Int): Long = {
          val row = storage.getAccountChanges(key, hh).get
          if (hh <= atHeight) row.state.balance
          else if (row.lastRowHeight == 0) 0L
          else loop(row.lastRowHeight)
        }

        loop(h)
      case _ =>
        0L
    }
  }

  def assetDistribution(storage: StateStorageI)(assetId: Array[Byte]): Map[String, Long] = {
    val encodedAssetId = Base58.encode(assetId)
    storage.accountAssets()
      .filter(e => e._2.contains(encodedAssetId))
      .map(e => {
        val assetAcc: AssetAcc = AssetAcc(new Account(e._1), Some(assetId))
        val key = assetAcc.key
        val balance = storage.getAccountChanges(key, storage.getLastStates(key).get).get.state.balance
        (e._1, balance)
      })
  }

  def hash(storage: StateStorageI): Int = {
    (BigInt(FastCryptographicHash(toJson(storage)(None).toString().getBytes)) % Int.MaxValue).toInt
  }

  def applyAssetTranscations(storage: AssetsStateStorageI)(tx: Transaction, blockTs: Long, height: Int): Unit = tx match {
    case tx: AssetIssuance =>
      addAsset(storage)(tx.assetId, height, tx.id, tx.quantity, tx.reissuable)
    case tx: BurnTransaction =>
      burnAsset(storage)(tx.assetId, height, tx.id, -tx.amount)
    case _ =>
  }

  def assetIssueReissueBurnValidation(storage: StateStorageI with AssetsStateStorageI)(tx: Transaction): Boolean = tx match {
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
          //  log.debug(s"Can't deserialise issue tx", f)
          false
      })
  }

  private[blockchain] def addAsset(storage: AssetsStateStorageI)(assetId: AssetId, height: Int, transactionId: Array[Byte], quantity: Long, reissuable: Boolean): Unit = {
    val asset = Base58.encode(assetId)
    val transaction = Base58.encode(transactionId)
    val assetAtHeight = s"$asset@$height"
    val assetAtTransaction = s"$asset@$transaction"

    storage.addHeight(asset, height)
    storage.addTransaction(assetAtHeight, transaction)
    storage.setQuantity(assetAtTransaction, quantity)
    storage.setReissuable(assetAtTransaction, reissuable)
  }

  def burnAsset(storage: AssetsStateStorageI)(assetId: AssetId, height: Int, transactionId: Array[Byte], quantity: Long): Unit = {
    require(quantity <= 0, "Quantity of burned asset should be negative")

    val asset = Base58.encode(assetId)
    val transaction = Base58.encode(transactionId)
    val assetAtHeight = s"$asset@$height"
    val assetAtTransaction = s"$asset@$transaction"

    storage.addHeight(asset, height)
    storage.addTransaction(assetAtHeight, transaction)
    storage.setQuantity(assetAtTransaction, quantity)
  }

  def rollbackAssets(storage: AssetsStateStorageI)(assetId: AssetId, height: Int): Unit = {
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

  def getAssetQuantity(storage: AssetsStateStorageI)(assetId: AssetId): Long = {
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

  def isReissuable(storage: AssetsStateStorageI)(assetId: AssetId): Boolean = {
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

  def previousPaymentTransactionValidation(storage: StateStorageI, settings: ChainParameters)(transaction: Transaction): Boolean = transaction match {
    case tx: PaymentTransaction =>
      tx.timestamp < settings.allowInvalidPaymentTransactionsByTimestamp || isPaymentTransactionAfterPreviousTimestamp(storage)(tx)
    case _ => true
  }

  private def isPaymentTransactionAfterPreviousTimestamp(storage: StateStorageI)(tx: PaymentTransaction): Boolean = {
    lastAccountPaymentTransaction(storage)(tx.sender) match {
      case Some(lastTransaction) => lastTransaction.timestamp < tx.timestamp
      case None => true
    }
  }

  def invalidatePaymentTransactionsByTimestamp(storage: StateStorageI)(transactions: Seq[Transaction]): Seq[Transaction] = {
    val paymentTransactions = transactions.filter(_.isInstanceOf[PaymentTransaction])
      .map(_.asInstanceOf[PaymentTransaction])

    val initialSelection: Map[String, (List[Transaction], Long)] = Map(paymentTransactions.map { payment =>
      val address = payment.sender.address
      val stateTimestamp = lastAccountPaymentTransaction(storage)(payment.sender) match {
        case Some(lastTransaction) => lastTransaction.timestamp
        case _ => 0
      }
      address -> (List[Transaction](), stateTimestamp)
    }: _*)

    val orderedTransaction = paymentTransactions.sortBy(_.timestamp)
    val selection: Map[String, (List[Transaction], Long)] = orderedTransaction.foldLeft(initialSelection) { (s, t) =>
      val address = t.sender.address
      val tuple = s(address)
      if (t.timestamp > tuple._2) {
        s.updated(address, (tuple._1, t.timestamp))
      } else {
        s.updated(address, (tuple._1 :+ t, tuple._2))
      }
    }

    selection.foldLeft(List[Transaction]()) { (l, s) => l ++ s._2._1 }
  }

  def lastAccountPaymentTransaction(storage: StateStorageI)(account: Account): Option[PaymentTransaction] = {
    def loop(h: Int, address: Address): Option[PaymentTransaction] = {
      storage.getAccountChanges(address, h) match {
        case Some(row) =>
          val accountTransactions = row.reason.flatMap(id => storage.getTransaction(id))
            .filter(_.isInstanceOf[PaymentTransaction])
            .map(_.asInstanceOf[PaymentTransaction])
            .filter(_.sender.address == address)
          if (accountTransactions.nonEmpty) Some(accountTransactions.maxBy(_.timestamp))
          else loop(row.lastRowHeight, address)
        case _ => None
      }
    }

    storage.getLastStates(account.address) match {
      case Some(height) => loop(height, account.address)
      case None => None
    }
  }

  def applyExchangeTransaction(storage: OrderMatchStorageI)(tx: Transaction, blockTs: Long, height: Int): Unit = tx match {
    case om: ExchangeTransaction =>
      def isSaveNeeded(order: Order): Boolean = {
        order.expiration >= blockTs
      }

      def putOrder(order: Order) = {
        if (isSaveNeeded(order)) {
          val orderDay = calcStartDay(order.expiration)
          storage.putSavedDays(orderDay)
          val orderIdStr = Base58.encode(order.id)
          val omIdStr = Base58.encode(om.id)
          val prev = storage.getOrderMatchTxByDay(orderDay, orderIdStr).getOrElse(Array.empty[String])
          if (!prev.contains(omIdStr)) storage.putOrderMatchTxByDay(orderDay, orderIdStr, prev :+ omIdStr)
        }
      }

      def removeObsoleteDays(timestamp: Long): Unit = {
        val ts = calcStartDay(timestamp)
        val daysToRemove: List[Long] = storage.savedDaysKeys.filter(t => t < ts)
        if (daysToRemove.nonEmpty) {
          synchronized {
            storage.removeOrderMatchDays(daysToRemove)
          }
        }
      }

      putOrder(om.buyOrder)
      putOrder(om.sellOrder)
      removeObsoleteDays(blockTs)
    case _ =>
  }

  def isExchangeTransactionSanctioned(storage: StateStorageI with OrderMatchStorageI)(tx: Transaction): Boolean = tx match {
    case om: ExchangeTransaction => isOrderMatchValid(om, findPrevOrderMatchTxs(storage)(om))
    case _ => true
  }

  def findPrevOrderMatchTxs(storage: StateStorageI with OrderMatchStorageI)(om: ExchangeTransaction): Set[ExchangeTransaction] =
    findPrevOrderExchangeTxs(storage)(om.buyOrder) ++ findPrevOrderExchangeTxs(storage)(om.sellOrder)

  val emptyTxIdSeq = Array.empty[String]

  def parseTxSeq(storage: StateStorageI)(a: Array[String]): Set[ExchangeTransaction] = {
    a.toSet.flatMap { s: String => Base58.decode(s).toOption }.flatMap { id =>
      storage.getTransactionBytes(id).flatMap(b => ExchangeTransaction.parseBytes(b).toOption)
    }
  }

  def findPrevOrderExchangeTxs(storage: StateStorageI with OrderMatchStorageI)(order: Order): Set[ExchangeTransaction] = {
    val orderDay = calcStartDay(order.expiration)
    if (storage.containsSavedDays(orderDay)) {
      parseTxSeq(storage)(storage.getOrderMatchTxByDay(calcStartDay(order.expiration), Base58.encode(order.id))
        .getOrElse(emptyTxIdSeq))
    } else Set.empty[ExchangeTransaction]
  }

  def calcStartDay(t: Long): Long = {
    val ts = t / 1000
    ts - ts % (24 * 60 * 60)
  }

  def isOrderMatchValid(exTrans: ExchangeTransaction, previousMatches: Set[ExchangeTransaction]): Boolean = {

    lazy val buyTransactions = previousMatches.filter { om =>
      om.buyOrder.id sameElements exTrans.buyOrder.id
    }
    lazy val sellTransactions = previousMatches.filter { om =>
      om.sellOrder.id sameElements exTrans.sellOrder.id
    }

    lazy val buyTotal = buyTransactions.foldLeft(0L)(_ + _.amount) + exTrans.amount
    lazy val sellTotal = sellTransactions.foldLeft(0L)(_ + _.amount) + exTrans.amount

    lazy val buyFeeTotal = buyTransactions.map(_.buyMatcherFee).sum + exTrans.buyMatcherFee
    lazy val sellFeeTotal = sellTransactions.map(_.sellMatcherFee).sum + exTrans.sellMatcherFee

    lazy val amountIsValid: Boolean = {
      val b = buyTotal <= exTrans.buyOrder.amount
      val s = sellTotal <= exTrans.sellOrder.amount
      b && s
    }

    def isFeeValid(fee: Long, feeTotal: Long, amountTotal: Long, maxfee: Long, maxAmount: Long): Boolean = {
      fee > 0 &&
        feeTotal <= BigInt(maxfee) * BigInt(amountTotal) / BigInt(maxAmount)
    }

    amountIsValid &&
      isFeeValid(exTrans.buyMatcherFee, buyFeeTotal, buyTotal, exTrans.buyOrder.matcherFee, exTrans.buyOrder.amount) &&
      isFeeValid(exTrans.sellMatcherFee, sellFeeTotal, sellTotal, exTrans.sellOrder.matcherFee, exTrans.sellOrder.amount)
  }

  def putTransaction(storage: StateStorageI, settings: ChainParameters)(tx: Transaction, blockTs: Long, height: Int): Unit = {
    storage.putTransaction(tx, height)
  }

  def uniquePaymentTransactionIdValidation(storage: StateStorageI, settings: ChainParameters)(tx: Transaction): Boolean = tx match {
    case tx: PaymentTransaction if tx.timestamp < settings.requirePaymentUniqueId => true
    case tx: Transaction => storage.included(tx.id).isEmpty
  }
}