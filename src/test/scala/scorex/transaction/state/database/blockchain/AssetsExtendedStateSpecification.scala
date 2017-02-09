package scorex.transaction.state.database.blockchain

import com.google.common.primitives.Longs
import org.h2.mvstore.MVStore
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Assertions, Matchers, PropSpec}
import scorex.transaction.TransactionGen
import scorex.transaction.state.database.state.storage.{MVStoreAssetsExtendedStateStorage, MVStoreStateStorage}
import AssetsExtendedState._

class AssetsExtendedStateSpecification extends PropSpec with PropertyChecks with GeneratorDrivenPropertyChecks
  with Matchers with TransactionGen with Assertions {

  property("Assets quantity and issueability should work on one update") {
    val storage = new MVStoreStateStorage with MVStoreAssetsExtendedStateStorage {
      override val db: MVStore = new MVStore.Builder().open()
    }

    forAll(bytes32gen, bytes32gen, positiveLongGen) { (assetId, transactionId, quantity) =>
      addAsset(storage)(assetId, 1, transactionId, quantity, reissuable = true)
      getAssetQuantity(storage)(assetId) shouldBe quantity
      isReissuable(storage)(assetId) shouldBe true
    }
  }

  property("Assets quantity should work on huge sequential updates") {
    val storage = new MVStoreStateStorage with MVStoreAssetsExtendedStateStorage {
      override val db: MVStore = new MVStore.Builder().open()
    }
    forAll(bytes32gen) { assetId =>
      var i = 0
      var q: Long = 0L
      forAll(bytes32gen, smallFeeGen) { (transactionId, quantity) =>
        i = i + 1
        q = q + quantity
        addAsset(storage)(assetId, i, transactionId, quantity, reissuable = true)
        getAssetQuantity(storage)(assetId) shouldBe q
      }
    }
  }

  property("Reissuable should work in simple case") {
    val storage = new MVStoreStateStorage with MVStoreAssetsExtendedStateStorage {
      override val db: MVStore = new MVStore.Builder().open()
    }
    val assetId = getId(0)

    addAsset(storage)(assetId, 1, getId(1), 10, reissuable = true)
    isReissuable(storage)(assetId) shouldBe true
    addAsset(storage)(assetId, 2, getId(2), 10, reissuable = false)
    isReissuable(storage)(assetId) shouldBe false
  }

  property("Reissuable should work correctly in case of few updates per block") {
    val storage = new MVStoreStateStorage with MVStoreAssetsExtendedStateStorage {
      override val db: MVStore = new MVStore.Builder().open()
    }
    val assetId = getId(0)

    addAsset(storage)(assetId, 1, getId(1), 10, reissuable = true)
    addAsset(storage)(assetId, 1, getId(2), 20, reissuable = true)

    getAssetQuantity(storage)(assetId) shouldBe 30
    isReissuable(storage)(assetId) shouldBe true

    addAsset(storage)(assetId, 3, getId(3), 30, reissuable = true)
    addAsset(storage)(assetId, 3, getId(4), 40, reissuable = false)

    getAssetQuantity(storage)(assetId) shouldBe 100
    isReissuable(storage)(assetId) shouldBe false

    rollbackTo(storage)(assetId, 2)

    getAssetQuantity(storage)(assetId) shouldBe 30
    isReissuable(storage)(assetId) shouldBe true
  }

  property("Rollback should work after simple sequence of updates") {
    val storage = new MVStoreStateStorage with MVStoreAssetsExtendedStateStorage {
      override val db: MVStore = new MVStore.Builder().open()
    }
    val assetId = getId(0)

    addAsset(storage)(assetId, 1, getId(1), 10, reissuable = true)
    addAsset(storage)(assetId, 2, getId(2), 10, reissuable = true)
    addAsset(storage)(assetId, 3, getId(3), 10, reissuable = true)

    getAssetQuantity(storage)(assetId) shouldBe 30
    isReissuable(storage)(assetId) shouldBe true

    addAsset(storage)(assetId, 4, getId(4), 10, reissuable = false)

    getAssetQuantity(storage)(assetId) shouldBe 40
    isReissuable(storage)(assetId) shouldBe false

    rollbackTo(storage)(assetId, 2)

    getAssetQuantity(storage)(assetId) shouldBe 20
    isReissuable(storage)(assetId) shouldBe true
  }

  property("Rollback should work after simple sequence of updates with gaps") {
    val storage = new MVStoreStateStorage with MVStoreAssetsExtendedStateStorage {
      override val db: MVStore = new MVStore.Builder().open()
    }
    val assetId = getId(0)

    addAsset(storage)(assetId, 10, getId(1), 10, reissuable = true)
    addAsset(storage)(assetId, 20, getId(2), 10, reissuable = true)
    addAsset(storage)(assetId, 30, getId(3), 10, reissuable = true)

    getAssetQuantity(storage)(assetId) shouldBe 30
    isReissuable(storage)(assetId) shouldBe true

    addAsset(storage)(assetId, 40, getId(4), 10, reissuable = false)

    getAssetQuantity(storage)(assetId) shouldBe 40
    isReissuable(storage)(assetId) shouldBe false

    rollbackTo(storage)(assetId, 25)

    getAssetQuantity(storage)(assetId) shouldBe 20
    isReissuable(storage)(assetId) shouldBe true
  }

  property("Duplicated calls should work correctly") {
    val storage = new MVStoreStateStorage with MVStoreAssetsExtendedStateStorage {
      override val db: MVStore = new MVStore.Builder().open()
    }
    val assetId = getId(0)

    addAsset(storage)(assetId, 10, getId(1), 10, reissuable = true)
    addAsset(storage)(assetId, 10, getId(1), 10, reissuable = true)
    addAsset(storage)(assetId, 10, getId(1), 10, reissuable = true)

    getAssetQuantity(storage)(assetId) shouldBe 10
    isReissuable(storage)(assetId) shouldBe true

    addAsset(storage)(assetId, 20, getId(2), 20, reissuable = false)
    addAsset(storage)(assetId, 20, getId(2), 20, reissuable = false)
    addAsset(storage)(assetId, 20, getId(2), 20, reissuable = false)

    getAssetQuantity(storage)(assetId) shouldBe 30
    isReissuable(storage)(assetId) shouldBe false

    rollbackTo(storage)(assetId, 18)

    getAssetQuantity(storage)(assetId) shouldBe 10
    isReissuable(storage)(assetId) shouldBe true
  }

  property("Burn should work after simple sequence of updates and rollback") {
    val storage = new MVStoreStateStorage with MVStoreAssetsExtendedStateStorage {
      override val db: MVStore = new MVStore.Builder().open()
    }
    val assetId = getId(0)

    addAsset(storage)(assetId, 10, getId(1), 10, reissuable = true)
    addAsset(storage)(assetId, 10, getId(2), 10, reissuable = true)

    addAsset(storage)(assetId, 20, getId(3), 20, reissuable = true)
    addAsset(storage)(assetId, 20, getId(4), 20, reissuable = true)

    addAsset(storage)(assetId, 30, getId(5), 30, reissuable = true)

    getAssetQuantity(storage)(assetId) shouldBe 90
    isReissuable(storage)(assetId) shouldBe true

    burnAsset(storage)(assetId, 40, getId(6), -50)
    addAsset(storage)(assetId, 40, getId(7), 10, reissuable = false)

    getAssetQuantity(storage)(assetId) shouldBe 50
    isReissuable(storage)(assetId) shouldBe false

    rollbackTo(storage)(assetId, 15)

    getAssetQuantity(storage)(assetId) shouldBe 20
    isReissuable(storage)(assetId) shouldBe true
  }

  private def getId(i: Int): Array[Byte] = {
    Longs.toByteArray(0) ++ Longs.toByteArray(0) ++ Longs.toByteArray(0) ++ Longs.toByteArray(i)
  }

}
