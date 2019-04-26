/**
 * lock_manager_test.cpp
 */

#include <thread>
#include <chrono>

#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace cmudb {


/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */
TEST(LockManagerTest, BasicTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::thread t0([&] {
    Transaction txn(0);
    bool res = lock_mgr.LockShared(&txn, rid);
    LOG_DEBUG("transaction 0 locked rid");
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t1([&] {
    Transaction txn(1);
    bool res = lock_mgr.LockShared(&txn, rid);
    if (res) {
        LOG_DEBUG("transaction 1 locked rid");
    } else {
        LOG_DEBUG("transaction 1 abort");
    }
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  t0.join();
  t1.join();
}

// 测试没有死锁预防的情况，在加了wait-die算法逻辑后，t1在t0已经获取rid_a排它锁
// 的情况下，试图获取rid_a的共享锁，将被abort
//TEST(LockManagerTest, SharedAndExclusiveTest) {
//  LockManager lock_mgr{false};
//  TransactionManager txn_mgr{&lock_mgr};
//  RID rid_a{0, 0};
//  int account_a = 100;
//  RID rid_b{0, 1};
//  int account_b = 200;
//  int total = 0;
//
//  // 不可能出现死锁
//  std::thread t0([&](int account_a, int account_b) {
//    Transaction txn(0);
//    bool res = lock_mgr.LockExclusive(&txn, rid_a);
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
//    account_a += 50;
//    res = lock_mgr.LockExclusive(&txn, rid_b);
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
//    account_b -= 50;
//    lock_mgr.Unlock(&txn, rid_a);
//    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
//    lock_mgr.Unlock(&txn, rid_b);
//    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
//  }, account_a, account_b);
//  std::thread t1([&] {
//    Transaction txn(1);
//    bool res = lock_mgr.LockShared(&txn, rid_a);
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
//
//    res = lock_mgr.LockShared(&txn, rid_b);
//    EXPECT_EQ(res, true);
//    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
//
//    total = account_a + account_b;
//
//    lock_mgr.Unlock(&txn, rid_a);
//    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
//    lock_mgr.Unlock(&txn, rid_b);
//    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
//  });
//
//  t0.join();
//  t1.join();
//  EXPECT_EQ(300, total);
//  EXPECT_NE(350, total);
//}

TEST(LockManagerTest, DeadLockTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid_a{0, 0};
  int account_a = 100;
  RID rid_b{0, 1};
  int account_b = 200;

  // 可能出现死锁
  std::thread t0([&](int account_a, int account_b) {
    Transaction txn(0);

    bool res = lock_mgr.LockExclusive(&txn, rid_b);
    LOG_DEBUG("thread 0 lock rid_b");
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    account_b -= 50;

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    res = lock_mgr.LockExclusive(&txn, rid_a);
    LOG_DEBUG("thread 0 lock rid_a");
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    account_a += 50;

    lock_mgr.Unlock(&txn, rid_a);
    LOG_DEBUG("thread 0 unlock rid_a");
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
    lock_mgr.Unlock(&txn, rid_b);
    LOG_DEBUG("thread 0 unlock rid_b");
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  }, account_a, account_b);

  std::thread t1([&] {
    Transaction txn(1);
    bool res = lock_mgr.LockShared(&txn, rid_a);
    LOG_DEBUG("thread 1 lock rid_a");
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    res = lock_mgr.LockShared(&txn, rid_b);
    if (res) {
        LOG_DEBUG("thread 1 lock rid_b");
    } else {
        LOG_DEBUG("thread 1 try lock rid_b, abort");
    }
    EXPECT_EQ(res, false);
    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    txn_mgr.Abort(&txn);
  });

  t0.join();
  t1.join();
}
} // namespace cmudb
