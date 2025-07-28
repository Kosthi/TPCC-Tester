import argparse
import os
import shutil
import time
from multiprocessing import Process, Lock

import matplotlib.pyplot as plt
import numpy as np

from mysql.driver import CNT_W, Driver
from record.record import analysis
from record.record import build_db
from tester import do_test


def clean():
    shutil.rmtree("TPCC-Tester/result")
    os.mkdir("TPCC-Tester/result")
    build_db()


def prepare():
    driver = Driver(scale=1)
    driver.all_in_load()  # loading 阶段
    driver.count_star()
    driver.consistency_check()  # 一致性校验
    # driver.build()  # 创建9个tables
    # driver.create_index() # 建立除history表外其余表的索引
    # driver.load()  # 加载csv数据到9张表
    driver.delay_close()


def test(lock, tid, txns=150, txn_prob=None):
    print(f"+ Test_{tid} Begin")
    driver = Driver(scale=CNT_W)
    do_test(driver, lock, txns, txn_prob)
    print(f"- Test_{tid} Finished")
    driver.delay_close()


def output_result():
    result, new_order_result = analysis()

    total_transactions = 0
    total_rollbacks = 0
    statistics_lines = []

    # 计算每个事务的回滚率和总回滚率
    for r in result:
        failure_count = r["total"] - r["success"]
        rollback_rate = (failure_count / r["total"]) * 100 if r["total"] > 0 else 0

        statistics_lines.append(
            f"{r['name']} - \navg time: {r['avg']}\ntotal: {r['total']}\nsuccess: {r['success']}\nRollback rate: {rollback_rate:.2f}%\n\n"
        )

        print(
            f"{r['name']} - \navg time: {r['avg']}\ntotal: {r['total']}\nsuccess: {r['success']}\nRollback rate: {rollback_rate:.2f}%"
        )

        total_transactions += r["total"]
        total_rollbacks += failure_count

    total_rollback_rate = (
        (total_rollbacks / total_transactions) * 100 if total_transactions > 0 else 0
    )
    print(f"Total Rollback Rate: {total_rollback_rate:.2f}%")

    # 写入 statistics_of_five_transactions.txt
    with open("TPCC-Tester/result/statistics_of_five_transactions.txt", "w") as f:
        f.writelines(statistics_lines)

    # 处理 new order 结果，写入 timecost_and_num_of_NewOrders.txt
    new_order_lines = [f"number: {n[0]}, time cost: {n[1]}\n" for n in new_order_result]
    with open("TPCC-Tester/result/timecost_and_num_of_NewOrders.txt", "w") as f2:
        f2.writelines(new_order_lines)

    # 画图并保存图像
    times = np.array([e[1] for e in new_order_result])
    numbers = np.array([e[0] for e in new_order_result])

    plt.plot(times, numbers)
    plt.ylabel("Number of New-Orders")
    plt.xlabel("Time unit: second")
    plt.savefig("TPCC-Tester/result/timecost_and_num_of_NewOrders.jpg")
    plt.show()

    # 删除数据库文件
    if os.path.exists("TPCC-Tester/result/rds.db"):
        os.remove("TPCC-Tester/result/rds.db")

    # 返回 new order 成功数量
    return result[0]["success"]


# useage: python TPCC-Tester/main.py --prepare --thread 8 --rw 150 --ro 150 --analyze
def main():
    parser = argparse.ArgumentParser(
        description="Python Script with Thread Number Argument"
    )
    parser.add_argument("--prepare", action="store_true", help="Enable prepare mode")
    parser.add_argument("--analyze", action="store_true", help="Enable analyze mode")
    parser.add_argument("--rw", type=int, help="Read write transaction phase time")
    parser.add_argument("--ro", type=int, help="Read only transaction phase time")
    parser.add_argument("--thread", type=int, help="Thread number")

    args = parser.parse_args()
    thread_num = args.thread

    clean()

    if args.prepare:
        lt1 = time.time()
        prepare()
        print(f"load time: {time.time() - lt1}")

    t1 = 0
    t2 = 0
    t3 = 0
    if thread_num:
        lock = Lock()
        t1 = time.time()
        process_list = []
        if args.rw:
            for i in range(thread_num):
                process_list.append(
                    Process(
                        target=test,
                        args=(
                            lock,
                            i + 1,
                            args.rw,
                            [10 / 23, 10 / 23, 1 / 23, 1 / 23, 1 / 23],
                        ),
                    )
                )
                process_list[i].start()

            for i in range(thread_num):
                process_list[i].join()
        t2 = time.time()
        process_list = []
        if args.ro:
            for i in range(thread_num):
                process_list.append(
                    Process(
                        target=test, args=(lock, i + 1, args.ro, [0, 0, 0, 0.5, 0.5])
                    )
                )
                process_list[i].start()

            for i in range(thread_num):
                process_list[i].join()
        t3 = time.time()

    driver = Driver(scale=CNT_W)
    driver.consistency_check()

    new_order_success = output_result()
    driver.consistency_check2(new_order_success)

    if args.analyze:
        print(f"total time of rw txns: {t2 - t1}")
        print(f"total time of ro txns: {t3 - t2}")
        print(f"total time: {t3 - t1}")
        print(f"tpmC: {new_order_success / ((t3 - t1) / 60)}")


if __name__ == "__main__":
    main()
