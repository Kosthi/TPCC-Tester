import time

from mysql.driver import SQLState
from record.record import *
from util import *


# 输入一个列表，其中每项代表选择该项目的概率，返回选择的项目的下标
def get_choice(choices):
    r = random.random() * sum(choices)
    upto = 0
    for i in range(len(choices)):
        if upto + choices[i] >= r:
            return i
        upto += choices[i]
    assert False, "Shouldn't get here"


def do_test(driver, lock, txns, txn_prob=None):
    # print(duration)
    # print('Test')
    t1 = 0
    t2 = 0

    w_id = 0
    d_id = 0
    c_id = 0
    ol_i_id = 0
    ol_supply_w_id = 0
    ol_quantity = 0
    o_carrier_id = 0
    c_w_id = 0
    c_d_id = 0
    h_amount = 0
    query_cus = 0
    threshold = 0

    if txn_prob is None:
        txn_prob = [10 / 23, 10 / 23, 1 / 23, 1 / 23, 1 / 23]

    t_start = time.time()

    for i in range(txns):
        txn = get_choice(txn_prob)
        ret = SQLState.ABORT

        # 预生成操作
        if txn == 0:  # NewOrder
            w_id = get_w_id()
            d_id = get_d_id()  # 获得地区id，1～10的随机数
            c_id = get_c_id()  # 获得客户id，1～3000的随机数
            ol_i_id = get_ol_i_id()  # 获得新订单中的商品id列表
            ol_supply_w_id = get_ol_supply_w_id(
                w_id, driver._scale, len(ol_i_id)
            )  # 为新订单中每个商品选择一个供应仓库，当前设定就一个供应仓库
            ol_quantity = get_ol_quantity(
                len(ol_i_id)
            )  # 为新订单中每个商品设置购买数量

        elif txn == 1:  # Payment
            w_id = get_w_id()
            d_id = get_d_id()  # 获得地区id，1～10的随机数
            query_cus = query_cus_by(True)
            h_amount = get_h_amount()
            c_w_id, c_d_id = get_c_w_id_d_id(
                w_id, d_id, driver._scale
            )  # 获得客户所属的仓库id和地区id

        elif txn == 2:  # Delivery
            w_id = get_w_id()
            o_carrier_id = get_o_carrier_id()

        elif txn == 3:  # OrderStatus
            w_id = get_w_id()
            d_id = get_d_id()  # 获得地区id，1～10的随机数
            query_cus = query_cus_by()

        elif txn == 4:  # StockLevel
            w_id = get_w_id()
            d_id = get_d_id()  # 获得地区id，1～10的随机数
            threshold = random.randrange(10, 21)

        while ret == SQLState.ABORT:
            if txn == 0:  # NewOrder
                t1 = time.time()
                ret = driver.do_new_order(
                    w_id, d_id, c_id, ol_i_id, ol_supply_w_id, ol_quantity
                )
                t2 = time.time()
                put_new_order(lock, t2 - t_start)

            elif txn == 1:  # Payment
                t1 = time.time()
                ret = driver.do_payment(w_id, d_id, c_w_id, c_d_id, query_cus, h_amount)
                t2 = time.time()

            elif txn == 2:  # Delivery
                t1 = time.time()
                ret = driver.do_delivery(w_id, o_carrier_id)
                t2 = time.time()

            elif txn == 3:  # OrderStatus
                t1 = time.time()
                ret = driver.do_order_status(w_id, d_id, query_cus)
                t2 = time.time()

            elif txn == 4:  # StockLevel
                t1 = time.time()
                ret = driver.do_stock_level(w_id, d_id, threshold)
                t2 = time.time()

            # if ret != SQLState.ABORT:
            #     put_txn(lock, txn, t2 - t1, True)

            if ret == SQLState.ABORT:
                put_txn(lock, txn, t2 - t1, False)
            else:
                put_txn(lock, txn, t2 - t1, True)

    # for i in range(txns):
    #     txn = get_choice(txn_prob)
    #     ret = SQLState.ABORT
    #     while ret == SQLState.ABORT:
    #         if txn == 0:  # NewOrder
    #             w_id = get_w_id()
    #             d_id = get_d_id()  # 获得地区id，1～10的随机数
    #             c_id = get_c_id()  # 获得客户id，1～3000的随机数
    #             ol_i_id = get_ol_i_id()  # 获得新订单中的商品id列表
    #             ol_supply_w_id = get_ol_supply_w_id(w_id, driver._scale, len(ol_i_id))  # 为新订单中每个商品选择一个供应仓库，当前设定就一个供应仓库
    #             ol_quantity = get_ol_quantity(len(ol_i_id))  # 为新订单中每个商品设置购买数量
    #
    #             t1 = time.time()
    #             ret = driver.do_new_order(w_id, d_id, c_id, ol_i_id, ol_supply_w_id, ol_quantity)
    #             t2 = time.time()
    #
    #             put_new_order(lock, t2 - t_start)
    #
    #         elif txn == 1:  # Payment
    #             w_id = get_w_id()
    #             d_id = get_d_id()  # 获得地区id，1～10的随机数
    #             c_w_id, c_d_id = get_c_w_id_d_id(w_id, d_id, driver._scale)  # 获得客户所属的仓库id和地区id
    #
    #             t1 = time.time()
    #             ret = driver.do_payment(w_id, d_id, c_w_id, c_d_id, query_cus_by(), random.random() * (5000 - 1) + 1)
    #             t2 = time.time()
    #
    #         elif txn == 2:  # Delivery
    #             w_id = get_w_id()
    #             t1 = time.time()
    #             ret = driver.do_delivery(w_id, get_o_carrier_id())
    #             t2 = time.time()
    #
    #         elif txn == 3:  # OrderStatus
    #             w_id = get_w_id()
    #             t1 = time.time()
    #             ret = driver.do_order_status(w_id, get_d_id(), query_cus_by())
    #             t2 = time.time()
    #
    #         elif txn == 4:  # StockLevel
    #             w_id = get_w_id()
    #             t1 = time.time()
    #             ret = driver.do_stock_level(w_id, get_d_id(), random.randrange(10, 21))
    #             t2 = time.time()
    #
    #         if ret == SQLState.ABORT:
    #             put_txn(lock, txn, t2 - t1, False)
    #         else:
    #             put_txn(lock, txn, t2 - t1, True)
