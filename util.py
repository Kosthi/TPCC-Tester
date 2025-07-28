import datetime
import random
import string

# _names = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE', 'ANTI', 'CALLY', 'ATION', 'EING']
_names = [
    "BARR",
    "OUGH",
    "ABLE",
    "PRII",
    "PRES",
    "ESEE",
    "ANTI",
    "CALL",
    "ATIO",
    "EING",
]
_C_LOAD = 117
_C_RUN = 191


def rand_str(lower, upper=0):
    if upper == 0:
        upper = lower + 1
    return "".join(
        [
            random.choice(string.ascii_letters)
            for i in range(random.randrange(lower, upper))
        ]
    )


def rand_dat(lower, upper):
    if random.randrange(100) < 10:
        s = rand_str(lower, upper - 8)
        k = random.randrange(lower, upper - 8)
        return s[lower:k] + "ORIGINAL" + s[k : upper - 8]
    else:
        return rand_str(lower, upper)


def rand_digit(num):
    return "".join([random.choice(string.digits) for i in range(num)])


def zip_code():
    rand_digit(4) + "11111"


def rand_perm(max):
    l = list(range(max))
    random.shuffle(l)
    return l


def NURand(A, x, y, C):
    return (
        ((random.randrange(0, A) | random.randrange(x, y)) + C) % (y - x)
    ) + x  # y-1 = y


def get_c_last(k=1000, run=False):
    C = _C_RUN if run else _C_LOAD
    if k >= 1000:
        k = NURand(255, 0, 1000, C)
    return "".join([_names[k // 100], _names[(k // 10) % 10], _names[k % 10]])


def current_time():
    return str(datetime.datetime.now())[:19]


def get_c_id():
    return NURand(1023, 1, 3001, C=_C_RUN)


def get_ol_i_id():
    ol_cnt = random.randrange(5, 16)
    rbk = random.randrange(100)
    ret = [NURand(8191, 1, 100001, C=_C_RUN) for i in range(ol_cnt)]
    # if rbk == 0:
    #     ret[-1] = 100001  # unused item number
    return ret


# 函数 get_ol_supply_w_id 的作用是生成一个订单列表中每个项目的供应仓库 ID 列表：
# 在几乎所有情况下，每个供应仓库 ID 都是 home_w_id。
# 在非常少见的情况下（订单行的仓库数量大于 1，且随机数恰好为 0），随机选择一个非 home_w_id 的仓库 ID。
# 这个函数主要用于某种模拟或测试场景，确保大部分订单项目都来自一个主要仓库 home_w_id，但偶尔订单项目可能来自其他仓库，比如当前仓库缺货了
def get_ol_supply_w_id(home_w_id, scale, ol_cnt):
    def supply_id():
        # 50% 概率使用 home_w_id 或如果 scale 为 1，则一定使用 home_w_id
        if random.randrange(100) > 0 or scale == 1:
            return home_w_id
        else:
            # 选择一个除 home_w_id 以外的仓库 ID
            other_ids = [i for i in range(1, scale + 1) if i != home_w_id]
            return random.choice(other_ids)

    return [supply_id() for _ in range(ol_cnt)]


# def get_ol_supply_w_id(home_w_id, scale, ol_cnt):
#     supply_id = lambda: home_w_id if random.randrange(100) > 0 or scale == 1 else random.choice(
#         list(range(scale)).remove(home_w_id))
#     return [supply_id() for i in range(ol_cnt)]


def get_ol_quantity(ol_cnt):
    return [random.randrange(1, 11) for i in range(ol_cnt)]


def get_w_id():
    return random.randrange(1, 51)


def get_d_id():
    return random.randrange(1, 11)


# 这个函数的目的是在某种交易模拟场景中，决定客户所属的 warehouse 和 district：
# 85% 的情况下，客户保持在原来的 warehouse 和 district。
# 15% 的情况下，客户尝试从另一个 warehouse 随机选择一个 district。
# 如果整个系统中只有一个 warehouse，客户始终集中在这个唯一的 warehouse
def get_c_w_id_d_id(home_w_id, d_id, scale):
    if random.randrange(100) < 85 or scale == 1:
        # 85% 概率返回 home_w_id 和给定的 d_id，或者 if scale == 1 就一定选择 home_w_id
        c_w_id = home_w_id
        c_d_id = d_id
    else:
        # 从除 home_w_id 外随机选择一个仓库 ID 和 1 到 10 之间随机选择一个区域 ID
        other_ids = [i for i in range(1, scale + 1) if i != home_w_id]
        c_w_id = random.choice(other_ids)
        c_d_id = random.randrange(1, 11)

    return c_w_id, c_d_id


# def get_c_w_id_d_id(home_w_id, d_id, scale):
#     c_w_id, c_d_id = (home_w_id, d_id) \
#         if random.randrange(100) < 85 or scale == 1 \
#         else (random.choice(list(range(1, scale + 1)).remove(home_w_id)), random.randrange(1, 11))
#     return c_w_id, c_d_id


def query_cus_by(fetch_id=False):
    """
    根据 fetch_id 参数决定获取客户 ID 还是按随机概率获取客户 last name 或 ID。

    Args:
        fetch_id (bool): 决定是否直接获取客户 ID。

    Returns:
        str: 客户 last name 或客户 ID。
    """
    if fetch_id:
        return get_c_id()

    y = random.randrange(100)
    if y < 60:
        return get_c_last(1000, run=True)
    else:
        return get_c_id()


# def query_cus_by():
#     y = random.randrange(100)
#     if y < 60:
#         return get_c_last(1000, run=True)
#     else:
#         return get_c_id()


def get_h_amount():
    return round(random.random() * (5000 - 1) + 1, 2)


def get_o_carrier_id():
    return random.randrange(1, 11)
