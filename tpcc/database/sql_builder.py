from .schema import *

# Operator
ALL = "*"
SELECT = "select"
FROM = "from"
WHERE = "where"
AND = " and "
ORDER_BY = "order by"
DESC = "desc"
ASC = "asc"
INSERT = "insert"
VALUES = "values"
UPDATE = "update"
DELETE = "delete"
SET = "set"
eq = "="
bt = ">"
lt = "<"
beq = ">="
leq = "<="


def select(client, table, col=ALL, where=False, order_by=False, asc=False):
    if type(table) != list:
        table = [table]
    # if type(col) != list:
    #     col = [col]
    if where and type(where) != list:
        where = [where]

    param = []
    if where:
        param = [ele[-1] for ele in where]

    table = ",".join(table)

    gen = lambda ele: str(ele[0]) + str(ele[1]) + "%s"
    where = " ".join([WHERE, AND.join([gen(ele) for ele in where])]) if where else ""
    order_by = " ".join([ORDER_BY, order_by, ASC if asc else DESC]) if order_by else ""
    sql = " ".join([SELECT, ",".join(col), FROM, table, where, order_by, ";"])
    # print(sql, param)
    for i in param:
        sql = sql.replace("%s", str(i), 1)
    # print(sql)

    result = client.send_cmd(sql)

    if result.startswith("abort"):
        from .const import SQLState

        return SQLState.ABORT
    # print(result)

    if result.startswith("Error") or result == None or result == "":
        return

    # 使用字符串分割提取数字部分
    ## 1. 确定要获取多少列属性
    real_col_num = 0
    if len(col) == 1 and col[0] == "*":
        real_col_num = num_of_cols[table]
    else:
        real_col_num = len(col)
    ## 2. 跳过第一行的“｜”
    shuxian_idx = result.find("|")
    for i in range(real_col_num):
        shuxian_idx = result.find("|", shuxian_idx + 1)
    ## 3. 初始化结果集
    results_allline = []
    result_oneline = []
    ## 4. 双层循环提取value
    while True:
        for i in range(real_col_num):
            start_index = result.find("|", shuxian_idx + 1) + 1
            end_index = result.find("|", start_index)
            if end_index == -1:
                # print(results_allline)
                return results_allline
            extracted_value = result[start_index:end_index].strip()
            result_oneline.append(extracted_value)
            shuxian_idx = start_index
        results_allline.append(result_oneline)
        result_oneline = []
        start_index = result.find("|", shuxian_idx + 1) + 1
        end_index = result.find("|", start_index)
        if end_index == -1:
            # print(results_allline)
            return results_allline
        shuxian_idx = start_index

    # should not get here
    # throw exception


def insert(client, table, rows):
    values = "".join(
        [VALUES, "(", ",".join(["%s" for i in range(num_of_cols[table])]), ")"]
    )
    sql = " ".join([INSERT, "into", table, values, ";"])
    # print(sql)
    # if type(rows[0]) != list:
    #     rows = [rows]
    for i in rows:
        sql = sql.replace("%s", str(i), 1)
    # print(sql)
    if client.send_cmd(sql).startswith("abort"):
        from .const import SQLState

        return SQLState.ABORT


def update(client, table, row, where=False):
    if type(row) != list:
        row = [row]
    if type(where) != list:
        where = [where]
    param = [ele[-1] for ele in where]
    gen = lambda ele: str(ele[0]) + str(ele[1]) + "%s"
    where = " ".join([WHERE, AND.join([gen(ele) for ele in where])]) if where else ""
    var = [e[0] + "=%s" for e in row]
    val = [e[1] for e in row]

    sql = " ".join([UPDATE, table, SET, ",".join(var), where, ";"])
    # print(sql,val+param)
    for i in val:
        sql = sql.replace("%s", str(i), 1)
    for i in param:
        sql = sql.replace("%s", str(i), 1)
    # print(sql)
    if client.send_cmd(sql).startswith("abort"):
        from .const import SQLState

        return SQLState.ABORT
    # print(result)


def delete(client, table, where):
    if type(where) != list:
        where = [where]
    param = [ele[-1] for ele in where]
    gen = lambda ele: str(ele[0]) + str(ele[1]) + "%s"
    where = " ".join([WHERE, AND.join([gen(ele) for ele in where])]) if where else ""
    sql = " ".join([DELETE, FROM, table, where, ";"])
    # print(sql)
    for i in param:
        sql = sql.replace("%s", str(i), 1)
    # print(sql)
    if client.send_cmd(sql).startswith("abort"):
        from .const import SQLState

        return SQLState.ABORT
