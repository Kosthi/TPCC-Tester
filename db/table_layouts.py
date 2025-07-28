# WAREHOUSE Table Layout
WAREHOUSE = "warehouse"
W_ID = "w_id"
W_NAME = "w_name"
W_STREET_1 = "w_street_1"
W_STREET_2 = "w_street_2"
W_CITY = "w_city"
W_STATE = "w_state"
W_ZIP = "w_zip"
W_TAX = "w_tax"
W_YTD = "w_ytd"

# STOCK Table Layout
STOCK = "stock"
S_I_ID = "s_i_id"
S_W_ID = "s_w_id"
S_QUANTITY = "s_quantity"
S_DIST_01 = "s_dist_01"
S_DIST_02 = "s_dist_02"
S_DIST_03 = "s_dist_03"
S_DIST_04 = "s_dist_04"
S_DIST_05 = "s_dist_05"
S_DIST_06 = "s_dist_06"
S_DIST_07 = "s_dist_07"
S_DIST_08 = "s_dist_08"
S_DIST_09 = "s_dist_09"
S_DIST_10 = "s_dist_10"
S_YTD = "s_ytd"
S_ORDER_CNT = "s_order_cnt"
S_REMOTE_CNT = "s_remote_cnt"
S_DATA = "s_data"

# DISTRICT Table Layout
DISTRICT = "district"
D_ID = "d_id"
D_W_ID = "d_w_id"
D_NAME = "d_name"
D_STREET_1 = "d_street_1"
D_STREET_2 = "d_street_2"
D_CITY = "d_city"
D_STATE = "d_state"
D_ZIP = "d_zip"
D_TAX = "d_tax"
D_YTD = "d_ytd"
D_NEXT_O_ID = "d_next_o_id"

# CUSTOMER Table Layout
CUSTOMER = "customer"
C_ID = "c_id"
C_D_ID = "c_d_id"
C_W_ID = "c_w_id"
C_LAST = "c_last"
C_MIDDLE = "c_middle"
C_FIRST = "c_first"
C_STREET_1 = "c_street_1"
C_STREET_2 = "c_street_2"
C_CITY = "c_city"
C_STATE = "c_state"
C_ZIP = "c_zip"
C_PHONE = "c_phone"
C_SINCE = "c_since"
C_CREDIT = "c_credit"
C_CREDIT_LIM = "c_credit_lim"
C_DISCOUNT = "c_discount"
C_BALANCE = "c_balance"
C_YTD_PAYMENT = "c_ytd_payment"
C_PAYMENT_CNT = "c_payment_cnt"
C_DELIVERY_CNT = "c_delivery_cnt"
C_DATA = "c_data"

# HISTORY Table Layout
HISTORY = "history"
H_C_ID = "h_c_id"
H_C_D_ID = "h_c_d_id"
H_C_W_ID = "h_c_w_id"
H_D_ID = "h_d_id"
H_W_ID = "h_w_id"
H_DATE = "h_date"
H_AMOUNT = "h_amount"
H_DATA = "h_data"

# ORDERS Table Layout
ORDERS = "orders"
O_ID = "o_id"
O_C_ID = "o_c_id"
O_D_ID = "o_d_id"
O_W_ID = "o_w_id"
O_ENTRY_D = "o_entry_d"
O_CARRIER_ID = "o_carrier_id"
O_OL_CNT = "o_ol_cnt"
O_ALL_LOCAL = "o_all_local"

# ORDER_LINE Tabel Layout
ORDER_LINE = "order_line"
OL_O_ID = "ol_o_id"
OL_D_ID = "ol_d_id"
OL_W_ID = "ol_w_id"
OL_NUMBER = "ol_number"
OL_I_ID = "ol_i_id"
OL_SUPPLY_W_ID = "ol_supply_w_id"
OL_DELIVERY_D = "ol_delivery_d"
OL_QUANTITY = "ol_quantity"
OL_AMOUNT = "ol_amount"
OL_DIST_INFO = "ol_dist_info"

# NEW_ORDER Table Layout
NEW_ORDERS = "new_orders"
NO_O_ID = "no_o_id"
NO_D_ID = "no_d_id"
NO_W_ID = "no_w_id"

# ITEM Table Layout
ITEM = "item"
I_ID = "i_id"
I_IM_ID = "i_im_id"
I_NAME = "i_name"
I_PRICE = "i_price"
I_DATA = "i_data"


def COUNT(x="*", alias=None):
    """
    Constructs a SQL MIN function with an optional alias.
    """
    query = "count(" + x + ")"
    if alias:
        query += " AS " + alias
    return query


def MIN(x="*", alias=None):
    """
    Constructs a SQL MIN function with an optional alias.
    """
    query = "min(" + x + ")"
    if alias:
        query += " AS " + alias
    return query


def MAX(x="*", alias=None):
    """
    Constructs a SQL MAX function with an optional alias.
    """
    query = "max(" + x + ")"
    if alias:
        query += " AS " + alias
    return query


def SUM(x="*", alias=None):
    """
    Constructs a SQL SUM function with an optional alias.
    """
    query = "sum(" + x + ")"
    if alias:
        query += " AS " + alias
    return query


num_of_cols = {
    WAREHOUSE: 9,
    STOCK: 17,
    DISTRICT: 11,
    CUSTOMER: 21,
    HISTORY: 8,
    ORDERS: 8,
    ORDER_LINE: 10,
    NEW_ORDERS: 3,
    ITEM: 5,
}
population = {ITEM: 100000, STOCK: 100000, CUSTOMER: 3000, DISTRICT: 10}
