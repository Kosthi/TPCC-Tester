create table warehouse (w_id int, w_name char(10), w_street_1 char(20), w_street_2 char(20), w_city char(20), w_state char(2), w_zip char(9), w_tax float, w_ytd float);
create table district (d_id int, d_w_id int, d_name char(10), d_street_1 char(20), d_street_2 char(20), d_city char(20), d_state char(2), d_zip char(9), d_tax float, d_ytd float, d_next_o_id int);
create table customer (c_id int, c_d_id int, c_w_id int, c_first char(16), c_middle char(2), c_last char(16), c_street_1 char(20), c_street_2 char(20), c_city char(20), c_state char(2), c_zip char(9), c_phone char(16), c_since char(30), c_credit char(2), c_credit_lim int, c_discount float, c_balance float, c_ytd_payment float, c_payment_cnt int, c_delivery_cnt int, c_data char(50));
create table history (h_c_id int, h_c_d_id int, h_c_w_id int, h_d_id int, h_w_id int, h_date datetime, h_amount float, h_data char(24));
create table new_orders (no_o_id int, no_d_id int, no_w_id int);
create table orders (o_id int, o_d_id int, o_w_id int, o_c_id int, o_entry_d datetime, o_carrier_id int, o_ol_cnt int, o_all_local int);
create table order_line ( ol_o_id int, ol_d_id int, ol_w_id int, ol_number int, ol_i_id int, ol_supply_w_id int, ol_delivery_d char(30), ol_quantity int, ol_amount float, ol_dist_info char(24));
create table item (i_id int, i_im_id int, i_name char(24), i_price float, i_data char(50));
create table stock (s_i_id int, s_w_id int, s_quantity int, s_dist_01 char(24), s_dist_02 char(24), s_dist_03 char(24), s_dist_04 char(24), s_dist_05 char(24), s_dist_06 char(24), s_dist_07 char(24), s_dist_08 char(24), s_dist_09 char(24), s_dist_10 char(24), s_ytd float, s_order_cnt int, s_remote_cnt int, s_data char(50));
create index warehouse(w_id);
create index district(d_w_id, d_id);
create index customer(c_w_id, c_d_id, c_id);
create index new_orders(no_w_id, no_d_id, no_o_id);
create index orders(o_w_id, o_d_id, o_id);
create index order_line(ol_w_id, ol_d_id, ol_o_id, ol_number);
create index item(i_id);
create index stock(s_w_id, s_i_id);
load ../../../src/test/performance_test/table_data/warehouse.csv into warehouse;
load ../../../src/test/performance_test/table_data/district.csv into district;
load ../../../src/test/performance_test/table_data/customer.csv into customer;
load ../../../src/test/performance_test/table_data/history.csv into history;
load ../../../src/test/performance_test/table_data/new_orders.csv into new_orders;
load ../../../src/test/performance_test/table_data/orders.csv into orders;
load ../../../src/test/performance_test/table_data/order_line.csv into order_line;
load ../../../src/test/performance_test/table_data/item.csv into item;
load ../../../src/test/performance_test/table_data/stock.csv into stock;
select COUNT(*) as count_warehouse from warehouse;
select COUNT(*) as count_district from district;
select COUNT(*) as count_customer from customer;
select COUNT(*) as count_history from history;
select COUNT(*) as count_new_orders from new_orders;
select COUNT(*) as count_orders from orders;
select COUNT(*) as count_order_line from order_line;
select COUNT(*) as count_item from item;
select COUNT(*) as count_stock from stock;
