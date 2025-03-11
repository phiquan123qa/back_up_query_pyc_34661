insert
into bccs3_inventory_la.sync_revenue_staff_rp (BRANCH_CODE, BRANCH_NAME, SHOP_CODE_LV3,
                                               SHOP_NAME_LV3, SHOP_ID, SHOP_CODE, SHOP_NAME, STAFF_ID,
                                               STAFF_CODE, STAFF_NAME, ORDER_CODE, SALE_TRANS_TYPE_ID,
                                               SALE_SERVICE_ID, STOCK_MODEL_ID, SALE_TRANS_TYPE_NAME,
                                               STOCK_TYPE_ID, STOCK_TYPE_NAME, STOCK_MODEL_CODE,
                                               STOCK_MODEL_NAME, ACCOUNTING_MODEL_CODE,
                                               ACCOUNTING_MODEL_NAME, DISPLAY_MODEL_CODE,
                                               DISPLAY_MODEL_NAME, QUANTITY, ACCOUNTING_QUANTITY,
                                               PRICE, ACCOUNTING_PRICE, AMOUNT, DISCOUNT_AMOUNT,
                                               AMOUNT_TAX, AMOUNT_NOT_TAX, VAT_AMOUNT, IN_SERVICE,
                                               ACCOUNTING_SALE_SERVICE, SYNC_DATE, PERIOD_REPORT, SHOP_PATH)
select nvl(a.branchcode, a.shop_code)                                                  branch_code,
       nvl(a.branchname, a.shop_name)                                                  branch_name,
       nvl(a.shop_code_lv3, a.shop_code),
       nvl(a.shop_name_lv3, a.shop_name),
       (select shop_id from bccs3_inventory_la.shop where shop_code = a.shop_code)     shop_id,
       a.shop_code,
       a.shop_name,
       (select staff_id from bccs3_inventory_la.staff where staff_code = a.staff_code) staff_id,
       a.staff_code,
       a.staff_name,
       a.order_code,
       a.sale_trans_type,
       a.sale_services_id,
       a.stock_model_id,
       a.sale_trans_type_name,
       a.stock_type_id,
       a.stock_type_name,
       a.stock_model_code,
       a.stock_model_name,
       a.accounting_model_code,
       a.accounting_model_name,
       a.display_model_code,
       a.display_model_name,
       SUM(a.quantity),
       SUM(a.accounting_quantity),
       a.price,
       a.accounting_price,
       SUM(a.amount),
       SUM(a.discount_amount),
       SUM(a.amount) - SUM(a.discount_amount)                                          amount_tax,
       SUM(a.amount) - SUM(a.discount_amount) - SUM(a.vat_amount)                      amount_not_tax,
       SUM(a.vat_amount),
       a.in_services,
       a.accounting_sale_service,
       NOW()                                                                           sync_date,
       str_to_date(:period_report, '%Y%m%d')                                           period_report,
       a.SHOP_PATH                                                                     shop_path
from ((select a.staff_code,
              a.staff_name,
              a.shop_code,
              a.shop_name,
              concat(a.sale_services_id, '_', a.stock_model_id) as                             order_code,
              a.sale_trans_type,
              a.sale_services_id,
              a.stock_model_id,
              a.sale_trans_type_name,
              a.stock_type_id,
              case
                  when a.stock_type_name is null then 'Make service in showroom'
                  else a.stock_type_name
                  end                                           as                             stock_type_name,
              a.stock_model_code,
              a.stock_model_name,
              a.accounting_model_code,
              case
                  when a.stock_model_id is null then ''
                  else a.accounting_model_name
                  end                                           as                             accounting_model_name,
              a.display_model_code,
              a.display_model_name,
              sum(case when a.sale_trans_type = 41 then (-1) * a.quantity else a.quantity end) quantity,
              sum(case
                      when a.sale_trans_type = 41 then (-1) * a.accounting_quantity
                      else a.accounting_quantity end)                                          accounting_quantity,
              case
                  when a.stock_model_id is null then a.sale_services_price
                  else a.price
                  end                                           as                             price,
              a.accounting_price,
              case
                  when a.stock_model_id is null then coalesce(a.sale_services_price_vat, 10)
                  else coalesce(a.vat, 10)
                  end                                           as                             vat,
              SUM(a.amount)                                                                    amount,
              SUM(ROUND((NVL(a.discount_amount, 0) + NVL(a.discount_amount, 0) * nvl(a.vat, 10) / 100) / 2) *
                  2)                                                                           discount_amount,
              SUM(a.vat_amount)                                                                vat_amount,
              null                                              as                             in_services,
              a.accounting_name                                 as                             accounting_sale_service,
              a.shop_code_lv2                                                                  branchcode,
              a.shop_name_lv2                                                                  branchname,
              a.shop_code_lv3                                                                  shop_code_lv3,
              a.shop_name_lv3                                                                  shop_name_lv3,
              a.shop_path                                                                      shop_path
       from bccs3_report_la.rp_daily_revenue a
       where 1 = 1
         and a.sale_trans_date >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
         and a.sale_trans_date < DATE_FORMAT(:date, '%Y%m%d')
         and ((sale_trans_type != 13
           and a.amount <> 0)
           or (sale_trans_type != 4
               and a.amount = 0))
         and ((a.sale_trans_status = 3
           and a.sale_trans_type <> 2)
           or a.sale_trans_status = 5
           or (a.sale_trans_status = 2
               and a.sale_trans_type <> 2))
         and (a.receiver_channel_type is null
           or a.receiver_channel_type not in (1000260))
       group by a.staff_code,
                a.staff_name,
                a.shop_code,
                a.shop_name,
                concat(a.sale_services_id, '_', a.stock_model_id),
                a.sale_trans_type,
                a.sale_services_id,
                a.stock_model_id,
                a.sale_trans_type_name,
                a.stock_type_id,
                a.stock_type_name,
                a.stock_model_code,
                a.stock_model_name,
                a.accounting_model_code,
                a.accounting_model_name,
                a.display_model_code,
                a.display_model_name,
                (case
                     when a.stock_model_id is null then a.sale_services_price
                     else a.price
                    end),
                a.accounting_price,
                (case
                     when a.stock_model_id is null then coalesce(a.sale_services_price_vat, 10)
                     else coalesce(a.vat, 10)
                    end),
                a.sale_services_id,
                a.accounting_name,
                a.shop_code_lv2,
                a.shop_name_lv2,
                a.shop_code_lv3,
                a.shop_name_lv3,
                a.shop_path)
      union all
      (select a.staff_code,
              a.staff_name,
              a.shop_code,
              a.shop_name,
              concat(a.sale_services_id, '_', a.stock_model_id) as order_code,
              a.sale_trans_type,
              a.sale_services_id,
              a.stock_model_id,
              a.sale_trans_type_name,
              a.stock_type_id,
              'Transaction without eliminating stock'           as stock_type_name,
              a.stock_model_code,
              a.stock_model_name,
              null                                              as accounting_model_code,
              null                                              as accounting_model_name,
              a.stock_model_code                                as display_model_code,
              a.stock_model_name                                as display_model_name,
              SUM(a.accounting_quantity)                        as quantity,
              SUM(a.accounting_quantity)                           accounting_quantity,
              a.amount                                          as price,
              a.amount                                          as accounting_price,
              nvl(a.vat,
                  10)                                           as vat,
              SUM(a.amount)                                     as amount,
              0                                                 as discount_amount,
              SUM(a.vat_amount)                                 as vat_amount,
              0                                                 as in_services,
              a.accounting_name                                 as accounting_sale_service,
              a.shop_code_lv2                                      branchcode,
              a.shop_name_lv2                                      branchname,
              a.shop_code_lv3                                      shop_code_lv3,
              a.shop_name_lv3                                      shop_name_lv3,
              a.shop_path                                          shop_path
       from bccs3_report_la.rp_daily_revenue a
       where sale_trans_type = 13
         and a.sale_trans_date >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
         and a.sale_trans_date < DATE_FORMAT(:date, '%Y%m%d')
         and ((a.sale_trans_status = 3
           and a.sale_trans_type <> 2)
           or a.sale_trans_status = 5
           or
              (a.sale_trans_status = 2
                  and a.sale_trans_type <> 2))
         and (a.receiver_channel_type is null
           or a.receiver_channel_type not in (1000260))
       group by a.staff_code,
                a.staff_name,
                a.shop_code,
                a.shop_name,
                concat(a.sale_services_id, '_', a.stock_model_id),
                a.sale_trans_type,
                a.sale_services_id,
                a.stock_model_id,
                a.sale_trans_type_name,
                a.stock_type_id,
                a.stock_type_name,
                a.stock_model_code,
                a.stock_model_name,
                a.accounting_model_code,
                a.accounting_model_name,
                a.display_model_code,
                a.display_model_name,
                a.amount,
                nvl(a.vat,
                    10),
                a.vat_amount,
                a.sale_services_id,
                a.accounting_name,
                a.shop_code_lv2,
                a.shop_name_lv2,
                a.shop_code_lv3,
                a.shop_name_lv3,
                a.shop_path)
      union all
      (select a.staff_code,
              a.staff_name,
              a.shop_code,
              a.shop_name,
              concat(a.sale_services_id, '_', a.stock_model_id) as order_code,
              a.sale_trans_type,
              a.sale_services_id,
              a.stock_model_id,
              a.sale_trans_type_name,
              a.stock_type_id,
              'Make service in showroom'                        as stock_type_name,
              a.stock_model_code,
              a.stock_model_name,
              a.accounting_model_code,
              a.accounting_model_name,
              null                                              as display_model_code,
              null                                              as display_model_name,
              0                                                 as quantity,
              sum(case
                      when a.sale_trans_type = 41 then (-1) * a.accounting_quantity
                      else a.accounting_quantity end)              accounting_quantity,
              0                                                 as price,
              a.accounting_price,
              0                                                 as vat,
              0                                                    amount,
              0                                                 as discount_amount,
              0                                                 as vat_amount,
              1                                                 as in_services,
              null                                              as accounting_sale_service,
              a.shop_code_lv2                                      branchcode,
              a.shop_name_lv2                                      branchname,
              a.shop_code_lv3                                      shop_code_lv3,
              a.shop_name_lv3                                      shop_name_lv3,
              a.shop_path                                          shop_path
       from bccs3_report_la.rp_daily_revenue a
       where a.sale_trans_type = 4
         and a.amount = 0
         and a.sale_trans_date >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
         and a.sale_trans_date < DATE_FORMAT(:date, '%Y%m%d')
         and ((a.sale_trans_status = 3
           and a.sale_trans_type <> 2)
           or a.sale_trans_status = 5
           or (a.sale_trans_status = 2
               and a.sale_trans_type <> 2))
         and (a.receiver_channel_type is null
           or a.receiver_channel_type not in (1000260))
       group by a.staff_code,
                a.staff_name,
                a.shop_code,
                a.shop_name,
                concat(a.sale_services_id, '_', a.stock_model_id),
                a.sale_trans_type,
                a.sale_services_id,
                a.stock_model_id,
                a.sale_trans_type_name,
                a.stock_type_id,
                a.stock_type_name,
                a.stock_model_code,
                a.stock_model_name,
                a.accounting_model_code,
                a.accounting_model_name,
                a.display_model_code,
                a.display_model_name,
                a.price,
                a.accounting_price,
                nvl(a.vat,
                    10),
                a.sale_services_id,
                a.accounting_name,
                a.shop_code_lv2,
                a.shop_name_lv2,
                a.shop_code_lv3,
                a.shop_name_lv3,
                a.SHOP_PATH)) a
where 2 = 2
group by a.staff_code,
         a.staff_name,
         a.shop_code,
         a.shop_name,
         a.order_code,
         a.sale_trans_type,
         a.sale_services_id,
         a.stock_model_id,
         a.sale_trans_type_name,
         a.stock_type_id,
         a.stock_type_name,
         a.stock_model_code,
         a.stock_model_name,
         a.accounting_model_code,
         a.accounting_model_name,
         a.display_model_code,
         a.display_model_name,
         a.price,
         a.accounting_price,
         a.in_services,
         a.accounting_sale_service,
         a.branchcode,
         a.branchname,
         a.shop_code_lv3,
         a.shop_name_lv3,
         a.shop_path
order by a.branchcode,
         a.shop_code,
         a.shop_code_lv3,
         a.staff_code,
         a.stock_type_name,
         a.order_code