INSERT INTO bccs3_inventory_la.sync_revenue_staff_rp (staff_code,
                                                      staff_name,
                                                      shop_code,
                                                      shop_name,
                                                      order_code,
                                                      SALE_TRANS_TYPE,
                                                      SALE_SERVICE_ID,
                                                      STOCK_MODEL_ID,
                                                      SALE_TRANS_TYPE_NAME,
                                                      STOCK_TYPE_ID,
                                                      STOCK_TYPE_NAME,
                                                      STOCK_MODEL_CODE,
                                                      STOCK_MODEL_NAME,
                                                      ACCOUNTING_MODEL_CODE,
                                                      ACCOUNTING_MODEL_NAME,
                                                      DISPLAY_MODEL_CODE,
                                                      DISPLAY_MODEL_NAME,
                                                      QUANTITY,
                                                      ACCOUNTING_QUANTITY,
                                                      PRICE,
                                                      ACCOUNTING_PRICE,
                                                      AMOUNT,
                                                      DISCOUNT_AMOUNT,
                                                      AMOUNT_TAX,
                                                      AMOUNT_NOT_TAX,
                                                      VAT_AMOUNT,
                                                      IN_SERVICE,
                                                      ACCOUNTING_SALE_SERVICE,
                                                      BRANCH_CODE,
                                                      BRANCH_NAME,
                                                      SHOP_CODE_LV3,
                                                      SHOP_NAME_LV3,
                                                      SHOP_ID,
                                                      SHOP_PATH,
                                                      STAFF_ID,
                                                      SYNC_DATE)
    (select a.staff_code,
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
            SUM(a.quantity)                                            quantity,
            SUM(a.accounting_quantity)                                 accounting_quantity,
            a.price,
            a.accounting_price,
            SUM(a.amount)                                              amount,
            SUM(a.discount_amount)                                     discount_amount,
            SUM(a.amount) - SUM(a.discount_amount)                     amount_tax,
            SUM(a.amount) - SUM(a.discount_amount) - SUM(a.vat_amount) amount_not_tax,
            SUM(a.vat_amount)                                          vat_amount,
            a.in_services,
            a.accounting_sale_service,
            nvl(a.branchcode, a.shop_code)                             branchcode,
            nvl(a.branchname, a.shop_name)                             branchname,
            nvl(a.shop_code_lv3, a.shop_code)                          shop_code_lv3,
            nvl(a.shop_name_lv3, a.shop_name)                          shop_name_lv3,
            SHOP_ID,
            SHOP_PATH,
            STAFF_ID,
            NOW()                                                      sync_date
     from ((select a.SHOP_ID,
                   a.SHOP_PATH,
                   a.STAFF_ID,
                   a.staff_code,
                   a.staff_name,
                   a.shop_code,
                   a.shop_name,
                   concat(a.sale_services_id, '_', a.stock_model_id) as                             order_code,
                   a.sale_trans_type,
                   a.sale_services_id,
                   a.stock_model_id,
                   a.sale_trans_type_name,
                   a.stock_type_id,
                   CASE
                       WHEN a.stock_type_name IS NULL THEN 'Make service in showroom'
                       ELSE a.stock_type_name
                       END                                           AS                             stock_type_name,
                   a.stock_model_code,
                   a.stock_model_name,
                   a.accounting_model_code,
                   CASE
                       WHEN a.stock_model_id IS NULL THEN ''
                       ELSE a.accounting_model_name
                       END                                           AS                             accounting_model_name,
                   a.display_model_code,
                   a.display_model_name,
                   sum(case when a.sale_trans_type = 41 then (-1) * a.quantity else a.quantity end) quantity,
                   sum(case
                           when a.sale_trans_type = 41 then (-1) * a.accounting_quantity
                           else a.accounting_quantity end)                                          accounting_quantity,
                   CASE
                       WHEN a.stock_model_id IS NULL THEN a.sale_services_price
                       ELSE a.price
                       END                                           AS                             price,
                   a.accounting_price,
                   CASE
                       WHEN a.stock_model_id IS NULL THEN COALESCE(a.sale_services_price_vat, 10)
                       ELSE COALESCE(a.vat, 10)
                       END                                           AS                             vat,
                   SUM(a.amount)                                                                    amount,
                   SUM(ROUND((NVL(a.discount_amount, 0) + NVL(a.discount_amount, 0) * nvl(a.vat, 10) / 100) / 2) *
                       2)                                                                           discount_amount,
                   SUM(a.vat_amount)                                                                vat_amount,
                   null                                              as                             in_services,
                   a.accounting_name                                 as                             accounting_sale_service,
                   a.shop_code_lv2                                                                  branchcode,
                   a.shop_name_lv2                                                                  branchname,
                   a.shop_code_lv3                                                                  shop_code_lv3,
                   a.shop_name_lv3                                                                  shop_name_lv3
            from bccs3_report_la.rp_daily_revenue a
            where 1 = 1
              and ((sale_trans_type != 13 and a.amount <> 0) or (sale_trans_type != 4 and a.amount = 0))
              and a.sale_trans_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), '%Y-%m-01')
              and a.sale_trans_date < LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) + INTERVAL 1 DAY
#          and (shop_id = :shopId or :shopId or SHOP_PATH like CONCAT('%\\_', :shopId, '\\_%'))
#          and a.STAFF_ID = :staffId
              and ((a.sale_trans_status = 3 and a.sale_trans_type <> 2) or a.sale_trans_status = 5
                or (a.sale_trans_status = 2 and a.sale_trans_type <> 2))
              and (a.receiver_channel_type is null or a.receiver_channel_type not in (1000260))
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
                     (CASE
                          WHEN a.stock_model_id IS NULL THEN a.sale_services_price
                          ELSE a.price
                         END),
                     a.accounting_price,
                     (CASE
                          WHEN a.stock_model_id IS NULL THEN COALESCE(a.sale_services_price_vat, 10)
                          ELSE COALESCE(a.vat, 10)
                         END),
                     a.sale_services_id,
                     a.accounting_name,
                     a.shop_code_lv2,
                     a.shop_name_lv2,
                     a.shop_code_lv3,
                     a.shop_name_lv3)
           union all
           (select a.SHOP_ID,
                   a.SHOP_PATH,
                   a.STAFF_ID,
                   a.staff_code,
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
                   nvl(a.vat, 10)                                    as vat,
                   SUM(a.amount)                                     as amount,
                   0                                                 as discount_amount,
                   SUM(a.vat_amount)                                 as vat_amount,
                   0                                                 as in_services,
                   a.accounting_name                                 as accounting_sale_service,
                   a.shop_code_lv2                                      branchcode,
                   a.shop_name_lv2                                      branchname,
                   a.shop_code_lv3                                      shop_code_lv3,
                   a.shop_name_lv3                                      shop_name_lv3
            from bccs3_report_la.rp_daily_revenue a
            where sale_trans_type = 13


              and a.sale_trans_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), '%Y-%m-01')
              and a.sale_trans_date < LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) + INTERVAL 1 DAY
#          and (shop_id = :shopId or :shopId or SHOP_PATH like CONCAT('%\\_', :shopId, '\\_%'))
#          and a.STAFF_ID = :staffId

              and ((a.sale_trans_status = 3 and a.sale_trans_type <> 2) or a.sale_trans_status = 5 or
                   (a.sale_trans_status = 2 and a.sale_trans_type <> 2))
              and (a.receiver_channel_type is null or a.receiver_channel_type not in (1000260))
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
                     nvl(a.vat, 10),
                     a.vat_amount,
                     a.sale_services_id,
                     a.accounting_name,
                     a.shop_code_lv2,
                     a.shop_name_lv2,
                     a.shop_code_lv3,
                     a.shop_name_lv3)
           union all
           (select a.SHOP_ID,
                   a.SHOP_PATH,
                   a.STAFF_ID,
                   a.staff_code,
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
                   a.shop_name_lv3                                      shop_name_lv3
            from bccs3_report_la.rp_daily_revenue a
            where a.sale_trans_type = 4
              and a.amount = 0
              and a.sale_trans_date >= DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), '%Y-%m-01')
              and a.sale_trans_date < LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) + INTERVAL 1 DAY
#          and (shop_id = :shopId or :shopId or SHOP_PATH like CONCAT('%\\_', :shopId, '\\_%'))
#          and a.STAFF_ID = :staffId
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
                     nvl(a.vat, 10),
                     a.sale_services_id,
                     a.accounting_name,
                     a.shop_code_lv2,
                     a.shop_name_lv2,
                     a.shop_code_lv3,
                     a.shop_name_lv3)) a
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
              a.shop_name_lv3
     order by a.branchcode,
              a.shop_code,
              a.shop_code_lv3,
              a.staff_code,
              a.stock_type_name,
              a.order_code)


