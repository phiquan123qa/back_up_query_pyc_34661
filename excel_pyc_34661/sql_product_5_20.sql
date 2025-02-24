INSERT INTO sync_offline_invent_summary_rp(PARENT_SHOP_ID,
                                                  PARENT_SHOP_CODE,
                                                  PARENT_SHOP_NAME,
                                                  SHOP_ID,
                                                  SHOP_CODE,
                                                  SHOP_NAME,
                                                  SHOP_PATH,
                                                  STAFF_CODE,
                                                  STAFF_NAME,
                                                  INVENTORY_TYPE,
                                                  INVENTORY_CODE,
                                                  INVENTORY_NAME,
                                                  STATUS,
                                                  UNIT_NAME,
                                                  CHANNEL_TYPE_ID,
                                                  AMOUNT,
                                                  QUANTITY_INFERIOR,
                                                  ACTIVE,
                                                  CREATE_DATE,
                                                  SYNC_DATE)


    (select parent_shop_id,
            parent_shop_code,
            parent_shop_name,
            shop_id,
            (case
                 when staff_code is null then CONCAT(shop_code, ' - ', shop_name)
                 else concat(concat(shop_code, '-', shop_name), ' | ', concat(staff_code, '-', staff_name))
                end)                                                                        as shop_code,
            shop_name,
            (select shop_path from bccs3_inventory_la.shop s where s.shop_id = roi.shop_id) as shop_path,
            staff_code,
            staff_name,
            pro_offer_type_name,
            product_offer_code,
            product_offer_name,
            state_id,
            unit_name,
            channel_type_id,
            quantity                                                                        as amount,
            nvl(quantity_inferior, 0)                                                       as quantity_inferior,
            CASE status
                WHEN 1 THEN 'Active'
                WHEN 0 THEN 'Inactive'
                END                                                                         as status,
            created_datetime,
            now()                                                                              sync_date
     from bccs3_report_la.rp_offline_inventory roi
     where roi.created_datetime = LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
     order by parent_shop_code,
              shop_code,
              pro_offer_type_name,
              product_offer_code);









