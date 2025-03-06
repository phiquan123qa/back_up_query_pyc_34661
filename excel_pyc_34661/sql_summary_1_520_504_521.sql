INSERT INTO bccs3_inventory_la.sync_serial_invent_revenue_diff_rp (SHOP_ID,
                                                                   SHOP_PATH,
                                                                   SHOP_CODE,
                                                                   SHOP_NAME,
                                                                   PARENT_SHOP_CODE,
                                                                   PARENT_SHOP_ID,
                                                                   PARENT_SHOP_NAME,
                                                                   prod_offer_type_name,
                                                                   product_offer_code,
                                                                   product_offer_name,
                                                                   state_id,
                                                                   state_name,
                                                                   status,
                                                                   UNIT_NAME,
                                                                   TOTAL_520,
                                                                   TOTAL_521,
                                                                   TOTAL_504,
                                                                   DIFFERENCE_520_521,
                                                                   DIFFERENCE_520_504,
                                                                   NOTE,
                                                                   SYNC_DATE,
                                                                   period_report)
    (SELECT base.shop_id,
            base.SHOP_PATH,
            base.SHOP_CODE,
            base.shop_name,
            base.PARENT_SHOP_CODE,
            base.PARENT_SHOP_ID,
            base.PARENT_SHOP_NAME,
            base.prod_offer_type_name,
            base.product_offer_code,
            base.product_offer_name,
            base.state_id,
            base.state_name,
            sirs.STATUS,
            sirs.UNIT_NAME,
            COALESCE(sirs_count, 0)                          AS sirs_count,
            COALESCE(sir_count, 0)                           AS sir_count,
            COALESCE(ir_count, 0)                            AS ir_count,
            COALESCE(sirs_count, 0) - COALESCE(sir_count, 0) AS sirs_minus_sir,
            COALESCE(sirs_count, 0) - COALESCE(ir_count, 0)  AS sirs_minus_ir,
            null                                             AS NOTE,
            NOW()                                            AS SYNC_DATE,
            str_to_date(:period_report, '%Y%m%d')            AS period_report
     FROM (SELECT DISTINCT SHOP_ID,
                           SHOP_CODE,
                           SHOP_NAME,
                           SHOP_PATH,
                           PARENT_SHOP_ID,
                           PARENT_SHOP_CODE,
                           PARENT_SHOP_NAME,
                           prod_offer_type_name,
                           product_offer_code,
                           product_offer_name,
                           state_id,
                           state_name,
                           STATUS,
                           UNIT_NAME
           FROM (SELECT SHOP_ID,
                        SHOP_CODE,
                        SHOP_NAME,
                        SHOP_PATH,
                        PARENT_SHOP_ID,
                        PARENT_SHOP_CODE,
                        PARENT_SHOP_NAME,
                        prod_offer_type_name,
                        product_offer_code,
                        product_offer_name,
                        state_id,
                        state_name,
                        STATUS,
                        UNIT_NAME
                 FROM bccs3_inventory_la.sync_offline_invent_summary_rp) AS all_products) AS base
              LEFT JOIN (SELECT SHOP_ID,
                                SHOP_NAME,
                                SHOP_CODE,
                                SHOP_PATH,
                                prod_offer_type_name,
                                product_offer_code,
                                product_offer_name,
                                state_id,
                                state_name,
                                SUM(QUANTITY) AS sirs_count,
                                CASE
                                    WHEN STATUS = '1' THEN 'Active'
                                    WHEN STATUS = '0' THEN 'Inactive'
                                    ELSE 'Unknown'
                                    END       AS STATUS,
                                UNIT_NAME
                         FROM bccs3_inventory_la.sync_offline_invent_summary_rp
                         GROUP BY shop_id, shop_path, prod_offer_type_name, product_offer_code, product_offer_name,
                                  state_id, STATUS, UNIT_NAME) AS sirs
                        ON base.shop_id = sirs.shop_id
                            AND base.shop_path = sirs.shop_path
                            AND base.prod_offer_type_name = sirs.prod_offer_type_name
                            AND base.product_offer_code = sirs.product_offer_code
                            AND base.product_offer_name = sirs.product_offer_name
                            AND base.state_id = sirs.state_id
              LEFT JOIN (SELECT SHOP_ID,
                                SHOP_NAME,
                                SHOP_CODE,
                                SHOP_PATH,
                                prod_offer_type_name,
                                product_offer_code,
                                product_offer_name,
                                state_id,
                                state_name,
                                SUM(QUANTITY) AS sir_count
                         FROM bccs3_inventory_la.sync_offline_serial_invent_rp
                         GROUP BY SHOP_ID, SHOP_PATH, prod_offer_type_name, product_offer_code, product_offer_name,
                                  state_id) AS sir
                        ON base.shop_id = sir.SHOP_ID
                            AND base.shop_path = sir.SHOP_path
                            AND base.prod_offer_type_name = sir.prod_offer_type_name
                            AND base.product_offer_code = sir.product_offer_code
                            AND base.product_offer_name = sir.product_offer_name
                            AND base.state_id = sir.state_id
              LEFT JOIN (SELECT SHOP_ID,
                                SHOP_NAME,
                                SHOP_CODE,
                                SHOP_PATH,
                                prod_offer_type_name,
                                product_offer_code,
                                product_offer_name,
                                state_id,
                                state_name,
                                SUM(QUANTITY) AS ir_count
                         FROM bccs3_inventory_la.sync_serial_invent_rp
                         GROUP BY SHOP_ID, PARENT_SHOP_CODE, PARENT_SHOP_ID, SHOP_path, prod_offer_type_name,
                                  product_offer_code,
                                  product_offer_name, state_id) AS ir ON base.shop_id = ir.SHOP_ID
         AND base.shop_path = ir.SHOP_path
         AND base.prod_offer_type_name = ir.prod_offer_type_name
         AND base.product_offer_code = ir.product_offer_code
         AND base.product_offer_name = ir.product_offer_name
         AND base.state_id = ir.state_id)