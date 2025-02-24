-- 247
-- SScript insert to table
INSERT INTO bccs3_inventory_la.sync_serial_invent_revenue_rp (SHOP_ID,
                                                                     SHOP_PATH,
#                                                                      SHOP_CODE,
                                                                     SHOP_NAME,
                                                                     PARENT_SHOP_CODE,
                                                                     PARENT_SHOP_ID,
                                                                     PARENT_SHOP_NAME,
                                                                     INVENTORY_TYPE,
                                                                     INVENTORY_CODE,
                                                                     INVENTORY_NAME,
                                                                     STATUS,
                                                                     TOTAL_520,
                                                                     TOTAL_521,
                                                                     TOTAL_504,
                                                                     DIFFERENCE_520_521,
                                                                     DIFFERENCE_520_504,
                                                                     NOTE,
                                                                     SYNC_DATE)
    (SELECT base.shop_id,
            base.SHOP_PATH,
            base.SHOP_CODE,
            base.shop_name,
            base.PARENT_SHOP_CODE,
            base.PARENT_SHOP_ID,
            base.PARENT_SHOP_NAME,
            base.INVENTORY_TYPE,
            base.inventory_code,
            base.inventory_name,
            base.STATUS,
            COALESCE(sirs_count, 0)                          AS sirs_count, -- Number of records in sync_offline_inventory_summary_report
            COALESCE(sir_count, 0)                           AS sir_count,  -- Number of records in sync_offline_serial_inventory_report
            COALESCE(ir_count, 0)                            AS ir_count,   -- Number of records in sync_serial_inventory_report
            COALESCE(sirs_count, 0) - COALESCE(sir_count, 0) AS sirs_minus_sir,
            COALESCE(sirs_count, 0) - COALESCE(ir_count, 0)  AS sirs_minus_ir,
            null                                             AS NOTE,
            NOW()                                            AS SYNC_DATE
     FROM (
              -- Base query to get all distinct products
              SELECT DISTINCT SHOP_ID,
                              SHOP_CODE,
                              SHOP_NAME,
                              SHOP_PATH,
                              PARENT_SHOP_ID,
                              PARENT_SHOP_CODE,
                              PARENT_SHOP_NAME,
                              INVENTORY_TYPE,
                              inventory_code,
                              inventory_name,
                              STATUS
              FROM (SELECT SHOP_ID,
                           SHOP_CODE,
                           SHOP_NAME,
                           SHOP_PATH,
                           PARENT_SHOP_ID,
                           PARENT_SHOP_CODE,
                           PARENT_SHOP_NAME,
                           INVENTORY_TYPE,
                           inventory_code,
                           inventory_name,
                           STATUS
                    FROM bccs3_inventory_la.sync_offline_inventory_summary_report
                    UNION ALL
                    SELECT SHOP_ID,
                           SHOP_CODE,
                           SHOP_NAME,
                           SHOP_PATH,
                           PARENT_SHOP_ID,
                           PARENT_SHOP_CODE,
                           PARENT_SHOP_NAME,
                           INVENTORY_TYPE,
                           inventory_code,
                           inventory_name,
                           STATUS
                    FROM bccs3_inventory_la.sync_offline_serial_inventory_report
                    UNION ALL
                    SELECT SHOP_ID,
                           SHOP_NAME,
                           SHOP_CODE,
                           SHOP_PATH,
                           PARENT_SHOP_ID,
                           PARENT_SHOP_CODE,
                           PARENT_SHOP_NAME,
                           INVENTORY_TYPE,
                           inventory_code,
                           inventory_name,
                           STATUS
                    FROM bccs3_inventory_la.sync_serial_inventory_report) AS all_products) AS base
-- Left join to count records in sync_offline_inventory_summary_report
              LEFT JOIN (SELECT SHOP_ID,
                                SHOP_NAME,
                                SHOP_CODE,
                                SHOP_PATH,
                                INVENTORY_TYPE,
                                inventory_code,
                                inventory_name,
                                STATUS,
                                SUM(AMOUNT) AS sirs_count
                         FROM bccs3_inventory_la.sync_offline_inventory_summary_report
                         GROUP BY shop_id, shop_name, INVENTORY_TYPE, inventory_code, inventory_name, STATUS) AS sirs
                        ON base.shop_id = sirs.shop_id
                            AND base.shop_name = sirs.shop_name
                            AND base.INVENTORY_TYPE = sirs.INVENTORY_TYPE
                            AND base.inventory_code = sirs.inventory_code
                            AND base.inventory_name = sirs.inventory_name
                            AND base.STATUS = sirs.STATUS
-- Left join to count records in sync_offline_serial_inventory_report
              LEFT JOIN (SELECT SHOP_ID,
                                SHOP_NAME,
                                SHOP_CODE,
                                SHOP_PATH,
                                INVENTORY_TYPE,
                                inventory_code,
                                inventory_name,
                                STATUS,
                                SUM(AMOUNT) AS sir_count
                         FROM bccs3_inventory_la.sync_offline_serial_inventory_report
                         GROUP BY SHOP_ID, SHOP_NAME, INVENTORY_TYPE, inventory_code, inventory_name, STATUS) AS sir
                        ON base.shop_id = sir.SHOP_ID
                            AND base.shop_name = sir.SHOP_NAME
                            AND base.INVENTORY_TYPE = sir.INVENTORY_TYPE
                            AND base.inventory_code = sir.inventory_code
                            AND base.inventory_name = sir.inventory_name
                            AND base.STATUS = sir.STATUS
-- Left join to count records in sync_serial_inventory_report
              LEFT JOIN (SELECT SHOP_ID,
                                SHOP_NAME,
                                SHOP_CODE,
                                SHOP_PATH,
                                INVENTORY_TYPE,
                                inventory_code,
                                inventory_name,
                                STATUS,
                                SUM(AMOUNT) AS ir_count
                         FROM bccs3_inventory_la.sync_serial_inventory_report
                         GROUP BY SHOP_ID, PARENT_SHOP_CODE, PARENT_SHOP_ID, SHOP_NAME, INVENTORY_TYPE, inventory_code,
                                  inventory_name, STATUS) AS ir ON base.shop_id = ir.SHOP_ID
         AND base.shop_name = ir.SHOP_NAME
         AND base.INVENTORY_TYPE = ir.INVENTORY_TYPE
         AND base.inventory_code = ir.inventory_code
         AND base.inventory_name = ir.inventory_name
         AND base.STATUS = ir.STATUS
#      group by base.shop_id,
#               base.PARENT_SHOP_ID,
#               base.PARENT_SHOP_ID,
#               base.INVENTORY_TYPE,
#               base.INVENTORY_CODE,
#               base.STATUS
     );

# -------------------------------------------------------------------------------
-- Query to get all distinct products with fillter
WITH date_range
         AS (SELECT DATE_ADD(STR_TO_DATE(CONCAT('01/', :monthYear), '%d/%m/%Y'), INTERVAL 1 MONTH) AS month_start,
                    LAST_DAY(DATE_ADD(STR_TO_DATE(CONCAT('01/', :monthYear), '%d/%m/%Y'), INTERVAL 1
                                      MONTH))                                                      AS month_end)
SELECT r.*
FROM bccs3_inventory_la.sync_serial_invent_revenue_rp r
WHERE 1 = 1
  AND (
    (:shopId IS NOT NULL AND (r.SHOP_ID = :shopId OR r.SHOP_PATH LIKE CONCAT('%\\_', :shopId, '\\_%')))
        OR
    (:shopId IS NULL AND
     (r.shop_id = :provinceShopId OR r.SHOP_PATH LIKE CONCAT('%\\_', :provinceShopId, '\\_%')))
    )
  and r.SYNC_DATE BETWEEN (SELECT month_start FROM date_range) AND (SELECT month_end FROM date_range);


