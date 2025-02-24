INSERT INTO sync_revenue_diff_rp
(SHOP_ID,
 SHOP_CODE,
 SHOP_NAME,
 SHOP_PATH,
 SHOP_STAFF,
 STAFF_ID,
 STAFF_CODE,
 Revenue_Of_Sale_Report_3_1,
 Revenue_Of_Sale_Report_4_1,
 Revenue_Of_Sale_Pending_4_1,
 DIFFERENCE_REVENUE_REPORT_3_1_WITH_4_1,
 DIFFERENCE_REVENUE_REPORT_Pending_3_1_WITH_4_1,
 NOTE,
 SYNC_DATE)
    (SELECT drd.SHOP_ID,
            drd.SHOP_CODE,
            drd.SHOP_NAME,
            drd.SHOP_PATH,
            drd.SHOP_STAFF,
            drd.STAFF_ID,
            drd.STAFF_CODE,
            Revenue_Of_Sale_Report_3_1,
            Revenue_Of_Sale_Report_4_1,
            Revenue_Of_Sale_Pending_4_1,
            DIFFERENCE_REVENUE_REPORT_3_1_WITH_4_1,
            DIFFERENCE_REVENUE_REPORT_Pending_3_1_WITH_4_1,
            null  as NOTE,
            NOW() AS SYNC_DATE
     FROM (SELECT rp_31.SHOP_ID,
                  rp_31.SHOP_CODE,
                  rp_31.SHOP_NAME,
                  rp_31.SHOP_PATH,
                  rp_31.STAFF_ID,
                  rp_31.SHOP_STAFF,
                  rp_31.STAFF_CODE,
                  COALESCE(Revenue_Of_Sale_Report_3_1, 0) AS Revenue_Of_Sale_Report_3_1,
                  COALESCE(Revenue_Of_Sale_Report_4_1, 0) AS Revenue_Of_Sale_Report_4_1,
                  null                                    AS Revenue_Of_Sale_Pending_4_1,
                  COALESCE(Revenue_Of_Sale_Report_3_1, 0) -
                  COALESCE(Revenue_Of_Sale_Report_4_1, 0) AS DIFFERENCE_REVENUE_REPORT_3_1_WITH_4_1,
                  null                                    AS DIFFERENCE_REVENUE_REPORT_Pending_3_1_WITH_4_1
           FROM (select srs.SHOP_ID,
                        srs.SHOP_CODE,
                        srs.SHOP_NAME,
                        srs.SHOP_PATH,
                        CONCAT(srs.SHOP_CODE, '|', srs.STAFF_NAME) AS SHOP_STAFF,
                        srs.STAFF_ID,
                        srs.STAFF_CODE,
                        SUM(srs.AMOUNT_TAX)                           Revenue_Of_Sale_Report_3_1
                 FROM sync_revenue_staff_rp srs
                 GROUP BY srs.SHOP_CODE, srs.SHOP_NAME, srs.SHOP_CODE, srs.STAFF_NAME, srs.STAFF_CODE) rp_31
                    LEFT JOIN
                (SELECT dsr.shop_id,
                        dsr.SHOP_CODE,
                        dsr.SHOP_NAME,
                        dsr.SHOP_PATH,
                        CONCAT(dsr.SHOP_CODE, '|', dsr.STAFF_NAME) AS SHOP_STAFF,
                        dsr.STAFF_ID,
                        dsr.STAFF_CODE,
                        dsr.REVENUE_OF_SALE_DETAIL                    Revenue_Of_Sale_Report_4_1
                 FROM bccs3_inventory_la.sync_debit_staff_rp dsr
                 GROUP BY dsr.SHOP_CODE, dsr.SHOP_NAME, SHOP_STAFF, dsr.STAFF_CODE) rp_41
                on rp_31.SHOP_CODE = rp_41.SHOP_CODE and rp_31.STAFF_CODE = rp_41.STAFF_CODE) drd);





WITH date_range AS (SELECT STR_TO_DATE(:fromDate, '%Y-%m-%d') AS month_start,
                           STR_TO_DATE(:toDate, '%Y-%m-%d')   AS month_end)
select SHOP_ID,
       SHOP_CODE,
       SHOP_NAME,
       SHOP_PATH,
       SHOP_STAFF,
       STAFF_ID,
       STAFF_CODE,
       Revenue_Of_Sale_Report_3_1,
       Revenue_Of_Sale_Report_4_1,
       Revenue_Of_Sale_Pending_4_1,
       DIFFERENCE_REVENUE_REPORT_3_1_WITH_4_1,
       DIFFERENCE_REVENUE_REPORT_Pending_3_1_WITH_4_1,
       NOTE,
       SYNC_DATE
from sync_revenue_diff_rp
WHERE 1 = 1
  AND (
    (:shopId IS NOT NULL AND (SHOP_ID = :shopId OR SHOP_PATH LIKE CONCAT('%\\_', :shopId, '\\_%')))
        OR
    (:shopId IS NULL AND
     (shop_id = :provinceShopId OR SHOP_PATH LIKE CONCAT('%\\_', :provinceShopId, '\\_%')))
    )
  and SYNC_DATE BETWEEN (SELECT month_start FROM date_range) AND (SELECT month_end FROM date_range)
  AND STAFF_ID = :staffID