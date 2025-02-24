INSERT INTO bccs3_inventory_la.sync_offline_serial_invent_rp(SHOP_NAME,
                                                                    SHOP_ID,
                                                                    SHOP_PATH,
                                                                    PARENT_SHOP_ID,
                                                                    PARENT_SHOP_CODE,
                                                                    PARENT_SHOP_NAME,
                                                                    BRANCH_NAME,
                                                                    INVENTORY_TYPE,
                                                                    INVENTORY_CODE,
                                                                    INVENTORY_NAME,
                                                                    AMOUNT,
                                                                    FROM_SERIAL,
                                                                    TO_SERIAL,
                                                                    STATUS,
                                                                    CREATE_DATE,
                                                                    CONTRACT_CODE,
                                                                    AGGREGATION_DATE,
                                                                    SYNC_DATE)
    (select rois.shop_name,
            rois.shop_id,
            rois.shop_path,
            s.shop_id   AS PARENT_SHOP_ID,
            s.shop_code AS PARENT_SHOP_CODE,
            s.name      AS PARENT_SHOP_NAME,
            rois.branch_name,
            rois.stock_type_name,
            rois.stock_model_code,
            rois.stock_model_name,
            rois.quantity,
            rois.from_serial,
            rois.to_serial,
            CASE
                WHEN state_name = 'New' THEN '1'
                WHEN state_name = 'Good' THEN '2'
                WHEN state_name = 'Damaged' THEN '3'
                WHEN state_name = 'Revoked' THEN '4'
                WHEN state_name = 'Sale Punish' THEN '5'
                WHEN state_name = 'Lock' THEN '6'
                WHEN state_name = 'Missing' THEN '7'
                ELSE '0'
                END     AS STATUS,
            rois.create_date,
            contract_code,
            rois.aggregation_date,
            now()          sync_date
     from bccs3_report_la.rp_offline_inventory_serials rois
              left join bccs3_inventory_la.shop s ON rois.shop_id = s.shop_id
     where year(rois.aggregation_date) = year(LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)))
       and month(rois.aggregation_date) = month(LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)))
       and staff_id is null
     order by shop_name,
              stock_model_code,
              state_name);
