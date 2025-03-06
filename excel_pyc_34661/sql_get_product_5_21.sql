insert
into bccs3_inventory_la.sync_offline_serial_invent_rp(PARENT_SHOP_ID,
                                                      PARENT_SHOP_CODE,
                                                      PARENT_SHOP_NAME,
                                                      SHOP_ID,
                                                      SHOP_CODE,
                                                      SHOP_NAME,
                                                      SHOP_PATH,
                                                      STAFF_ID,
                                                      STAFF_CODE,
                                                      STAFF_NAME,
                                                      PROD_OFFER_TYPE_NAME,
                                                      PRODUCT_OFFER_CODE,
                                                      PRODUCT_OFFER_NAME,
                                                      STATE_ID,
                                                      STATE_NAME,
                                                      FROM_SERIAL,
                                                      TO_SERIAL,
                                                      QUANTITY,
                                                      BRANCH_NAME,
                                                      CREATE_DATE,
                                                      SYNC_DATE,
                                                      PERIOD_REPORT,
                                                      AGGREGATION_DATE)
    (select s.PARENT_SHOP_ID                                                                       as PARENT_SHOP_ID,
            s.PAR_SHOP_CODE                                                                        as PARENT_SHOP_CODE,
            (select sp.name from bccs3_inventory_la.shop sp where rois.shop_id = s.PARENT_SHOP_ID) as PARENT_SHOP_NAME,
            rois.shop_id                                                                           as SHOP_ID,
            (select shop_code from bccs3_inventory_la.shop sp where rois.shop_id = sp.shop_id)     as SHOP_CODE,
            rois.shop_name                                                                         as SHOP_NAME,
            rois.shop_path                                                                         as SHOP_PATH,
            rois.staff_id                                                                          as STAFF_ID,
            (select staff_code from bccs3_inventory_la.staff st where rois.staff_id = st.staff_id) as STAFF_CODE,
            (select name from bccs3_inventory_la.staff st where rois.staff_id = st.staff_id)       as STAFF_NAME,
            rois.stock_type_name                                                                   as PROD_OFFER_TYPE_NAME,
            rois.stock_model_code                                                                  as PRODUCT_OFFER_CODE,
            rois.stock_model_name                                                                  as PRODUCT_OFFER_NAME,
            case
                when rois.state_name = 'New' then '1'
                when rois.state_name = 'Good' then '2'
                when rois.state_name = 'Damaged' then '3'
                when rois.state_name = 'Revoked' then '4'
                when rois.state_name = 'Sale Punish' then '5'
                when rois.state_name = 'Lock' then '6'
                when rois.state_name = 'Missing' then '7'
                else '0'
                end                                                                                as state_id,
            rois.state_name                                                                        as STATE_NAME,
            rois.from_serial                                                                       as FROM_SERIAL,
            rois.to_serial                                                                         as TO_SERIAL,
            rois.quantity                                                                          as QUANTITY,
            rois.branch_name                                                                       as BRANCH_NAME,
            rois.create_date                                                                       as CREATE_DATE,
            now()                                                                                     sync_date,
            str_to_date(:period_report, '%Y%m%d')                                                  as PERIOD_REPORT,
            rois.aggregation_date                                                                  as AGGREGATION_DATE

     from bccs3_report_la.rp_offline_inventory_serials rois
              left join bccs3_inventory_la.shop s on
         rois.shop_id = s.shop_id
     where year(rois.aggregation_date) = year(LAST_DAY(DATE_SUB(CURRENT_DATE(), interval 1 month)))
       and month(rois.aggregation_date) = month(LAST_DAY(DATE_SUB(CURRENT_DATE(), interval 1 month)))
       and staff_id is null
     group by PARENT_SHOP_ID,
              PARENT_SHOP_CODE,
              PARENT_SHOP_NAME,
              SHOP_ID,
              SHOP_CODE,
              SHOP_NAME,
              SHOP_PATH,
              STAFF_ID,
              STAFF_CODE,
              STAFF_NAME,
              PROD_OFFER_TYPE_NAME,
              PRODUCT_OFFER_CODE,
              PRODUCT_OFFER_NAME,
              STATE_ID,
              STATE_NAME,
              FROM_SERIAL,
              TO_SERIAL,
              QUANTITY,
              BRANCH_NAME,
              CREATE_DATE,
              AGGREGATION_DATE
     order by SHOP_NAME,
              PRODUCT_OFFER_CODE,
              STATE_NAME)