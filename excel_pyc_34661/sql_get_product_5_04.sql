-- Câu lệnh insert dữ liệu vào bảng sync_serial_inventory_report
INSERT INTO bccs3_inventory_la.sync_serial_invent_rp (SHOP_CODE,
                                                             SHOP_NAME,
                                                             PARENT_SHOP_ID,
                                                             PARENT_SHOP_NAME,
                                                             PARENT_SHOP_CODE,
                                                             SHOP_PATH,
                                                             SHOP_ID,
                                                             INVENTORY_TYPE,
                                                             INVENTORY_NAME,
                                                             INVENTORY_CODE,
                                                             AMOUNT,
                                                             FROM_SERIAL,
                                                             TO_SERIAL,
                                                             STATUS,
                                                             SYNC_DATE)
    (select a.shop_code,
            a.shop_name,
            a.parent_shop_code,
            a.parent_shop_name,
            a.parent_shop_id,
            a.SHOP_PATH,
            a.shop_id,
            'SIM'                                                stock_type_name,
            po.code                                              stock_model_code,
            po.NAME,
            MAX(to_number_serial) - MIN(to_number_serial) + 1 as quantity,
            cast(MIN(to_number_serial) as char)                  from_serial,
            cast(MAX(to_number_serial) as char)                  to_serial,
            state_id                                             state_name,
            NOW()                                                create_date
     from bccs3_inventory_la.product_offering po,
          (select s.SHOP_CODE                            shop_code,
                  s.name                                 shop_name,
                  s.parent_shop_id,
                  s.parent_shop_code,
                  s.parent_shop_name,
                  s.SHOP_PATH,
                  s.SHOP_ID                              shop_id,
                  ss.prod_offer_id,
                  state_id,
                  cast(DATE(ss.create_date) as DATETIME) create_date,
                  ss.contract_code,
                  ss.to_number_serial,
                  ss.to_number_serial -
                  row_number() over (
                      order by
                          s.shop_id,ss.PROD_OFFER_ID,
                          ss.to_number_serial)           rn
           from (select s.*,
                        sh.SHOP_CODE parent_shop_code,
                        sh.NAME      parent_shop_name
                 from bccs3_inventory_la.shop s
                          LEFT JOIN bccs3_catalog_la.shop sh
                                    on sh.parent_shop_id =
                                       2 and
                                       sh.shop_code like
                                       'BOP%' and
                                       s.shop_path like
                                       CONCAT(sh.shop_path, '_%')) s,
                bccs3_inventory_la.stock_sim ss
           where s.shop_id = ss.OWNER_ID
             and ss.owner_type = 1
             and s.STATUS = 1
             and ss.status = 1
             and s.shop_id not in (35387, 30885)
             and s.shop_id <> 2) a
     where po.PROD_OFFER_ID = a.prod_offer_id
       and po.STATUS <> 0
     group by a.shop_name,
              po.code,
              po.ACCOUNTING_MODEL_CODE,
              po.name,
              a.state_id,
              a.rn
     order by a.shop_name, po.code, a.state_id)
union all
(select a.shop_code,
        a.shop_name,
        a.parent_shop_id,
        a.parent_shop_code,
        a.parent_shop_name,
        a.SHOP_PATH,
        a.SHOP_ID,
        'KIT'                                                stock_type_name,
        po.CODE                                              stock_model_code,
        po.name                                              stock_model_name,
        MAX(to_number_serial) - MIN(to_number_serial) + 1 as quantity,
        cast(MIN(to_number_serial) as char)                  from_serial,
        cast(MAX(to_number_serial) as char)                  to_serial,
        (select osv.name
         from bccs3_inventory_la.option_set os,
              bccs3_inventory_la.option_set_value osv
         where os.code = 'GOODS_STATE'
           and os.id = osv.option_set_id
           and osv.VALUE = cast(a.state_id as CHAR)
           and osv.status = 1
           and os.status = 1)                                state_name,
        NOW()                                                create_date
 from bccs3_inventory_la.product_offering po,
      (select s.SHOP_CODE                            shop_code,
              s.name                                 shop_name,
              s.parent_shop_id,
              s.parent_shop_code                     parent_shop_code,
              s.parent_shop_name                     parent_shop_name,
              s.SHOP_PATH                            SHOP_PATH,
              s.SHOP_ID                              shop_id,
              ss.prod_offer_id,
              state_id,
              cast(DATE(ss.create_date) as DATETIME) create_date,
              ss.contract_code,
              ss.to_number_serial,
              ss.to_number_serial -
              row_number() over (
                  order by
                      s.shop_id,ss.PROD_OFFER_ID,
                      ss.to_number_serial)           rn
       from (select s.*, sh.SHOP_CODE parent_shop_code, sh.NAME parent_shop_name
             from bccs3_inventory_la.shop s
                      LEFT JOIN bccs3_catalog_la.shop sh
                                on sh.parent_shop_id =
                                   2 and
                                   sh.shop_code like
                                   'BOP%' and
                                   s.shop_path like
                                   CONCAT(sh.shop_path, '_%')) s,
            bccs3_inventory_la.stock_kit ss
       where s.shop_id = ss.OWNER_ID
         and ss.owner_type = 1
         and s.STATUS = 1
         and ss.status = 1
         and s.shop_id not in (35387, 30885)
         and s.shop_id <> 2) a
 where po.PROD_OFFER_ID = a.prod_offer_id
   and po.STATUS <> 0
 group by a.shop_name,
          po.code,
          po.ACCOUNTING_MODEL_CODE,
          po.name,
          a.state_id,
          a.rn
 order by a.shop_name, po.code, a.state_id)
union all
(select s.name,
        s.SHOP_CODE           shop_name,
        s.PARENT_SHOP_ID,
        s.parent_shop_code,
        s.parent_shop_name,
        s.SHOP_PATH,
        s.SHOP_ID,
        'HANDSET'             stock_type_name,
        po.CODE               stock_model_code,
        po.NAME               stock_model_name,
        1                     quantity,
        sh.serial             from_serial,
        sh.serial             to_serial,
        (select osv.name
         from bccs3_inventory_la.option_set os,
              bccs3_inventory_la.option_set_value osv
         where os.code = 'GOODS_STATE'
           and os.id = osv.option_set_id
           and osv.VALUE = cast(sh.state_id as CHAR)
           and osv.status = 1
           and os.status = 1) state_name,
        NOW()                 create_date
 from (select s.*, sh.SHOP_CODE parent_shop_code, sh.NAME parent_shop_name
       from bccs3_inventory_la.shop s
                LEFT JOIN bccs3_catalog_la.shop sh
                          on sh.parent_shop_id =
                             2 and sh.shop_code like
                                   'BOP%' and
                             s.shop_path like
                             CONCAT(sh.shop_path, '_%')) s,
      bccs3_inventory_la.product_offering po,
      bccs3_inventory_la.stock_handset sh
 where 1 = 1
   and s.shop_id = sh.owner_id
   and sh.owner_type = 1
   and sh.status = 1
   and s.status = 1
   and po.STATUS = 1
   and s.shop_id not in (35387, 30885)
   and po.PROD_OFFER_ID = sh.prod_offer_id
   and s.shop_id <> 2
 order by s.shop_code, po.code, sh.state_id);

select *
from bccs3_inventory_la.sync_serial_inventory_report
where month(sync_date) = month(current_date())
  and year(sync_date) = year(current_date())
limit 10








