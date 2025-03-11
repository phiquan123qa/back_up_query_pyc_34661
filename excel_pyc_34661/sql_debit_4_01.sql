INSERT INTO bccs3_inventory_la.sync_debit_staff_rp(PARENT_SHOP_NAME, PARENT_SHOP_CODE_GROUP, SHOP_ID, SHOP_CODE,
                                                   SHOP_NAME, STAFF_ID, STAFF_CODE, STAFF_NAME, SHOP_PATH,
                                                   STAFF_ID_DEPT, SHOP_ID_DEPT, OPENING_DEPT,
                                                   REVENUE_OF_SALE_DETAIL, REVENUE_SALE_PENDING,
                                                   POST_PAID, SALE_DEALER,
                                                   OTHER_REVENUE, RECEIVE_CASH_FROM_STAFF, DEPOSIT_ROAMING,
                                                   SALE_PENALTY, REVENUE_SHOP_NO_BACK, MONEY_NO_TRANSFER,
                                                   UMONEY_TRANSFER, MONEY_TRANSFER_NOT_CREDIT, CASH_ON_HAND,
                                                   OTHER_DEDUCE, TOTAL_DEPT, TOTAL_DEDUCE, CLOSING_DEPT,
                                                   CLOSING_DEPT_AFTER, SYNC_DATE, PERIOD_REPORT)
    (select *
     from (select ifnull((select concat(sh1.shop_code, ' - ', sh1.NAME)
                          from bccs3_catalog_la.shop sh1
                          where sh1.shop_id = sh.parent_shop_id), ' - Unknown')                            parentShopName,
                  ifnull((select sh1.shop_code from bccs3_catalog_la.shop sh1 where sh1.shop_id = sh.parent_shop_id),
                         ' - Unknown')                                                                     parent_shop_code_group,
                  sh.shop_id                                                                               shopId,
                  sh.shop_code                                                                             shopCode,
                  ifnull(sh.name, ' - Unknown')                                                            shopName,
                  s.staff_id                                                                               staffId,
                  s.staff_code                                                                             staffCode,
                  concat(sh.shop_code, ' | ', s.staff_code, ' - ', s.name)                                 staffName,
                  sh.shop_path,
                  b.*,
                  (b.revenueOfSaleDetail + b.postPaid + b.otherRevenue + b.receiveCashFromStaff + b.depositRoaming +
                   b.salePenalty)                                                                          totalDebt,
                  (b.moneyTransfer + b.umoneyTransfer + b.moneyTransferNotCredit + b.cashOnHand + b.revenueShopNoBank +
                   b.otherDeduce)                                                                          totalDeduce,
                  (b.openingDebt + b.revenueOfSaleDetail + b.postPaid + b.otherRevenue + b.receiveCashFromStaff +
                   b.depositRoaming + b.salePenalty - (b.moneyTransfer + b.umoneyTransfer + b.cashOnHand)) closingDebt,
                  (b.openingDebt + b.revenueOfSaleDetail + b.postPaid + b.otherRevenue + b.receiveCashFromStaff +
                   b.depositRoaming + b.salePenalty -
                   (b.moneyTransfer + b.umoneyTransfer + b.cashOnHand + b.moneyTransferNotCredit + b.revenueShopNoBank +
                    b.otherDeduce))                                                                        closingDebtAfter,
                  NOW()                                                                                    syncDate,
                  str_to_date(:period_report, '%Y%m%d')                                                    periodReport
           from (select a.staff_id_debt,
                        a.shop_id_debt,
                        SUM(debit_remain)             openingDebt,
                        SUM(revenue_sale)             revenueOfSaleDetail,
                        SUM(revenue_sale_pending)     revenueOfSalePending,
                        SUM(post_paid)                postPaid,
                        SUM(sale_dealer)              saleDealer,
                        SUM(revenue_other)            otherRevenue,
                        SUM(receive_cash)             receiveCashFromStaff,
                        SUM(deposit_roaming)          depositRoaming,
                        SUM(fines_sale)               salePenalty,
                        SUM(revenue_shop)             revenueShopNoBank,
                        SUM(money_transer)            moneyTransfer,
                        SUM(umoney_transer)           umoneyTransfer,
                        SUM(money_transer_not_credit) moneyTransferNotCredit,
                        SUM(cash_hand)                cashOnHand,
                        SUM(reduce_other)             otherDeduce
                 from (select dr.shop_id                 as shop_id_debt,
                              dr.staff_id                as staff_id_debt,
                              ifnull(dr.debit_amount, 0) as debit_remain,
                              0                          as revenue_sale,
                              0                          as revenue_sale_pending,
                              0                          as post_paid,
                              0                          as sale_dealer,
                              0                          as receive_cash,
                              0                          as fines_sale,
                              0                          as deposit_roaming,
                              0                          as revenue_other,
                              0                          as revenue_shop,
                              0                          as money_transer,
                              0                          as umoney_transer,
                              0                          as money_transer_not_credit,
                              0                          as cash_hand,
                              0                          as reduce_other
                       from bccs3_sale_trans_la.debit_remain dr
                       where dr.debit_remain_date =
                             DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
                       union all
                       select dt.shop_id     as shop_id_debt,
                              dt.staff_id    as staff_id_debt,
                              0              as debit_remain,
                              sum(dt.AMOUNT) as revenue_sale,
                              0              as revenue_sale_pending,
                              0              as post_paid,
                              0              as sale_dealer,
                              0              as receive_cash,
                              0              as fines_sale,
                              0              as deposit_roaming,
                              0              as revenue_other,
                              0              as revenue_shop,
                              0              as money_transer,
                              0              as umoney_transer,
                              0              as money_transer_not_credit,
                              0              as cash_hand,
                              0              as reduce_other
                       from bccs3_sale_trans_la.debit_transaction dt
                       where dt.debit_type = '1'
                         and dt.status = 1
                         and dt.revenue_type in ('1', '3')
                         and dt.DEBIT_TRANS_DATE >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
                         and dt.DEBIT_TRANS_DATE <
                             DATE_FORMAT(:date, '%Y%m%d')
                       group by dt.shop_id, dt.staff_id
                       union all
                       select dt.shop_id     as shop_id_debt,
                              dt.staff_id    as staff_id_debt,
                              0              as debit_remain,
                              0              as revenue_sale,
                              0              as revenue_sale_pending,
                              sum(dt.AMOUNT) as post_paid,
                              0              as sale_dealer,
                              0              as receive_cash,
                              0              as fines_sale,
                              0              as deposit_roaming,
                              0              as revenue_other,
                              0              as revenue_shop,
                              0              as money_transer,
                              0              as umoney_transer,
                              0              as money_transer_not_credit,
                              0              as cash_hand,
                              0              as reduce_other
                       from bccs3_sale_trans_la.debit_transaction dt
                       where dt.debit_type = '1'
                         and dt.status = 1
                         and dt.revenue_type in ('2')
                         and dt.DEBIT_TRANS_DATE >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
                         and dt.DEBIT_TRANS_DATE <
                             DATE_FORMAT(:date, '%Y%m%d')
                       group by dt.shop_id, dt.staff_id
                       union all
                       select dt.shop_id     as shop_id_debt,
                              dt.staff_id    as staff_id_debt,
                              0              as debit_remain,
                              0              as revenue_sale,
                              0              as revenue_sale_pending,
                              0              as post_paid,
                              0              as sale_dealer,
                              0              as receive_cash,
                              0              as fines_sale,
                              0              as deposit_roaming,
                              sum(dt.AMOUNT) as revenue_other,
                              0              as revenue_shop,
                              0              as money_transer,
                              0              as umoney_transer,
                              0              as money_transer_not_credit,
                              0              as cash_hand,
                              0              as reduce_other
                       from bccs3_sale_trans_la.debit_transaction dt
                       where dt.debit_type = '1'
                         and dt.status = 1
                         and dt.revenue_type not in ('1', '2', '3', '5', '6', '7')
                         and dt.DEBIT_TRANS_DATE >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
                         and dt.DEBIT_TRANS_DATE <
                             DATE_FORMAT(:date, '%Y%m%d')
                       group by dt.shop_id, dt.staff_id
                       union all
                       select dt.shop_id     as shop_id_debt,
                              dt.staff_id    as staff_id_debt,
                              0              as debit_remain,
                              0              as revenue_sale,
                              0              as revenue_sale_pending,
                              0              as post_paid,
                              0              as sale_dealer,
                              sum(dt.AMOUNT) as receive_cash,
                              0              as fines_sale,
                              0              as deposit_roaming,
                              0              as revenue_other,
                              0              as revenue_shop,
                              0              as money_transer,
                              0              as umoney_transer,
                              0              as money_transer_not_credit,
                              0              as cash_hand,
                              0              as reduce_other
                       from bccs3_sale_trans_la.debit_transaction dt
                       where dt.debit_type = '1'
                         and dt.status = 1
                         and dt.revenue_type in ('5')
                         and dt.DEBIT_TRANS_DATE >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
                         and dt.DEBIT_TRANS_DATE <
                             DATE_FORMAT(:date, '%Y%m%d')
                       group by dt.shop_id, dt.staff_id
                       union all
                       select dt.shop_id     as shop_id_debt,
                              dt.staff_id    as staff_id_debt,
                              0              as debit_remain,
                              0              as revenue_sale,
                              0              as revenue_sale_pending,
                              0              as post_paid,
                              0              as sale_dealer,
                              0              as receive_cash,
                              0              as fines_sale,
                              sum(dt.AMOUNT) as deposit_roaming,
                              0              as revenue_other,
                              0              as revenue_shop,
                              0              as money_transer,
                              0              as umoney_transer,
                              0              as money_transer_not_credit,
                              0              as cash_hand,
                              0              as reduce_other
                       from bccs3_sale_trans_la.debit_transaction dt
                       where dt.debit_type = '1'
                         and dt.status = 1
                         and dt.revenue_type in ('7')
                         and dt.DEBIT_TRANS_DATE >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
                         and dt.DEBIT_TRANS_DATE <
                             DATE_FORMAT(:date, '%Y%m%d')
                       group by dt.shop_id, dt.staff_id
                       union all
                       select dt.shop_id     as shop_id_debt,
                              dt.staff_id    as staff_id_debt,
                              0              as debit_remain,
                              0              as revenue_sale,
                              0              as revenue_sale_pending,
                              0              as post_paid,
                              0              as sale_dealer,
                              0              as receive_cash,
                              0              as fines_sale,
                              0              as deposit_roaming,
                              0              as revenue_other,
                              0              as revenue_shop,
                              sum(dt.AMOUNT) as money_transer,
                              0              as umoney_transer,
                              0              as money_transer_not_credit,
                              0              as cash_hand,
                              0              as reduce_other
                       from bccs3_sale_trans_la.debit_transaction dt
                       where dt.debit_type = '2'
                         and dt.status = 1
                         and dt.REDUCE_TYPE in ('2')
                         and dt.DEBIT_TRANS_DATE >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
                         and dt.DEBIT_TRANS_DATE <
                             DATE_FORMAT(:date, '%Y%m%d')
                       group by dt.shop_id, dt.staff_id
                       union all
                       select dt.shop_id     as shop_id_debt,
                              dt.staff_id    as staff_id_debt,
                              0              as debit_remain,
                              0              as revenue_sale,
                              0              as revenue_sale_pending,
                              0              as post_paid,
                              0              as sale_dealer,
                              0              as receive_cash,
                              0              as fines_sale,
                              0              as deposit_roaming,
                              0              as revenue_other,
                              0              as revenue_shop,
                              0              as money_transer,
                              sum(dt.AMOUNT) as umoney_transer,
                              0              as money_transer_not_credit,
                              0              as cash_hand,
                              0              as reduce_other
                       from bccs3_sale_trans_la.debit_transaction dt
                       where dt.debit_type = '2'
                         and dt.status = 1
                         and dt.REDUCE_TYPE in ('8')
                         and dt.DEBIT_TRANS_DATE >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
                         and dt.DEBIT_TRANS_DATE <
                             DATE_FORMAT(:date, '%Y%m%d')
                       group by dt.shop_id, dt.staff_id
                       union all
                       select br.shop_id     as shop_id_debt,
                              br.staff_id    as staff_id_debt,
                              0              as debit_remain,
                              0              as revenue_sale,
                              0              as revenue_sale_pending,
                              0              as post_paid,
                              0              as sale_dealer,
                              0              as receive_cash,
                              0              as fines_sale,
                              0              as deposit_roaming,
                              0              as revenue_other,
                              0              as revenue_shop,
                              0              as money_transer,
                              0              as umoney_transer,
                              sum(br.AMOUNT) as money_transer_not_credit,
                              0              as cash_hand,
                              0              as reduce_other
                       from bccs3_sale_trans_la.bank_receipt br
                       where br.receipt_status = 1
                         and br.org_bank_date >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
                         and br.org_bank_date < DATE_FORMAT(:date, '%Y%m%d')
                       group by br.shop_id, br.staff_id
                       union all
                       select dt.shop_id     as shop_id_debt,
                              dt.staff_id    as staff_id_debt,
                              0              as debit_remain,
                              0              as revenue_sale,
                              0              as revenue_sale_pending,
                              0              as post_paid,
                              0              as sale_dealer,
                              0              as receive_cash,
                              0              as fines_sale,
                              0              as deposit_roaming,
                              0              as revenue_other,
                              0              as revenue_shop,
                              0              as money_transer,
                              0              as umoney_transer,
                              0              as money_transer_not_credit,
                              sum(dt.AMOUNT) as cash_hand,
                              0              as reduce_other
                       from bccs3_sale_trans_la.debit_transaction dt
                       where dt.debit_type = '2'
                         and dt.status = 1
                         and dt.REDUCE_TYPE in ('4')
                         and dt.DEBIT_TRANS_DATE >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
                         and dt.DEBIT_TRANS_DATE <
                             DATE_FORMAT(:date, '%Y%m%d')
                       group by dt.shop_id, dt.staff_id
                       union all
                       select dt.shop_id     as shop_id_debt,
                              dt.staff_id    as staff_id_debt,
                              0              as debit_remain,
                              0              as revenue_sale,
                              0              as revenue_sale_pending,
                              0              as post_paid,
                              0              as sale_dealer,
                              0              as receive_cash,
                              0              as fines_sale,
                              0              as deposit_roaming,
                              0              as revenue_other,
                              0              as revenue_shop,
                              0              as money_transer,
                              0              as umoney_transer,
                              0              as money_transer_not_credit,
                              0              as cash_hand,
                              sum(dt.AMOUNT) as reduce_other
                       from bccs3_sale_trans_la.debit_transaction dt
                       where dt.debit_type = '2'
                         and dt.status = 1
                         and dt.REDUCE_TYPE not in ('1', '2', '3', '4', '8')
                         and dt.DEBIT_TRANS_DATE >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
                         and dt.DEBIT_TRANS_DATE <
                             DATE_FORMAT(:date, '%Y%m%d')
                       group by dt.shop_id, dt.staff_id
                       union all
                       select dpt.shop_id     as shop_id_debt,
                              dpt.staff_id    as staff_id_debt,
                              0               as debit_remain,
                              0               as revenue_sale,
                              sum(dpt.AMOUNT) as revenue_sale_pending,
                              0               as post_paid,
                              0               as sale_dealer,
                              0               as receive_cash,
                              0               as fines_sale,
                              0               as deposit_roaming,
                              0               as revenue_other,
                              0               as revenue_shop,
                              0               as money_transer,
                              0               as umoney_transer,
                              0               as money_transer_not_credit,
                              0               as cash_hand,
                              0               as reduce_other
                       from bccs3_sale_trans_la.debit_pending_transaction dpt
                       where dpt.status = 1
                         and dpt.revenue_type = 1
                         and dpt.created_date >= DATE_FORMAT(DATE_SUB(:date, INTERVAL 1 DAY), '%Y%m%d')
                         and dpt.created_date < DATE_FORMAT(:date, '%Y%m%d')
                       group by dpt.shop_id, dpt.staff_id) a
                 group by a.staff_id_debt, a.shop_id_debt) b,
                bccs3_catalog_la.staff s,
                bccs3_catalog_la.shop sh
           where s.staff_id = b.staff_id_debt
             and sh.shop_id = b.shop_id_debt
             and (b.openingDebt <> 0 or b.revenueOfSaleDetail <> 0 or b.saleDealer <> 0 or b.postPaid <> 0 or
                  b.otherRevenue <> 0 or b.receiveCashFromStaff <> 0 or b.depositRoaming <> 0 or b.salePenalty <> 0 or
                  b.revenueShopNoBank <> 0 or b.moneyTransfer <> 0 or b.umoneyTransfer <> 0 or
                  b.moneyTransferNotCredit <> 0 or b.cashOnHand <> 0 or b.otherDeduce <> 0)) c
     order by c.parent_shop_code_group, c.shopCode, c.staffCode)