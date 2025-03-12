# ALTER TABLE bccs3_inventory_la.sync_serial_inventory_report
# ADD UNIQUE KEY unique_inventory_uk (PARENT_SHOP_ID, SHOP_ID, INVENTORY_TYPE, INVENTORY_CODE, STATUS);

# ALTER TABLE bccs3_inventory_la.sync_serial_inventory_report
# CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

# UPDATE bccs3_inventory_la.sync_serial_inventory_report
# SET STATUS = CASE
#     WHEN STATUS IN ('New', 'Mới') THEN 1
#     WHEN STATUS IN ('Revoked', 'Bảo Hành') THEN 2
#     WHEN STATUS IN ('Damaged', 'Hỏng') THEN 3
#     WHEN STATUS IN ('Missing', 'Hàng mất') THEN 4
#     WHEN STATUS IN ('Lock', 'Khóa') THEN 5
#     WHEN STATUS IN ('Sale Punish', 'Phạt') THEN 6
#     ELSE 0
# END
# WHERE STATUS IS NOT NULL;

UPDATE bccs3_inventory_la.sync_serial_inventory_report SET SYNC_DATE = STR_TO_DATE('2025-02-01', '%Y-%m-%d') WHERE SYNC_DATE IS NOT NULL;


select * from bccs3_inventory_la.shop s
select * from bccs3_catalog_la.shop
select * from bccs3_report_la.rp_offline_inventory where created_datetime >= STR_TO_DATE('2025-01-01', '%Y-%m-%d')


select
	CONCAT(s.SHOP_CODE,' - ',IFNULL(s.NAME,s.SHOP_CODE)) as name,
	s.shop_id as shopId,
	s.shop_path
from
	bccs3_catalog_la.shop s
where
	status = 1
	and (parent_shop_id = 2 and shop_code like 'BOP%' or shop_code = 'STT')
# 	and (s.shop_path like CONCAT('%\\_', ? , '%') or s.shop_id = ?)
order by name ASC

SELECT concat(s.SHOP_CODE, ' - ', s.NAME) as SHOP_CODE,
s.shop_id as SHOP_ID
FROM shop s
WHERE s.status = 1
# and (shop_Path like CONCAT('%\\_',?,'\\_%') or shop_Id= ?)
order by s.SHOP_CODE ASC;

    -- Lấy ngày đầu và cuối của tháng trước
select DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), '%Y-%m-01'),
       LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH));

DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), '%Y-%m-01')
       LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH));


delete from bccs3_inventory_la.sync_revenue_staff_rp where PERIOD_REPORT <= STR_TO_DATE('2025-03-12', '%Y-%m-%d')


