-- 

SELECT 
    wb."memberId",
    wb."walletCode",
    wb."expiredAt",
    wb.amount
FROM public."WalletBalance" wb 
WHERE wb."memberId" = '01JYB8AY4FT0JJR23DCV8DX6HQ'

-- 
SELECT 
    wt."memberId",
    wt."walletCode",
    wt."expiredAt",
    COALESCE(SUM(wt.amount), 0) as total_amount
FROM public."WalletTransaction" wt 
WHERE wt."memberId" = '01JYB8AY4FT0JJR23DCV8DX6HQ'
AND wt."walletCode" = 'CARAT_WALLET'
GROUP BY wt."memberId", wt."walletCode", wt."expiredAt";

-- identify wallet transaction which has futher expired date than its wallet balance
SELECT wt.id,
       wt."memberId",
       wt."balanceId",
       wt."walletCode",
       wt."expiredAt" as trxExpiry,
       wb."expiredAt" as balExpiry,

    (SELECT wb2."expiredAt"
     FROM point_service."WalletBalance" as wb2
     WHERE wb."memberId" = wb2."memberId"
         AND wb2."expiredAt" >= wt."expiredAt" 
         AND wb2."walletCode" = 'CARAT_WALLET'
     ORDER BY "expiredAt"
     LIMIT 1) as balNext,

    (SELECT wb2."id"
     FROM point_service."WalletBalance" as wb2
     WHERE wb."memberId" = wb2."memberId"
         AND wb2."expiredAt" >= wt."expiredAt" 
         AND wb2."walletCode" = 'CARAT_WALLET'
     ORDER BY "expiredAt"
     LIMIT 1) as balNextId,

    wt.amount as trxAamount,
    wb.amount as balAamount

FROM point_service."WalletTransaction" as wt
INNER JOIN point_service."WalletBalance" as wb ON wt."balanceId" = wb.id
WHERE wb."expiredAt" <> wt."expiredAt"
AND wb."walletCode" = 'CARAT_WALLET'
ORDER BY balNext DESC
LIMIT 1000


-- identify unmatched wallet balance
SELECT 
    wb."memberId",
    wb."walletCode" as code,
    wb.sumAmount as balance_amount,
    wt.sumAmount as transaction_total
FROM (
    SELECT 
        wb."memberId", 
        wb."walletCode",
        COALESCE(SUM(wb.amount), 0) as sumAmount
    FROM public."WalletBalance" wb
    WHERE wb."expiredAt" > NOW()
    GROUP BY wb."memberId", wb."walletCode"
) wb
LEFT JOIN (
    SELECT 
        wt."memberId", 
        wt."walletCode",
        COALESCE(SUM(wt.amount), 0) as sumAmount,
        wt.id
    FROM public."WalletTransaction" wt
    WHERE wt."expiredAt" > NOW()
    GROUP BY wt."memberId", wt."walletCode"
) wt 
ON wb."memberId" = wt."memberId" 
AND wb."walletCode" = wt."walletCode"
-- LIMIT 1000;


SELECT * FROM point_service."WalletBalance" as wb WHERE wb."memberId" IN ('01JYB8AY5Q9NX0WQF1Y9RXJJJV')