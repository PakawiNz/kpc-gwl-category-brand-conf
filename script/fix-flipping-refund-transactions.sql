-- SELECT COUNT(1) FROM public."RefundSalesTransaction" rst WHERE rst."revokeAccumSpendableAmount" > 0

SELECT mem."id",
       (mem."accumulateSpending" - (st_sum.acm + COALESCE(neg_st_sum.acm, 0) + COALESCE(valid_rst_sum.acm, 0))) as diff,
       mem."accumulateSpending" as currentAccum,
       (st_sum.acm + COALESCE(neg_st_sum.acm, 0) + COALESCE(valid_rst_sum.acm, 0)) as expectedAccum,
       tier."name" as currentTier,
        min_tier."name" as minTier,

    (SELECT tier."name"
     FROM public."Tier" tier
     WHERE tier."minimumSpending" <= (st_sum.acm + COALESCE(neg_st_sum.acm, 0) + COALESCE(valid_rst_sum.acm, 0))
     ORDER BY tier."minimumSpending" DESC
     LIMIT 1) as expectedTier,

    (SELECT tier."name"
     FROM public."Tier" tier
     WHERE tier."minimumSpending" <= mem."accumulateSpending"
     ORDER BY tier."minimumSpending" DESC
     LIMIT 1) as willBeTier,
        mem."upgradeGroupCode",
        mem."upgradeReasonCode",
       cob.count as coBrand,
       cob2.count as legacyCoBrand 
       
-- COUNT(1),
 -- SUM(mem."accumulateSpending") as accum,
 -- SUM(st_sum.acm) as sale_transaction_sum,
 -- SUM(COALESCE(neg_st_sum.acm, 0)) as neg_sale_transaction_sum,
 -- SUM(rst_sum.acm) as refund_transaction_sum,
 -- SUM(COALESCE(valid_rst_sum.acm, 0)) as valid_refund_transaction_sum
FROM public."Member" mem
INNER JOIN
    (SELECT st."memberId",
            SUM(st."totalAccumSpendableAmount") as acm
     FROM public."SalesTransaction" st
     WHERE st."totalAccumSpendableAmount" > 0
         AND st."completedAt" >= '2023-06-30 17:00'
     GROUP BY st."memberId") st_sum ON st_sum."memberId" = mem."id"
LEFT JOIN
    (SELECT st."memberId",
            SUM(st."totalAccumSpendableAmount") as acm
     FROM public."SalesTransaction" st
     WHERE st."totalAccumSpendableAmount" < 0
         AND st."completedAt" >= '2023-06-30 17:00'
     GROUP BY st."memberId") neg_st_sum ON neg_st_sum."memberId" = mem."id"
INNER JOIN
    (SELECT rst."memberId",
            SUM(rst."revokeAccumSpendableAmount") as acm
     FROM public."RefundSalesTransaction" rst
     WHERE rst."revokeAccumSpendableAmount" > 0
         AND rst."refundedAt" >= '2023-06-30 17:00'
     GROUP BY rst."memberId") rst_sum ON rst_sum."memberId" = mem."id"
LEFT JOIN
    (SELECT rst."memberId",
            SUM(rst."revokeAccumSpendableAmount") as acm
     FROM public."RefundSalesTransaction" rst
     WHERE rst."revokeAccumSpendableAmount" < 0
         AND rst."refundedAt" >= '2023-06-30 17:00'
     GROUP BY rst."memberId") valid_rst_sum ON valid_rst_sum."memberId" = mem."id"
LEFT JOIN
    (SELECT cob."memberId",
            COUNT(0) as count
     FROM public."MemberCoBrandCard" cob
     WHERE cob."status" = 'ACTIVE'
     GROUP BY cob."memberId") cob ON cob."memberId" = mem."id"
LEFT JOIN
    (SELECT cob."memberId",
            COUNT(0) as count
     FROM public."MemberLegacyCoBrandHistory" cob
     WHERE cob."cardStatus" = 'ACTIVE'
     GROUP BY cob."memberId") cob2 ON cob2."memberId" = mem."id"
INNER JOIN public."Tier" tier ON tier."id" = mem."tierId" 
INNER JOIN public."Tier" min_tier ON min_tier."id" = mem."minimumTierId" 
-- WHERE NOT (mem."upgradeGroupCode" = 'CO_BRAND' AND tier."name" = 'CROWN') AND NOT (tier."name" = 'CRYSTAL')
ORDER BY diff DESC 
-- LIMIT 4000