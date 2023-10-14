UPDATE
    {table_name}
SET
    netWasteTradeMT = (
        CASE
            WHEN netWasteTradeMT < 0 THEN 0
            ELSE netWasteTradeMT
        END
    ),
    netWasteTradePercent = (
        CASE
            WHEN netWasteTradePercent < 0 THEN 0
            ELSE netWasteTradePercent
        END
    )
WHERE
    region = 'china'
    AND year > 2020