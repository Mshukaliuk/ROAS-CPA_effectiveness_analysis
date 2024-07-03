WITH 
CTE_CPA AS (
SELECT 
    Company,
    Campaign_name,
    Bidding_strategy,
    Cost_28d_usd,
    Target_28d,
    Actual_28d,
    
    ROUND(Target_28d-Actual_28d,2) AS efficiency 
FROM (
    SELECT
        division_name AS  company,
        campaign_name AS campaign_name,
        'tCPA' AS Bidding_strategy,
        total_28d_cost_usd AS Cost_28d_usd,
        advertising_channel_type,
        billing_category,

        ROUND(1e-6 * SAFE_DIVIDE(
        SUM(
          LocalCostMicrosForCostMicrosWeightedTargetCpa_28d
          * quarterly_fx_value_usd)
          + SUM(
            LocalCostMicrosForPConvsWeightedTargetCpa_28d
            * quarterly_fx_value_usd),
        SUM(LocalCostMicrosWeightedSumInverseTargetCpa_28d)
          + SUM(LocalCostMicrosForPConvsWeightedTargetCpa_28d)
            * IFNULL(
              SAFE_DIVIDE(
                SUM(LocalPConvsForPConvsWeightedTargetCpa_28d),
                SUM(LocalPConvsWeightedSumTargetCpa_28d)),
              0))
              ,2)
               AS Target_28d,

      ROUND(SAFE_DIVIDE(SUM(tcpa_28d_cost_usd), SUM(tcpa_28d_conversions)),2)
        AS Actual_28d

    FROM
        pdw.prod.search.GrowthReadyDashboard_tCPAMonitor.CampaignStats_Latest_DSF
        AS _t
    GROUP BY 1, 2, 3, 4, 5, 6
) AS derived_table
WHERE
    ((advertising_channel_type) IN ("SEARCH", "SHOPPING", "PMAX"))
    AND (billing_category = "Billable")
    AND (company IN ("Havehandel.dk", "Prishammeren", "BMD Trading"))
    AND (Target_28d > 0)

GROUP BY
    Company,
    Campaign_name,
    Target_28d,
    Actual_28d,
    Cost_28d_usd,
    Bidding_strategy, advertising_channel_type, billing_category
    
    HAVING efficiency > 0	)
,
CTE_ROAS AS (
          
    SELECT 
    Company,
    Campaign_name,
    Bidding_strategy,
    Target_28d,
    Actual_28d,
    Cost_28d_usd,
    ROUND(Actual_28d-Target_28d,2) AS efficiency 
FROM (
    SELECT
        division_name AS  company,
        campaign_name AS campaign_name,
        'tROAS' AS bidding_strategy,
        total_28d_cost_usd AS Cost_28d_usd,
        advertising_channel_type,
        billing_category,

        ROUND(
        SAFE_DIVIDE(SUM(troas_28d_conv_value), SUM(troas_28d_cost)), 
        2)
         AS Actual_28d,

       ROUND(
        IFNULL(
        SAFE_DIVIDE(
          SUM(LocalCostMicrosWeightedSumTargetRoas_28d)
            + SUM(LocalCostMicrosForPValueWeightedTargetRoas_28d)
              * IFNULL(
                SAFE_DIVIDE(
                  SUM(LocalPValueForPValueWeightedTargetRoas_28d),
                  SUM(LocalPValueWeightedSumInverseTargetRoas_28d)),
                0),
          SUM(LocalCostMicrosForPValueWeightedTargetRoas_28d)
            + SUM(LocalCostMicrosForCostMicrosWeightedTargetRoas_28d)),
        0),
        2) AS Target_28d,
     FROM
      pdw.prod.search.GrowthReadyDashboard_tROASMonitor.CampaignStats_Latest_DSF
        AS _t
    GROUP BY 1, 2, 3, 4, 5, 6
) AS derived_table
WHERE
    ((advertising_channel_type) IN ("SEARCH", "SHOPPING", "PMAX"))
    AND (billing_category = "Billable")
    AND (company IN ("Havehandel.dk", "Prishammeren", "BMD Trading"))
    AND (Target_28d > 0)

GROUP BY
    Company,
    Campaign_name,
    Target_28d,
    Actual_28d,
    Cost_28d_usd,
    bidding_strategy, advertising_channel_type, billing_category
    
    HAVING efficiency > 0
)


SELECT 
Company,
Campaign_name,
Bidding_strategy,
Target_28d,
Actual_28d,
Efficiency,
ROUND(Cost_28d_usd,2) AS Cost_28d_usd
FROM CTE_CPA UNION ALL 
SELECT 
Company,
Campaign_name,
Bidding_strategy,
Target_28d,
Actual_28d,
Efficiency,
ROUND(Cost_28d_usd,2) AS Cost_28d_usd
FROM CTE_ROAS
ORDER BY efficiency DESC;

