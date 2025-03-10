WITH CustomerStats AS (
    -- Aggregate statistics by country
    SELECT
        "Country",
        COUNT(*) AS TotalCustomers,
        AVG(DATEDIFF('day', "Subscription Date", CURRENT_DATE)) AS AvgDaysSinceSubscription
    FROM read_file('customers.csv')
    GROUP BY "Country"
),
     RankedCustomers AS (
         -- Rank customers per country by most recent subscription date
         SELECT
             "Customer Id",
             "First Name",
             "Last Name",
             "Country",
             "Subscription Date",
             ROW_NUMBER() OVER (PARTITION BY "Country" ORDER BY "Subscription Date" DESC) AS RankPerCountry
         FROM read_file('customers.csv')
         WHERE "Email" LIKE '%@%'
     ),
     UnionQuery AS (
         -- Combine two sets of customer selections: those with a company specified and those in cities starting with 'New'
         SELECT
             "Customer Id",
             "First Name",
             "Last Name",
             "Country",
             "Subscription Date"
         FROM read_file('customers.csv')
         WHERE "Company" IS NOT NULL
         UNION
         SELECT
             "Customer Id",
             "First Name",
             "Last Name",
             "Country",
             "Subscription Date"
         FROM read_file('customers.csv')
         WHERE "City" LIKE 'New%'
     )
SELECT
    rc."Customer Id",
    rc."First Name",
    rc."Last Name",
    rc."Country",
    cs.TotalCustomers,
    cs.AvgDaysSinceSubscription,
    rc."Subscription Date",
    CASE
        WHEN rc.RankPerCountry = 1 THEN 'Most recent subscriber'
        WHEN rc.RankPerCountry <= 10 THEN 'Top 10 recent'
        ELSE 'Regular'
        END AS SubscriptionRank
FROM RankedCustomers rc
         JOIN CustomerStats cs ON rc."Country" = cs."Country"
WHERE rc.RankPerCountry <= 10
ORDER BY cs.TotalCustomers DESC, rc."Subscription Date" DESC;
