-- Main query to gather travel metrics and behavior indices of users from TravelTide
-- This query employs CTEs for each of its logical components.

-- Using COALESCE() to handle potential NULL values and ensure accurate calculations.
-- NULLIF is used to prevent division by zero errors.

-- CTE: UserSessions
-- Selects users who have more than 7 sessions starting from '2023-01-04'
-- Purpose is to filter only active users based on a threshold.
WITH UserSessions AS (
  SELECT user_id
  FROM sessions
  WHERE session_start >= '2023-01-04'  
  GROUP BY user_id
  HAVING COUNT(session_id) > 7
),
-- CTE: UserTravelSpendSummary
-- Aggregates financial metrics related to user's spending on hotels and flights.
-- COALESCE is used to handle NULLs and replace them with zeroes for aggregate sums.
UserTravelSpendSummary AS (
  SELECT
    u.user_id,
    -- Average Daily Spend (ADS) on hotels: Calculated as average of (discount * per_room_cost * rooms).
    -- Note: Assuming that hotel_discount_amount is a multiplier.
    AVG(s.hotel_discount_amount * (h.hotel_per_room_usd * h.rooms)) AS ADS_hotel,
    -- Total USD spent on hotels: Summation of (per_room_cost * rooms)
    COALESCE(SUM((h.hotel_per_room_usd * h.rooms)),0) AS total_hotel_usd_spent,
    -- Total USD spent on flights: Summation of base fares.
    COALESCE(SUM(f.base_fare_usd),0) AS total_flight_usd_spent,
    -- Grand total USD spent: Summation of both hotel and flight expenses.
	-- hotel_per_room_usd according to the "Technical Team" is already an aggregated value of the total USD of hotels, based on cost per day of the hotel.
    COALESCE(SUM((h.hotel_per_room_usd * h.rooms) + f.base_fare_usd),0) AS total_usd_spent
  FROM UserSessions us
  JOIN users u ON u.user_id = us.user_id
  LEFT JOIN sessions s ON s.user_id = u.user_id
  LEFT JOIN flights f ON f.trip_id = s.trip_id
  LEFT JOIN hotels h ON h.trip_id = s.trip_id
  GROUP BY u.user_id
  ORDER BY ADS_hotel DESC
),
-- CTE: MinMaxAdsHotel
-- Calculates the minimum and maximum values of Average Daily Spend (ADS) on hotels across all users.
-- This will be used later for scaling ADS_hotel to a [0,1] range.
MinMaxAdsHotel AS (
  SELECT
    MIN(ADS_hotel) AS min_hotel_ads,
    MAX(ADS_hotel) AS max_hotel_ads
  FROM UserTravelSpendSummary 
),

-- CTE: ScaledTravelMetrics
-- Scales the ADS_hotel between 0 and 1 based on MinMax scaling technique.
-- Uses a CASE expression to handle NULL values, defaulting them to 0.
ScaledTravelMetrics AS (
  SELECT 
    utss.user_id,
    utss.ADS_hotel,
    CASE
      WHEN (utss.ADS_hotel - mma.min_hotel_ads) /
         (mma.max_hotel_ads - mma.min_hotel_ads) IS NULL THEN 0
      ELSE (utss.ADS_hotel - mma.min_hotel_ads) /
         (mma.max_hotel_ads - mma.min_hotel_ads)
    END AS scaled_hotel_ads
  FROM UserTravelSpendSummary utss 
  CROSS JOIN MinMaxAdsHotel mma
),
-- CTE: UserDiscountMetrics
-- The purpose of this CTE is to aggregate various metrics related to user behavior and discounts on flights and hotels.
-- Calculations are detailed to show the average and proportional metrics, and other behavior indices per user.
-- Functions like COALESCE and CASE are used to handle null values and conditional aggregation.
-- NOTE: COALESCE is employed to replace potential NULLs with zeros to ensure that the calculations are not skewed.
-- The denominators in proportions (e.g., flight_discount_proportion) could be zero, which is managed to avoid divide-by-zero errors.
UserDiscountMetrics AS (
  SELECT
    u.user_id,
    -- Average Flight Discount: The average amount of discounts received by a user for flight bookings. Nulls replaced by zero.
    COALESCE(AVG(s.flight_discount_amount),0) AS average_flight_discount,
    -- Average Hotel Discount: The average amount of discounts received by a user for hotel bookings. Nulls replaced by zero.
    COALESCE(AVG(s.hotel_discount_amount),0) AS average_hotel_discount, 
    -- Flight Discount Proportion: The proportion of trips where only a flight discount was received. 
    -- Ensures the user received a flight discount but no hotel discount.
    COALESCE(SUM(
      CASE
        WHEN s.flight_discount = 'true' AND s.flight_discount_amount > 0 AND s.hotel_discount = 'false' THEN 1 
        ELSE 0
      END
    ) :: FLOAT / COUNT(*)
    ,0) AS flight_discount_proportion,
    -- Hotel Discount Proportion: The proportion of trips where only a hotel discount was received.
    -- Ensures the user received a hotel discount but no flight discount.   
    COALESCE(SUM(
      CASE 
       WHEN hotel_discount = 'true' AND s.hotel_discount_amount > 0 AND s.flight_discount ='false' THEN 1  
       ELSE 0 
      END) :: FLOAT / COUNT(*)
    ,0) AS hotel_discount_proportion,
    -- Both Discounts Proportion: Proportion of trips where discounts on both flights and hotels were received.
     COALESCE(SUM(
      CASE
        WHEN s.flight_discount = 'true' AND s.flight_discount_amount > 0 AND s.hotel_discount = 'true' AND s.hotel_discount_amount > 0 THEN 1
        ELSE 0
      END) :: FLOAT / COUNT(*)
    ,0) AS both_discount_proportion, 
    -- Total Only Flights: Counts trips where only a flight was booked.
    COUNT(
      CASE
        WHEN s.flight_booked = 'true' AND s.hotel_booked = 'false' THEN s.trip_id
        ELSE NULL
      END
    ) AS total_only_flights,
    -- Total Only Hotels: Counts trips where only a hotel was booked.
    COUNT(
      CASE
         WHEN s.hotel_booked = 'true' AND s.flight_booked ='false' THEN s.trip_id
         ELSE NULL
      END
     ) AS total_only_hotels,
    -- Total Together: Counts trips where both a flight and a hotel were booked.
    COUNT(
      CASE
         WHEN s.hotel_booked = 'true' AND s.flight_booked ='true' THEN s.trip_id
         ELSE NULL
      END
     ) AS total_together,
    -- Total Trips: Counts all trips irrespective of what was booked.
    COUNT(s.trip_id) AS total_trips,  
    -- Total Sessions: Counts all sessions. Assumes that a null trip_id signifies a browsing session.
    COUNT(s.trip_id IS NOT NULL) AS total_sessions,
    -- Average Clicks: Calculates the average number of page clicks during the sessions.
    AVG(s.page_clicks) AS average_clicks,
    -- Total Clicks: Aggregates total number of clicks across all sessions.
    SUM(s.page_clicks) AS total_clicks,  
    -- Total Cancellations: Counts the total number of cancelled trips.
    COUNT(
     DISTINCT 
       CASE 
         WHEN s.cancellation = 'true' THEN s.trip_id 
         ELSE NULL 
       END
    ) AS total_cancellations,
    -- Average Checked Bags: The average number of checked bags per flight. Nulls replaced by zero.
    COALESCE(AVG(f.checked_bags),0) AS average_checked_bags
  FROM UserSessions us
  JOIN users u ON u.user_id = us.user_id
  LEFT JOIN sessions s ON s.user_id = u.user_id
  LEFT JOIN flights f ON f.trip_id = s.trip_id
  LEFT JOIN hotels h ON h.trip_id = s.trip_id
GROUP BY u.user_id
ORDER BY u.user_id ASC
),
-- CTE: UserBehaviorIndices
-- The purpose of this CTE is to derive behavioral indices such as cancellation rate, engagement level, and others.
-- Metrics are calculated based on the aggregated metrics from the UserDiscountMetrics CTE.
-- Functions like COALESCE and NULLIF are used to handle divide-by-zero errors and null values.
UserBehaviorIndices AS (
  SELECT
    udm.user_id,
    udm.average_checked_bags,
    -- Total Cancellation Rate: Proportion of cancelled trips to total trips. 
    -- Nulls and zeros in the denominator are handled.
    COALESCE(
      (udm.total_cancellations::FLOAT
       / NULLIF(udm.total_trips::FLOAT ,0))
    ,0) AS total_cancellation_rate, -- 0 to 1 
    -- Engagement Index: It's a ratio of (average clicks * total trips) to total clicks.
    -- Measures user's involvement per click over all sessions.
    COALESCE(
      (udm.average_clicks * udm.total_trips)::FLOAT
             / udm.total_clicks
    ,0) AS engagement_index,
    -- Conversion Rate: Proportion of total trips to total sessions.
    COALESCE(
      udm.total_trips::FLOAT 
      / udm.total_sessions
    ,0) AS conversion_rate,
    -- Prefers Flights: Proportion of trips where only a flight was booked to the total number of trips.
    COALESCE(
      udm.total_only_flights::FLOAT 
      / 
      NULLIF(udm.total_trips,0)
    ,0) AS prefers_flights,
    -- Prefers Hotels: Proportion of trips where only a hotel was booked to the total number of trips.
    COALESCE(
      udm.total_only_hotels::FLOAT 
      / 
      NULLIF(udm.total_trips,0)
    ,0) AS prefers_hotels,
    -- Prefers Both: Proportion of trips where both flight and hotel were booked to the total number of trips.
    COALESCE(
      udm.total_together::FLOAT
      / 
      NULLIF(udm.total_trips,0)
    ,0) AS prefers_both,  
    -- Discount Responsiveness: Weighted sum of discount proportions for flights, hotels, and both.
    -- It's divided by total trips to normalize.
   COALESCE(
      (udm.flight_discount_proportion * udm.total_only_flights +
      udm.hotel_discount_proportion * udm.total_only_hotels + 
      udm.both_discount_proportion * udm.total_together)
      / 
      NULLIF(udm.total_trips,0)
    ,0) AS discount_responsiveness,
    -- Click Efficiency: Average number of clicks per trip.
    -- A lower value might indicate a more decisive user.
    COALESCE(
      udm.total_clicks::FLOAT
      / NULLIF(udm.total_trips,0)
    ,0) AS click_efficiency
  FROM UserDiscountMetrics udm
),
-- FinalQuery CTE: Aggregates user-specific metrics for reporting
-- This query compiles multiple user attributes, behaviors, preferences, and spending patterns 
-- It fetches this data from multiple other CTEs, namely UserSessions, UserDiscountMetrics,
-- UserTravelSpendSummary, ScaledTravelMetrics, and UserBehaviorIndices.
FinalQuery AS(
SELECT
  u.user_id,
  -- Handling NULLs for basic user information fields
  COALESCE(u.birthdate) AS birthdate,
  COALESCE(u.gender, '') AS gender,
  COALESCE(u.married) AS married,
  COALESCE(u.has_children) AS has_children,
  COALESCE(u.home_country, '') AS home_country,
  COALESCE(u.home_city, '') AS home_city,
  -- Extract the latest session date for each user, only considering Date part and not time
  MAX(DATE(s.session_end)) AS latest_session,
  -- Metrics from UserDiscountMetrics CTE  
  udm.total_trips,
  udm.total_cancellations,  
  udm.total_sessions,
  -- Metrics from UserBehaviorIndices CTE
  ubi.total_cancellation_rate,
  ubi.average_checked_bags,
  ubi.prefers_flights,
  ubi.prefers_hotels,
  ubi.prefers_both,  
  ubi.conversion_rate,
  -- Additional metrics
  udm.average_clicks,
  udm.total_clicks,
  ubi.click_efficiency,
  -- Discount-related metrics  
  udm.average_hotel_discount,
  udm.average_flight_discount,
  udm.flight_discount_proportion,  
  udm.hotel_discount_proportion,
  udm.both_discount_proportion,
  ubi.discount_responsiveness,
  -- Metrics from UserTravelSpendSummary CTE  
  utss.total_hotel_usd_spent,
  utss.total_flight_usd_spent,
  -- Calculating total spending in USD across flights and hotels
  utss.total_hotel_usd_spent + utss.total_flight_usd_spent AS total_usd_spent,
  -- Computation of an index to identify users primarily looking for hotel discounts.
  -- Uses scaled_hotel_ads from ScaledTravelMetrics, hotel_discount_proportion and average_hotel_discount from UserDiscountMetrics
  (COALESCE(scaled_hotel_ads,0)
  * COALESCE(udm.hotel_discount_proportion,0)
  * COALESCE(udm.average_hotel_discount,0)
  ) AS hotel_hunter_index  
FROM UserSessions us
JOIN users u ON u.user_id = us.user_id
LEFT JOIN sessions s ON u.user_id = s.user_id
LEFT JOIN UserDiscountMetrics udm ON u.user_id = udm.user_id
LEFT JOIN UserTravelSpendSummary utss ON u.user_id = utss.user_id
LEFT JOIN ScaledTravelMetrics stm ON u.user_id = stm.user_id
LEFT JOIN UserBehaviorIndices ubi ON u.user_id = ubi.user_id
GROUP BY u.user_id, 
-- Include other metrics that are not aggregate functions in the GROUP BY clause
udm.total_trips,
udm.total_cancellations,  
udm.total_sessions,
ubi.total_cancellation_rate,
ubi.average_checked_bags,
ubi.engagement_index, 
ubi.conversion_rate,
ubi.prefers_flights,
ubi.prefers_hotels,
ubi.prefers_both,  
ubi.discount_responsiveness,
ubi.click_efficiency,
udm.average_clicks,
udm.total_clicks,
stm.scaled_hotel_ads,
udm.average_flight_discount,
udm.average_hotel_discount,
udm.flight_discount_proportion,
udm.hotel_discount_proportion,
udm.both_discount_proportion,
utss.total_hotel_usd_spent,
utss.total_flight_usd_spent
ORDER BY u.user_id ASC
)
-- Final Output
SELECT *
FROM FinalQuery;




-- Query to fetch raw data required for distance calculation considering earth as an oblate spheroid in Python
-- The query gathers various attributes related to the user, sessions, flights, and hotels, which are then used for further calculations.
SELECT
  u.user_id,
  -- If trip_id is NULL, replace with an empty string
  COALESCE(s.trip_id, '') AS trip_id,
  -- Handling NULLs for basic user information fields
  COALESCE(u.birthdate) AS birthdate,
  COALESCE(u.gender, '') AS gender,
  COALESCE(u.married) AS married,
  COALESCE(u.has_children) AS has_children,
  COALESCE(u.home_country, '') AS home_country,
  COALESCE(u.home_city, '') AS home_city,
  -- Capture sign-up date for each user
  COALESCE(u.sign_up_date) AS sign_up_date,
  -- Capture discount information for flights and hotels
  COALESCE(s.flight_discount) AS f_discount,
  COALESCE(s.hotel_discount) AS h_discount,
  -- Actual amounts of discounts
  COALESCE(s.flight_discount_amount, 0) AS fd_amount,
  COALESCE(s.hotel_discount_amount, 0) AS hd_amount,
  -- Whether the flight or hotel was booked
  COALESCE(s.flight_booked) AS f_booked,
  COALESCE(s.hotel_booked) AS h_booked,
  -- Session end timestamp
  COALESCE(s.session_end) AS s_timestamp,
  -- Information on whether the trip was cancelled
  COALESCE(s.cancellation) AS cancelled,
  -- Number of page clicks during the session
  COALESCE(s.page_clicks, 0) AS page_clicks,
  -- Hotel details, if available
  COALESCE(h.hotel_name, '') AS h_hotel,
  COALESCE(h.rooms) AS h_rooms,
  -- Time spent at the hotel
  COALESCE(h.check_out_time - h.check_in_time) AS h_timespent,
  -- Hotel room price in USD
  COALESCE(h.hotel_per_room_usd) AS hotel_per_room_usd,
  -- Flight details, if available
  COALESCE(f.destination, '') AS f_destination,
  -- Whether a return flight was booked
  COALESCE(f.return_flight_booked) AS f_return_booked,
  -- Time spent on the flight
  COALESCE(f.return_time - f.departure_time) AS f_timespent,
  -- Number of checked bags
  COALESCE(f.checked_bags) AS f_checked_bags,
  -- Latitude and Longitude information for home and destination airports
  u.home_airport_lat,
  u.home_airport_lon,
  f.destination_airport_lat,
  f.destination_airport_lon,
  -- Base fare for the flight in USD
  COALESCE(f.base_fare_usd,0) AS base_fare_usd
FROM UserSessions us
JOIN users u ON u.user_id = us.user_id
-- LEFT JOINs are used to fetch optional data that may or may not exist for each user
LEFT JOIN sessions s ON s.user_id = u.user_id
LEFT JOIN flights f ON f.trip_id = s.trip_id
LEFT JOIN hotels h ON h.trip_id = s.trip_id
-- Sorting the results by user_id
ORDER BY user_id;
-- In case the Database is limited to only allow for 50,000 results add the following:
-- LIMIT 25000 OFFSET 0
-- Rerun and change LIMIT to: LIMIT 26000 OFFSET 25000