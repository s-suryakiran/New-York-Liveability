CREATE TABLE FinalScores AS
WITH BoroughZipMapping AS (
    SELECT CAST(CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) AS VARCHAR) AS zipcode,
           CASE
               WHEN CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) BETWEEN 10001 AND 10282 THEN 'manhattan'
               WHEN CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) BETWEEN 10451 AND 10475 THEN 'bronx'
               WHEN CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) BETWEEN 11201 AND 11256 THEN 'brooklyn'
               WHEN CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) BETWEEN 11001 AND 11697 THEN 'queens'
               WHEN CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) BETWEEN 10301 AND 10314 THEN 'staten island'
               ELSE 'Unknown'
           END AS borough
    FROM (
        SELECT DISTINCT zipcode FROM subway
        UNION
        SELECT DISTINCT zipcode FROM bus_location
        UNION
        SELECT DISTINCT zipcode FROM shop
        UNION
        SELECT DISTINCT zipcode FROM restaurants
        UNION
        SELECT DISTINCT zipcode FROM citi_bikes
        UNION
        SELECT DISTINCT zipcode FROM facilities
        UNION
        SELECT DISTINCT zipcode FROM complaint
        UNION
        SELECT DISTINCT zipcode FROM health_facilities
        UNION
        SELECT DISTINCT zipcode FROM bus_shelter
        UNION
        SELECT DISTINCT zipcode FROM shooting
    ) sub
),

TransportationScores AS (
    SELECT z.zipcode,
           ROUND(COALESCE(subway_score, 5) * 0.4 + 
                  COALESCE(bus_location_score, 5) * 0.3 + 
                  COALESCE(citi_bike_score, 5) * 0.2 + 
                  COALESCE(bus_shelter_score, 5) * 0.1, 2) AS transportation_score
    FROM BoroughZipMapping z
    LEFT JOIN (
        SELECT CAST(CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) AS VARCHAR) AS zipcode, CAST(COUNT(stopname) AS DOUBLE) / MAX(COUNT(stopname)) OVER () * 10 AS subway_score
        FROM subway GROUP BY zipcode
    ) s ON z.zipcode = s.zipcode
    LEFT JOIN (
        SELECT CAST(CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) AS VARCHAR) AS zipcode, CAST(SUM(CAST(currentloc AS INT)) AS DOUBLE) / MAX(SUM(CAST(currentloc AS INT))) OVER () * 10 AS bus_location_score
        FROM bus_location GROUP BY zipcode
    ) b ON z.zipcode = b.zipcode
    LEFT JOIN (
        SELECT CAST(CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) AS VARCHAR) AS zipcode, CAST(SUM(ride_count_per_bike_type_per_station) AS DOUBLE) / MAX(SUM(ride_count_per_bike_type_per_station)) OVER () * 10 AS citi_bike_score
        FROM citi_bikes GROUP BY zipcode
    ) c ON z.zipcode = c.zipcode
    LEFT JOIN (
        SELECT CAST(CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) AS VARCHAR) AS zipcode, CAST(COUNT(*) AS DOUBLE) / MAX(COUNT(*)) OVER () * 10 AS bus_shelter_score
        FROM bus_shelter GROUP BY zipcode
    ) bs ON z.zipcode = bs.zipcode
),

AmenitiesScores AS (
    SELECT z.zipcode,
           ROUND(COALESCE(shop_score, 5) * 0.1 + 
                  COALESCE(restaurant_score, 5) * 0.3 + 
                  COALESCE(facility_score, 5) * 0.6, 2) AS amenities_score
    FROM BoroughZipMapping z
    LEFT JOIN (
        SELECT CAST(CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) AS VARCHAR) AS zipcode, CAST(COUNT(*) AS DOUBLE) / MAX(COUNT(*)) OVER () * 10 AS shop_score
        FROM shop GROUP BY zipcode
    ) sh ON z.zipcode = sh.zipcode
    LEFT JOIN (
        SELECT CAST(CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) AS VARCHAR) AS zipcode, CAST(COUNT(*) AS DOUBLE) / MAX(COUNT(*)) OVER () * 10 AS restaurant_score
        FROM restaurants GROUP BY zipcode
    ) r ON z.zipcode = r.zipcode
    LEFT JOIN (
        SELECT CAST(CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) AS VARCHAR) AS zipcode, CAST(COUNT(*) AS DOUBLE) / MAX(COUNT(*)) OVER () * 10 AS facility_score
        FROM facilities GROUP BY zipcode
    ) f ON z.zipcode = f.zipcode
),

SafetyScores AS (
    SELECT z.zipcode,
           ROUND(
             COALESCE(complaint_score, 5) * 0.3 + 
             COALESCE(shooting_score, 5) * 0.4 + 
             COALESCE(health_facility_score, 5) * 0.3, 
             2
           ) AS safety_score
    FROM BoroughZipMapping z
    LEFT JOIN (
        SELECT CAST(CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) AS VARCHAR) AS zipcode, 
               10 - (CAST(SUM(severity_weight * num_complaints) AS DOUBLE) / MAX(SUM(severity_weight * num_complaints)) OVER () * 10) AS complaint_score
        FROM (
            SELECT zipcode, COUNT(*) AS num_complaints,
                   CASE lawcatcd
                       WHEN 'FELONY' THEN 3
                       WHEN 'MISDEMEANOR' THEN 2
                       WHEN 'VIOLATION' THEN 1
                       ELSE 1
                   END AS severity_weight
            FROM complaint
            GROUP BY zipcode, lawcatcd
        ) comp 
        GROUP BY zipcode
    ) com ON z.zipcode = com.zipcode
    LEFT JOIN (
        SELECT CAST(CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) AS VARCHAR) AS zipcode, 
               10 - (CAST(SUM(weighted_count) AS DOUBLE) / MAX(SUM(weighted_count)) OVER () * 10) AS shooting_score
        FROM (
            SELECT zipcode, SUM(CAST(TRIM(year) AS INT) - 2005) AS weighted_count
            FROM shooting
            GROUP BY zipcode
        ) sht
        GROUP BY zipcode
    ) sht ON z.zipcode = sht.zipcode
    LEFT JOIN (
        SELECT CAST(CAST(ROUND(CAST(zipcode AS DOUBLE)) AS INT) AS VARCHAR) AS zipcode, CAST(COUNT(*) AS DOUBLE) / MAX(COUNT(*)) OVER () * 10 AS health_facility_score
        FROM health_facilities GROUP BY zipcode
    ) hf ON z.zipcode = hf.zipcode
),

AirQualityScores AS (
    SELECT b.borough,
           AVG(CASE
                   WHEN name = 'Fine particles (PM 2.5)' THEN (10 - (CAST(datavalue AS DOUBLE) / 35) * 10) * 0.5
                   WHEN name = 'Nitrogen dioxide (NO2)' THEN (10 - (CAST(datavalue AS DOUBLE) / 53) * 10) * 0.3
                   WHEN name = 'Ozone (O3)' THEN (10 - (CAST(datavalue AS DOUBLE) / 70) * 10) * 0.2
                   ELSE 0
               END) AS air_quality_score
    FROM airquality a
    JOIN BoroughZipMapping b ON a.borough = b.borough
    WHERE CAST(TRIM(year) AS INT) = 2022
    GROUP BY b.borough
),

FinalScores AS (
    SELECT z.zipcode,
           ts.transportation_score,
           ascore.amenities_score,
           ss.safety_score,
           COALESCE(aq.air_quality_score, 2.6) AS air_quality_score,
           ROUND(ts.transportation_score * 0.3 + 
                  ascore.amenities_score * 0.15 + 
                  ss.safety_score * 0.4 + 
                  COALESCE(aq.air_quality_score, 0) * 0.15, 2) AS final_score
    FROM BoroughZipMapping z
    LEFT JOIN TransportationScores ts ON z.zipcode = ts.zipcode
    LEFT JOIN AmenitiesScores ascore ON z.zipcode = ascore.zipcode
    LEFT JOIN SafetyScores ss ON z.zipcode = ss.zipcode
    LEFT JOIN AirQualityScores aq ON z.borough = aq.borough
)

SELECT DISTINCT
       zipcode,
       transportation_score,
       amenities_score,
       safety_score,
       air_quality_score,
       final_score
FROM FinalScores
WHERE (CAST(zipcode AS INT) BETWEEN 10001 AND 10282) OR -- Manhattan
      (CAST(zipcode AS INT) BETWEEN 10451 AND 10475) OR -- Bronx
      (CAST(zipcode AS INT) BETWEEN 11201 AND 11256) OR -- Brooklyn
      (CAST(zipcode AS INT) BETWEEN 11004 AND 11697) OR -- Queens
      (CAST(zipcode AS INT) BETWEEN 10301 AND 10314)  
;
