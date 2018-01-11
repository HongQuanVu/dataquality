WITH
  gust_measures AS (
    SELECT
      stn,
      year,
      mo,
      da,
      ROUND((gust * 1.852), 2) AS gust_speed_kph
    FROM
      `bigquery-public-data.noaa_gsod.gsod{{ macros.ds_format(yesterday_ds_nodash, "%Y%m%d", "%Y") }}`
    WHERE
      stn != '999999'
      AND year = '{{ macros.ds_format(yesterday_ds_nodash, "%Y%m%d", "%Y") }}'
      AND mo = '{{ macros.ds_format(yesterday_ds_nodash, "%Y%m%d", "%m") }}'
      AND da= '{{ macros.ds_format(yesterday_ds_nodash, "%Y%m%d", "%d") }}'
      AND gust > (50 / 1.852) -- gust speed > 50 km/h
      AND gust < 999.9
  ),
  stations AS (
    SELECT
      usaf AS stn,
      country,
      state,
      name,
      lat,
      lon,
      CASE WHEN elev = '' THEN NULL ELSE CAST(elev AS FLOAT64) END AS elev
    FROM
      `bigquery-public-data.noaa_gsod.stations`
  )
 
SELECT
  *
FROM
  gust_measures
INNER JOIN
  stations USING (stn)
