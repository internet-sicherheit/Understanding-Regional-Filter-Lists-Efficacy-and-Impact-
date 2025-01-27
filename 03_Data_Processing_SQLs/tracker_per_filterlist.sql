SELECT
  *
FROM (
  SELECT
    "China" AS country,
    COUNT(*) AS detected
  FROM
    filterlists.measurement.requests_backup
  WHERE
    is_tracker=1
    AND filterlist_China_is_blocked
  GROUP BY
    filterlist_China_is_blocked
  UNION ALL
  SELECT
    "USA" AS country,
    COUNT(*) AS detected
  FROM
    filterlists.measurement.requests_backup
  WHERE
    is_tracker=1
    AND filterlist_USA_is_blocked
  GROUP BY
    filterlist_USA_is_blocked
  UNION ALL
  SELECT
    "VAE" AS country,
    COUNT(*) AS detected
  FROM
    filterlists.measurement.requests_backup
  WHERE
    is_tracker=1
    AND filterlist_VAE_is_blocked
  GROUP BY
    filterlist_VAE_is_blocked
  UNION ALL
  SELECT
    "Indian" AS country,
    COUNT(*) AS detected
  FROM
    filterlists.measurement.requests_backup
  WHERE
    is_tracker=1
    AND filterlist_Indian_is_blocked
  GROUP BY
    filterlist_Indian_is_blocked
  UNION ALL
  SELECT
    "Japanese" AS country,
    COUNT(*) AS detected
  FROM
    filterlists.measurement.requests_backup
  WHERE
    is_tracker=1
    AND filterlist_Japanese_is_blocked
  GROUP BY
    filterlist_Japanese_is_blocked
  UNION ALL
  SELECT
    "Scandinavia" AS country,
    COUNT(*) AS detected
  FROM
    filterlists.measurement.requests_backup
  WHERE
    is_tracker=1
    AND filterlist_Scandinavia_is_blocked
  GROUP BY
    filterlist_Scandinavia_is_blocked
  UNION ALL
  SELECT
    "Indian_cleaned" AS country,
    COUNT(*) AS detected
  FROM
    filterlists.measurement.requests_backup
  WHERE
    is_tracker=1
    AND filterlist_Indian_cleaned_is_blocked
  GROUP BY
    filterlist_Indian_cleaned_is_blocked
  UNION ALL
  SELECT
    "Israel" AS country,
    COUNT(*) AS detected
  FROM
    filterlists.measurement.requests_backup
  WHERE
    is_tracker=1
    AND filterlist_Israel_is_blocked
  GROUP BY
    filterlist_Israel_is_blocked
  UNION ALL
  SELECT
    "Germany" AS country,
    COUNT(*) AS detected
  FROM
    filterlists.measurement.requests_backup
  WHERE
    is_tracker=1
    AND filterlist_Germany_is_blocked
  GROUP BY
    filterlist_Germany_is_blocked
  UNION ALL
  SELECT
    "France" AS country,
    COUNT(*) AS detected
  FROM
    filterlists.measurement.requests_backup
  WHERE
    is_tracker=1
    AND filterlist_France_is_blocked
  GROUP BY
    filterlist_France_is_blocked)
