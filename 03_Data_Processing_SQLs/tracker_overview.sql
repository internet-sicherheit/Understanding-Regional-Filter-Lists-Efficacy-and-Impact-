# 4.9.1 Number of requets blocked by EasyList
SELECT count(*) AS number, (count(*) / (SELECT count(*) FROM measurement.requests ) * 100) AS perc,
FROM `measurement.requests`
WHERE filterlist_USA_is_blocked;

# 4.9.2 Number of blocked eTLD+1 by EasyList
SELECT count(*) AS blocked_etld1,
FROM (SELECT DISTINCT(etld) FROM `measurement.requests` WHERE filterlist_USA_is_blocked);

# 4.9.4



SELECT tracker.c as trackers, all_urls.c as total_urls, tracker.c/all_urls.c * 100 as identified_trackers FROM tracker, all_urls;


# trackers per list
SELECT
  browser_id,
  COUNTIF(filterlist_China_is_blocked) AS blocked_by_China,
  COUNTIF(filterlist_France_is_blocked) AS blocked_by_France,
  COUNTIF(filterlist_Germany_is_blocked) AS blocked_by_Germany,
  COUNTIF(filterlist_Indian_is_blocked) AS blocked_by_India,
  COUNTIF(filterlist_Israel_is_blocked) AS blocked_by_Israel,
  COUNTIF(filterlist_Japanese_is_blocked) AS blocked_by_Japan,
  COUNTIF(filterlist_Scandinavia_is_blocked) AS blocked_by_Scandinavia,
  COUNTIF(filterlist_USA_is_blocked) AS blocked_by_USA,
  COUNTIF(filterlist_VAE_is_blocked) AS blocked_by_VAE
FROM
  measurement.requests
GROUP BY browser_id;


# blocked by US list AND other list

SELECT COUNT(*) AS number, (COUNT(*) / (SELECT COUNT(*)FROM `measurement.requests` WHERE filterlist_USA_is_blocked)*100) AS sahre
FROM `measurement.requests`
WHERE filterlist_USA_is_blocked AND (filterlist_China_is_blocked OR filterlist_France_is_blocked OR filterlist_Germany_is_blocked OR filterlist_Indian_is_blocked OR filterlist_Israel_is_blocked OR filterlist_Japanese_is_blocked OR filterlist_Scandinavia_is_blocked OR filterlist_VAE_is_blocked);



SELECT
  COUNTIF(filterlist_China_is_blocked) AS blocked_by_China,
  COUNTIF(filterlist_France_is_blocked) AS blocked_by_France,
  COUNTIF(filterlist_Germany_is_blocked) AS blocked_by_Germany,
  COUNTIF(filterlist_Indian_is_blocked) AS blocked_by_India,
  COUNTIF(filterlist_Israel_is_blocked) AS blocked_by_Israel,
  COUNTIF(filterlist_Japanese_is_blocked) AS blocked_by_Japan,
  COUNTIF(filterlist_Scandinavia_is_blocked) AS blocked_by_Scandinavia,
  COUNTIF(filterlist_USA_is_blocked) AS blocked_by_USA,
  COUNTIF(filterlist_VAE_is_blocked) AS blocked_by_VAE
FROM `measurement.requests`
WHERE filterlist_USA_is_blocked
