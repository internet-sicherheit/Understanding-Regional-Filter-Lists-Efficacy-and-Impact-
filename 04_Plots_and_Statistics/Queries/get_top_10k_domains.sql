# USA
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_us.202401` 
order by rank
LIMIT 10000;

# China
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_cn.202401` 
order by rank
LIMIT 10000;

# Japanese
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_jp.202401` 
order by rank
LIMIT 10000;

# Indian
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_in.202401` 
order by rank
LIMIT 10000;

# Germany
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_de.202401` 
order by rank
LIMIT 10000;

# Norweagin
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_no.202401` 
order by rank
LIMIT 10000;

# France
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_fr.202401` 
order by rank
LIMIT 10000;

# Israel
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_il.202401` 
order by rank
LIMIT 10000;

# VAE
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_ae.202401` 
order by rank
LIMIT 10000;