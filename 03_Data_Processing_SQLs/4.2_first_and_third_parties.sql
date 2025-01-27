# First and Third-party cookies
SELECT is_third_party, count(*) FROM filterlists.measurement.cookies GROUP BY is_third_party;
