# Known Tracker
SELECT browser_id, is_tracker, count(*) FROM filterlists.measurement.requests GROUP BY browser_id, is_tracker
