-- Copyright (c) 2013 Snowplow Analytics Ltd. All rights reserved.
--
-- This program is licensed to you under the Apache License Version 2.0,
-- and you may not use this file except in compliance with the Apache License Version 2.0.
-- You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the Apache License Version 2.0 is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
--
-- OLAP compatible views at visit (session) level of granularity
--
-- Version:     0.1.0
-- URL:         -
--
-- Authors:     Yali Sassoon
-- Copyright:   Copyright (c) 2013 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0


-- Create schema
CREATE SCHEMA cubes_visits;

-- VIEW 1
-- Simplest visit-level view
CREATE VIEW cubes_visits.basic AS
	SELECT
		domain_userid,
		domain_sessionidx,
		MIN(collector_tstamp) AS visit_start_ts,
		MIN(dvce_tstamp) AS dvce_visit_start_ts,
		MAX(dvce_tstamp) AS dvce_visit_finish_ts,
		EXTRACT(EPOCH FROM (MAX(dvce_tstamp) - MIN(dvce_tstamp))) AS visit_duration_s,
		COUNT(*) AS number_of_events,
		COUNT(DISTINCT(page_urlpath)) AS distinct_pages_viewed
	FROM
		atomic.events
	GROUP BY 1,2;

-- VIEW 2
-- Referer data returned in a format that makes it easy to join with the visits view above
CREATE VIEW cubes_visits.referer_basic AS
	SELECT *
	FROM (
		SELECT
			domain_userid,
			domain_sessionidx,
			mkt_source,
			mkt_medium,
			mkt_campaign,
			mkt_term,
			refr_source,
			refr_medium,
			refr_term,
			refr_urlhost,
			refr_urlpath,
			dvce_tstamp,
			RANK() OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp) AS "rank"
		FROM
			atomic.events
		WHERE 
			refr_medium != 'internal' -- Not an internal referer
			AND (
				NOT(refr_medium IS NULL OR refr_medium = '') OR 
				NOT ((mkt_campaign IS NULL AND mkt_content IS NULL AND mkt_medium IS NULL AND mkt_source IS NULL AND mkt_term IS NULL)
					OR (mkt_campaign = '' AND mkt_content = '' AND mkt_medium = '' AND mkt_source = '' AND mkt_term = '')
				)
			) -- Either the refr or mkt fields are set (not blank)
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12) AS t
	WHERE "rank" = 1 -- Only pull the first referer for each visit


-- VIEW 3
-- View that joins refer data (view 2) with visit data (view 1)
CREATE VIEW cubes_visits.referer AS
	SELECT
		v.domain_userid,
		v.domain_sessionidx,
		r.mkt_source,
		r.mkt_medium,
		r.mkt_campaign,
		r.mkt_term,
		r.refr_medium,
		r.refr_term,
		r.refr_urlhost,
		r.refr_urlpath,
		v.visit_start_ts,
		v.visit_duration_s,
		v.number_of_events,
		v.distinct_pages_viewed
	FROM 
		cubes_visits.basic v
	LEFT JOIN cubes_visits.referer_basic r
	ON v.domain_userid = r.domain_userid
	AND v.domain_sessionidx = r.domain_sessionidx;

-- VIEW 4
-- Geographic info by visit
CREATE VIEW cubes_visits.geo_basic AS 
SELECT *
FROM (
	SELECT
		domain_userid,
		domain_sessionidx,
		geo_country,
		geo_region,
		geo_city,
		geo_zipcode,
		geo_latitude,
		geo_longitude,
		RANK() OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp) AS "rank" -- Oddly, we have users who show multiple locations per session. Maybe they're working behind proxy IPs?
	FROM atomic.events
	WHERE geo_country IS NOT NULL
	AND geo_country != '' ) AS t
	WHERE "rank" = 1;

-- VIEW 5
-- Join geo data (view 4) with visit / referer data (view 3)
CREATE VIEW cubes_visits.geo_and_referer AS
	SELECT
		r.domain_userid,
		r.domain_sessionidx,
		r.mkt_source,
		r.mkt_medium,
		r.mkt_campaign,
		r.mkt_term,
		r.refr_medium,
		r.refr_term,
		r.refr_urlhost,
		r.refr_urlpath,
		g.geo_country,
		g.geo_region,
		g.geo_city,
		g.geo_zipcode,
		g.geo_latitude,
		g.geo_longitude,
		r.visit_start_ts,
		r.visit_duration_s,
		r.number_of_events,
		r.distinct_pages_viewed
	FROM cubes_visits.referer r
	LEFT JOIN cubes_visits.geo_basic g 
	ON r.domain_userid = g.domain_userid
	AND r.domain_sessionidx = g.domain_sessionidx;

	

-- VIEW 6
-- Entry and exit pages by visit
-- First create a record of the pages each visitor has looked at
CREATE VIEW cubes_visits.page_views_in_sequence AS
	SELECT
	domain_userid,
	domain_sessionidx,
	page_urlhost,
	page_urlpath,
	rank_asc,
	rank_desc
	FROM (
		SELECT
		domain_userid,
		domain_sessionidx,
		page_urlhost,
		page_urlpath,
		RANK() OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp) AS "rank_asc",
		RANK() OVER (PARTITION BY domain_userid, domain_sessionidx ORDER BY dvce_tstamp DESC) AS "rank_desc"
		FROM atomic.events
		WHERE event = 'page_view') AS t
	GROUP BY 1,2,3,4,5,6; -- remove duplicates


-- Now use the above table to populate the entry and exit pages table
CREATE VIEW cubes_visits.entry_and_exit_pages AS
	SELECT
		v.domain_userid,
		v.domain_sessionidx,
		v.visit_start_ts,
		p1.page_urlhost AS entry_page_host,
		p1.page_urlpath AS entry_page_path,
		p2.page_urlhost AS exit_page_host,
		p2.page_urlpath AS exit_page_path,
		v.number_of_events,
		v.distinct_pages_viewed
	FROM
		cubes_visits.basic v
		LEFT JOIN page_views_in_sequence p1
			ON v.domain_userid = p1.domain_userid
			AND v.domain_sessionidx = p1.domain_sessionidx
		LEFT JOIN page_views_in_sequence p2
			ON v.domain_userid = p2.domain_userid
			AND v.domain_sessionidx = p2.domain_sessionidx
	WHERE p1.rank_asc = 1
	AND   p2.rank_desc = 1;


-- VIEW 7
-- Consolidated table with geo and referer data (VIEW 5) and entry / exit page data (VIEW 6)
CREATE VIEW cubes_visits.referer_entries_and_exits AS
	SELECT
		a.*,
		b.entry_page_host,
		b.entry_page_path,
		b.exit_page_host,
		b.exit_page_path
	FROM
		cubes_visits.geo_and_referer a
		LEFT JOIN cubes_visits.entry_and_exit_pages b
	ON a.domain_userid = b.domain_userid
	AND a.domain_sessionidx = b.domain_sessionidx;
