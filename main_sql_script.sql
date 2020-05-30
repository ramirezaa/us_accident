create table us_accidents_min as
select
	start_time,
	EXTRACT(QUARTER FROM to_timestamp("start_time", 'YYYY-MM-DD')) as "quarter",
	EXTRACT(YEAR FROM to_timestamp("start_time", 'YYYY-MM-DD')) as "year",
	EXTRACT(month FROM to_timestamp("start_time", 'YYYY-MM-DD')) as "month",
	tmc,
	severity::money::numeric,
	state,
	city,
	zipcode,
	visibilitymi::money::numeric,
	weather_condition,
	bump,
	wind_speedmph::money::numeric,
	precipitationin::money::numeric,
	pressurein::money::numeric,
	humidity::money::numeric,
	wind_chillf::money::numeric,
	temperaturef::money::numeric,
	distancemi::money::numeric,
	count(*) as accident_count
from us_accidents
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19;