-- STEP 0: Deduplicate raw bloq records to keep the most recent row per bloq per hour
WITH ranked AS
  (SELECT bloqs_id,
          partner,
          active,
          LOCATION,
          metadata,
          details,
          firmwaredetails,
          date_trunc('hour', system_updated_at) AS hour_bucket,
          ROW_NUMBER() OVER (PARTITION BY bloqs_id, date_trunc('hour', system_updated_at)
                             ORDER BY system_updated_at DESC) AS rn
   FROM prod_bo2dl_bloqs_gluedb_prepared.prod_bo2dl_bloqs_prepared_iceberg_t
   WHERE 1 = 1
     AND system_updated_at >= current_date - INTERVAL '1' MONTH
     AND powersource = 'BATTERY'
     AND lifecyclestatus IS NOT NULL
     AND lifecyclestatus <> 'terminated'
     AND active = 'true'),

-- STEP 1: Keep only the latest hourly row and ensure both batteries exist
pre_filtered AS
  (SELECT *
   FROM ranked
   WHERE rn = 1
     AND lower(firmwaredetails) LIKE '%bat1%'
     AND lower(firmwaredetails) LIKE '%bat2%'),

-- STEP 2: Flatten battery JSON array into one row per battery per hour
daily_battery AS
  (SELECT pf.hour_bucket,
          pf.bloqs_id,
          CASE
              WHEN json_extract_scalar(pf.metadata, '$.bloqitName') IS NULL THEN NULL
              WHEN length(json_extract_scalar(pf.metadata, '$.bloqitName')) > 5
                   THEN substr(json_extract_scalar(pf.metadata, '$.bloqitName'), 1,
                               length(json_extract_scalar(pf.metadata, '$.bloqitName')) - 5)
              ELSE json_extract_scalar(pf.metadata, '$.bloqitName')
          END AS partner,
          json_extract_scalar(pf.metadata, '$.bloqitName') AS bloq_name,
          json_extract_scalar(pf.firmwaredetails, '$.firmwareVersion') AS fw_version,
          CASE
              WHEN json_extract_scalar(pf.firmwaredetails, '$.firmwareVersion') LIKE '7.1.%'
                   AND json_extract_scalar(pf.firmwaredetails, '$.firmwareVersion') NOT IN ('7.1.1', '7.1.2')
              THEN 'recent'
              ELSE 'old'
          END AS fw_version_type,
          json_extract_scalar(bat, '$.batteryName') AS battery_name,
          CAST(json_extract_scalar(bat, '$.capacityStateOffCharge') AS DOUBLE) AS soc,
          json_extract_scalar(bat, '$.voltage') AS voltage,
          json_extract_scalar(bat, '$.temperature') AS temperature,
          json_extract_scalar(bat, '$.extraInfo.battery_serial_number') AS battery_serial_number,
          json_extract_scalar(pf.details, '$.country') AS country,
          json_extract_scalar(pf.details, '$.address') AS address,
          coalesce(json_extract_scalar(pf.metadata, '$.apm_name'),
                   json_extract_scalar(pf.metadata, '$.vintedCode')) AS point_code
   FROM pre_filtered pf
   CROSS JOIN UNNEST(CAST(json_parse(json_extract_scalar(pf.firmwaredetails, '$.battery')) AS array(JSON))) AS t(bat)
   WHERE json_extract_scalar(bat, '$.batteryName') IS NOT NULL
     AND json_extract_scalar(bat, '$.batteryName') <> ''
     AND json_extract_scalar(bat, '$.capacityStateOffCharge') IS NOT NULL
     AND json_extract_scalar(bat, '$.capacityStateOffCharge') <> ''),

-- STEP 3: Deduplicate events and aggregate rents per bloq per hour
events_ranked AS
  (SELECT rent,
          bloq,
          CAST(date_trunc('hour', from_iso8601_timestamp(timestamp)) AS timestamp) AS timestp,
          ROW_NUMBER() OVER (PARTITION BY events_id
                             ORDER BY pipeline_ingested_at DESC) AS rn
   FROM prod_bo2dl_events_gluedb_prepared.prod_bo2dl_events_prepared_iceberg_t
   WHERE bloq IS NOT NULL),

rents_table AS
  (SELECT timestp,
          bloq,
          count(DISTINCT rent) AS rents
   FROM events_ranked
   WHERE rn = 1
   GROUP BY bloq,
            timestp)

SELECT d.*,
       r.rents
FROM daily_battery d
LEFT JOIN rents_table r ON r.bloq = d.bloqs_id
AND r.timestp = d.hour_bucket;
