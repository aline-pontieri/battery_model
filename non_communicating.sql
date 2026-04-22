-- Non-communicating batteries: bloqs with no update for 24h or more
WITH ranked AS (
    SELECT
        bloqs_id,
        metadata,
        details,
        firmwaredetails,
        date_trunc('hour', system_updated_at) AS hour_bucket,
        ROW_NUMBER() OVER (
            PARTITION BY bloqs_id, date_trunc('hour', system_updated_at)
            ORDER BY system_updated_at DESC
        ) AS rn
    FROM prod_bo2dl_bloqs_gluedb_prepared.prod_bo2dl_bloqs_prepared_iceberg_t
    WHERE system_updated_at >= current_date - INTERVAL '30' DAY
      AND powersource = 'BATTERY'
      AND lifecyclestatus IS NOT NULL
      AND lifecyclestatus <> 'terminated'
      AND active = 'true'
),
pre_filtered AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
      AND lower(firmwaredetails) LIKE '%bat1%'
      AND lower(firmwaredetails) LIKE '%bat2%'
),
batteries AS (
    SELECT
        pf.bloqs_id,
        pf.hour_bucket,
        json_extract_scalar(bat, '$.batteryName')                        AS battery_name,
        json_extract_scalar(bat, '$.extraInfo.battery_serial_number')    AS battery_serial_number,
        json_extract_scalar(pf.metadata, '$.bloqitName')                 AS bloq_name,
        json_extract_scalar(pf.details, '$.address')                     AS address,
        json_extract_scalar(pf.details, '$.city')                        AS city,
        json_extract_scalar(pf.details, '$.country')                     AS country,
        CASE
            WHEN json_extract_scalar(pf.metadata, '$.bloqitName') IS NULL THEN NULL
            WHEN length(json_extract_scalar(pf.metadata, '$.bloqitName')) > 5
                 THEN substr(json_extract_scalar(pf.metadata, '$.bloqitName'), 1,
                             length(json_extract_scalar(pf.metadata, '$.bloqitName')) - 5)
            ELSE json_extract_scalar(pf.metadata, '$.bloqitName')
        END AS partner
    FROM pre_filtered pf
    CROSS JOIN UNNEST(
        CAST(json_parse(json_extract_scalar(pf.firmwaredetails, '$.battery')) AS array(json))
    ) AS t(bat)
    WHERE json_extract_scalar(bat, '$.batteryName') IS NOT NULL
      AND json_extract_scalar(bat, '$.batteryName') <> ''
),
latest_per_battery AS (
    SELECT
        bloqs_id,
        battery_name,
        battery_serial_number,
        bloq_name,
        partner,
        address,
        city,
        country,
        MAX(hour_bucket) AS last_update,
        DATE_DIFF('hour', MAX(hour_bucket), current_timestamp) AS hours_since_last_comm
    FROM batteries
    GROUP BY
        bloqs_id, battery_name, battery_serial_number,
        bloq_name, partner, address, city, country
)
SELECT
    bloqs_id           AS bloq_id,
    bloq_name,
    partner,
    battery_name,
    battery_serial_number,
    last_update,
    hours_since_last_comm,
    address,
    city,
    country
FROM latest_per_battery
WHERE hours_since_last_comm >= 24
ORDER BY hours_since_last_comm DESC;
