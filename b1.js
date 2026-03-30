const { MongoClient } = require("mongodb");
const { Client: PgClient } = require("pg");

// -------------------- Config --------------------
const MONGO_URI = process.env.MONGO_URI || "mongodb://localhost:27018";
const MONGO_DB = process.env.MONGO_DB || "status";
const MONGO_COLLECTION = process.env.MONGO_COLLECTION || "media_list";

const ZONE_ID = Number(process.env.ZONE_ID || 6925);
const PLAYLIST_DATE = process.env.PLAYLIST_DATE || new Date().toISOString().slice(0, 10); // YYYY-MM-DD

// Postgres
const pgClient = new PgClient({
  host: process.env.PG_HOST || "localhost",
  port: Number(process.env.PG_PORT || 5433),
  user: process.env.PG_USER || "postgres",
  password: process.env.PG_PASSWORD || "reallybadpassword",
  database: process.env.PG_DB || "retail_radio",
  ssl: false,
});

// -------------------- Helpers --------------------
function normalizeName(name) {
  if (!name) return "";
  return name
    .normalize("NFKD")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, " ")
    .replace(/\([^)]*\)/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function printSection(title) {
  const line = "─".repeat(60);
  console.log(`\n${line}`);
  console.log(`  ${title}`);
  console.log(line);
}

function printKeyValue(key, value) {
  const paddedKey = String(key).padEnd(28);
  console.log(`  ${paddedKey}: ${value}`);
}

// -------------------- SQL --------------------
// Notes:
// - We use $1 = ZONE_ID and $2 = PLAYLIST_DATE::date
// - Holiday + Regular + Default are included (superset) to avoid missing tracks.
// - Include-tags always union with P_Failsafe2021 to avoid missing failsafe songs.
// - Banned artists are excluded via production_song_artists (NOT artist_primary_id).
const GET_PG_PLAYABLE_SONGS_SQL = `
WITH RECURSIVE
station_group_chain AS (
    SELECT
        ssmg.basegroup_ptr_id,
        ssmg.parent_id,
        ssmg.allow_holiday_override,
        sz.supported_format,
        sz.owner
    FROM stores_zone sz
    JOIN scheduler_stationmediagroup ssmg
        ON ssmg.basegroup_ptr_id = sz.station_media_group_id
    WHERE sz.id = $1

    UNION ALL

    SELECT
        parent.basegroup_ptr_id,
        parent.parent_id,
        parent.allow_holiday_override,
        child.supported_format,
        child.owner
    FROM scheduler_stationmediagroup parent
    JOIN station_group_chain child
        ON child.parent_id = parent.basegroup_ptr_id
),

holiday_policies AS (
    SELECT
        sp.station_id,
        sg.supported_format,
        sg.owner,
        sp.weight,
        sp.id
    FROM station_group_chain sg
    JOIN scheduler_stationmediagroup_holiday_program smghp
        ON smghp.stationmediagroup_id = sg.basegroup_ptr_id
    JOIN scheduler_stationpolicy sp
        ON sp.id = smghp.stationpolicy_id
    WHERE
        sg.allow_holiday_override = TRUE
        AND (sp.start_date IS NULL OR sp.start_date <= $2::date)
        AND (sp.end_date IS NULL OR sp.end_date >= $2::date)
        AND (
            sp.day_of_week_mask IS NULL
            OR sp.day_of_week_mask = 0
            OR (sp.day_of_week_mask & (1 << EXTRACT(DOW FROM $2::date)::int)) > 0
        )
),

regular_policies AS (
    SELECT
        sp.station_id,
        sg.supported_format,
        sg.owner,
        sp.weight,
        sp.id
    FROM station_group_chain sg
    JOIN scheduler_stationpolicy sp
        ON sp.group_id = sg.basegroup_ptr_id
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM scheduler_stationmediagroup_holiday_program smghp
            WHERE smghp.stationpolicy_id = sp.id
        )
        AND (sp.start_date IS NULL OR sp.start_date <= $2::date)
        AND (sp.end_date IS NULL OR sp.end_date >= $2::date)
        AND (
            sp.day_of_week_mask IS NULL
            OR sp.day_of_week_mask = 0
            OR (sp.day_of_week_mask & (1 << EXTRACT(DOW FROM $2::date)::int)) > 0
        )
),

default_station_policies AS (
    SELECT
        dsp.station_id,
        sg.supported_format,
        sg.owner,
        dsp.combine_rule,
        ROW_NUMBER() OVER (ORDER BY dsp.combine_rule DESC) AS rank
    FROM station_group_chain sg
    JOIN scheduler_defaultstationpolicy dsp
        ON dsp.group_id = sg.basegroup_ptr_id
),

default_station_policy AS (
    SELECT station_id, supported_format, owner
    FROM default_station_policies
    WHERE rank = 1
),

all_station_policies AS (
    SELECT station_id, supported_format, owner FROM holiday_policies
    UNION ALL
    SELECT station_id, supported_format, owner FROM regular_policies
    UNION ALL
    SELECT station_id, supported_format, owner FROM default_station_policy
    UNION ALL
    SELECT
        19865 AS station_id,
        sg.supported_format,
        sg.owner
    FROM (
        SELECT supported_format, owner
        FROM station_group_chain
        LIMIT 1
    ) sg
    WHERE NOT EXISTS (SELECT 1 FROM holiday_policies)
      AND NOT EXISTS (SELECT 1 FROM regular_policies)
      AND NOT EXISTS (SELECT 1 FROM default_station_policy)
),

effective_medialist AS (
    SELECT DISTINCT supported_format, owner, station_id AS media_list_id
    FROM all_station_policies
),

station_tree AS (
    SELECT
        em.supported_format,
        em.owner,
        em.media_list_id,
        sm.base_station_id,
        sm.station_id
    FROM effective_medialist em
    JOIN scheduler_mixpolicy sm
        ON sm.base_station_id = em.media_list_id

    UNION ALL

    SELECT
        st.supported_format,
        st.owner,
        st.media_list_id,
        sm.base_station_id,
        sm.station_id
    FROM scheduler_mixpolicy sm
    INNER JOIN station_tree st
        ON sm.base_station_id = st.station_id
),

all_medialist_ids AS (
    SELECT media_list_id AS id FROM effective_medialist
    UNION
    SELECT base_station_id FROM station_tree
    UNION
    SELECT station_id FROM station_tree
),

failsafe_tag AS (
    SELECT id AS tag_id
    FROM production_tag
    WHERE name = 'P_Failsafe2021'
    LIMIT 1
),

aggregated_rules AS (
    SELECT
        /* Always union failsafe tag into include_tags (prevents missing failsafe-only days). */
        (
          SELECT ARRAY(
            SELECT DISTINCT t.tag_id
            FROM (
              SELECT unnest(COALESCE(ARRAY_AGG(DISTINCT str.tag_id), '{}'::int[])) AS tag_id
              UNION ALL
              SELECT tag_id FROM failsafe_tag
            ) t
          )
        ) AS include_tags,

        ARRAY_REMOVE(ARRAY_AGG(DISTINCT smlet.tag_id), NULL) AS exclude_tags,
        ARRAY_REMOVE(ARRAY_AGG(DISTINCT smlba.artist_id), NULL) AS banned_artists,
        ARRAY_REMOVE(ARRAY_AGG(DISTINCT smlbs.song_id), NULL) AS banned_songs
    FROM all_medialist_ids ids
    LEFT JOIN scheduler_tagrule str
        ON ids.id = str.media_list_id
    LEFT JOIN scheduler_medialist_excluded_tags smlet
        ON ids.id = smlet.medialist_id
    LEFT JOIN scheduler_medialist_banned_artists smlba
        ON ids.id = smlba.medialist_id
    LEFT JOIN scheduler_medialist_banned_songs smlbs
        ON ids.id = smlbs.medialist_id
)

SELECT DISTINCT pm.id, pm.name
FROM production_media pm
LEFT JOIN production_song ps
    ON ps.media_ptr_id = pm.id
CROSS JOIN aggregated_rules r
WHERE
    ps.synced = TRUE
    AND pm.media_file IS NOT NULL
    AND pm.media_file <> ''

    /* Banned artists: exclude if the song has ANY of the banned artists. */
    AND (
      r.banned_artists IS NULL
      OR NOT EXISTS (
        SELECT 1
        FROM production_song_artists psa
        WHERE psa.song_id = pm.id
          AND psa.artist_id = ANY(r.banned_artists)
      )
    )

    AND (
      r.banned_songs IS NULL
      OR NOT (pm.id = ANY(r.banned_songs))
    )

    /* Excluded tags */
    AND NOT EXISTS (
        SELECT 1
        FROM production_song_tags pst_ex
        WHERE pst_ex.song_id = pm.id
          AND r.exclude_tags IS NOT NULL
          AND pst_ex.tag_id = ANY(r.exclude_tags)
    )

    /* Included tags (OR semantics, superset; generator uses AND for active tags). */
    AND (
      r.include_tags IS NULL
      OR array_length(r.include_tags, 1) IS NULL
      OR EXISTS (
        SELECT 1
        FROM production_song_tags pst_in
        WHERE pst_in.song_id = pm.id
          AND pst_in.tag_id = ANY(r.include_tags)
      )
    );
`;

const GET_ORDER_ID_SQL = `
SELECT mp.ordering_id
FROM stores_zone z
JOIN scheduler_mediagroup mg ON mg.basegroup_ptr_id = z.media_group_id
JOIN scheduler_mediapolicy mp ON mp.group_id = mg.basegroup_ptr_id
WHERE z.id = $1
AND mp.ordering_id IS NOT NULL
ORDER BY mp.id DESC
LIMIT 1;
`;

const GET_MEDIA_LIST_IDS_SQL = `
SELECT op.media_list_id
FROM scheduler_ordering o
JOIN scheduler_orderingpolicy op ON op.ordering_id = o.id
WHERE o.id = $1
ORDER BY op."order";
`;

const GET_MEDIA_FROM_ORDER_SQL = `
SELECT DISTINCT pm.name
FROM scheduler_mediaorder smo
JOIN production_media pm ON pm.id = smo.media_id
WHERE smo.media_list_id = ANY($1);
`;

// -------------------- Main --------------------
async function compareNestedMongoNames() {
  let mongo;
  try {
    // Mongo: scope to zone + playlist date paths
    mongo = await MongoClient.connect(MONGO_URI, { serverSelectionTimeoutMS: 5000 });
    const db = mongo.db(MONGO_DB);
    const zoneInfo = db.collection("zoneInfo");
    const mediaList = db.collection(MONGO_COLLECTION);

    const dayUTC = new Date(`${PLAYLIST_DATE}T00:00:00.000Z`);

    const agg = await zoneInfo
      .aggregate([
        { $match: { grrid_id: ZONE_ID } },
        { $unwind: "$schedule" },
        { $unwind: "$schedule.triggers" },
        { $match: { "schedule.date": dayUTC } },
        { $group: { _id: null, paths: { $addToSet: "$schedule.triggers.path" } } },
      ])
      .toArray();

    const paths = (agg[0]?.paths || []).filter(Boolean);
    if (paths.length === 0) {
      console.warn(
        `No Mongo media_list paths found for zone=${ZONE_ID}, date=${PLAYLIST_DATE}. ` +
          `Try a different PLAYLIST_DATE matching the scheduler job.`
      );
    }

    const mongoDocs = paths.length
  ? await mediaList.find({ path: { $in: paths } }).toArray()
  : await mediaList.find({}).toArray();

    const mongoNames = new Set();
    for (const doc of mongoDocs) {
      const list = doc?.list;
      if (!Array.isArray(list)) continue;
      for (const item of list) {
        if (!item) continue;
        const rawName = item.name || item.title || null;
        if (rawName) mongoNames.add(normalizeName(rawName));
      }
    }

    // Postgres
    await pgClient.connect();

    const pgRes = await pgClient.query(GET_PG_PLAYABLE_SONGS_SQL, [ZONE_ID, PLAYLIST_DATE]);
    const pgNames = new Set(pgRes.rows.map(r => normalizeName(r.name)).filter(Boolean));

    // Ordering additions (kept from your original script)
    const orderRes = await pgClient.query(GET_ORDER_ID_SQL, [ZONE_ID]);
    if (orderRes.rows.length > 0) {
      const orderingId = orderRes.rows[0].ordering_id;
      const mediaListRes = await pgClient.query(GET_MEDIA_LIST_IDS_SQL, [orderingId]);
      const mediaListIds = mediaListRes.rows.map(r => r.media_list_id).filter(Boolean);

      if (mediaListIds.length > 0) {
        const mediaRes = await pgClient.query(GET_MEDIA_FROM_ORDER_SQL, [mediaListIds]);
        for (const row of mediaRes.rows) {
          if (row.name) pgNames.add(normalizeName(row.name));
        }
      }
    }

    // Compare
    const present = [];
    const missing = [];
    for (const name of mongoNames) {
      (pgNames.has(name) ? present : missing).push(name);
    }

    const matchPercentage =
      mongoNames.size === 0 ? 0 : ((present.length / mongoNames.size) * 100).toFixed(2);

    printSection("COMPARISON RESULT  (Mongo ↔ Postgres)");
    printKeyValue("Zone ID", ZONE_ID);
    printKeyValue("Playlist Date", PLAYLIST_DATE);
    printKeyValue("Total Mongo names", mongoNames.size);
    printKeyValue("Total Postgres names", pgNames.size);
    printKeyValue("Present in Postgres", present.length);
    printKeyValue("Missing in Postgres", missing.length);
    printKeyValue("Match percentage", `${matchPercentage}%`);

    if (missing.length > 0) {
      console.log("\n  Missing tracks (first 30):");
      missing.slice(0, 30).forEach(n => console.log(`    - ${n}`));
    }

    console.log("\n" + "─".repeat(60));
  } catch (err) {
    console.error("❌ Comparison failed");
    console.error(err.message);
    console.error(err.stack);
  } finally {
    if (mongo) await mongo.close();
    await pgClient.end();
  }
}

compareNestedMongoNames();