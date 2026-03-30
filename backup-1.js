const { MongoClient } = require("mongodb");
const { Client: PgClient } = require("pg");

const MONGO_URI = "mongodb://localhost:27018";
const MONGO_DB = "status";
const MONGO_COLLECTION = "media_list";

const pgClient = new PgClient({
  host: "localhost",
  port: 5433,
  user: "postgres",
  password: "reallybadpassword",
  database: "retail_radio",
  ssl: false,
});

const ZONE_ID = 6870;

// ── SQL: Aggregated rules via station media group hierarchy ──────────────────
// Fixed: uses LIMIT 1 with proper ORDER BY weight ASC, id DESC for policy
// selection; adds group-level artist/song exclusions from scheduler_artistexclusion
// and scheduler_songexclusion (were completely missing before).
const GET_AGGREGATED_RULES_SQL = `
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
    WHERE sg.allow_holiday_override = TRUE
      AND (sp.start_date IS NULL OR sp.start_date <= CURRENT_DATE)
      AND (sp.end_date IS NULL OR sp.end_date >= CURRENT_DATE)
      AND (
          sp.day_of_week_mask IS NULL
          OR sp.day_of_week_mask = 0
          OR (sp.day_of_week_mask & (1 << EXTRACT(DOW FROM CURRENT_DATE)::int)) > 0
      )
),
-- Fixed: pick the single best holiday policy (weight ASC, id DESC)
best_holiday_policy AS (
    SELECT station_id, supported_format, owner
    FROM holiday_policies
    ORDER BY weight ASC, id DESC
    LIMIT 1
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
    WHERE NOT EXISTS (
            SELECT 1
            FROM scheduler_stationmediagroup_holiday_program smghp
            WHERE smghp.stationpolicy_id = sp.id
          )
      AND (sp.start_date IS NULL OR sp.start_date <= CURRENT_DATE)
      AND (sp.end_date IS NULL OR sp.end_date >= CURRENT_DATE)
      AND (
          sp.day_of_week_mask IS NULL
          OR sp.day_of_week_mask = 0
          OR (sp.day_of_week_mask & (1 << EXTRACT(DOW FROM CURRENT_DATE)::int)) > 0
      )
),
-- Fixed: pick the single best regular policy (weight ASC, id DESC)
best_regular_policy AS (
    SELECT station_id, supported_format, owner
    FROM regular_policies
    WHERE NOT EXISTS (SELECT 1 FROM best_holiday_policy)
    ORDER BY weight ASC, id DESC
    LIMIT 1
),
default_station_policies AS (
    SELECT
        dsp.station_id,
        sg.supported_format,
        sg.owner,
        dsp.combine_rule,
        ROW_NUMBER() OVER (
            ORDER BY dsp.combine_rule DESC, sg.basegroup_ptr_id ASC
        ) AS rank
    FROM station_group_chain sg
    JOIN scheduler_defaultstationpolicy dsp
        ON dsp.group_id = sg.basegroup_ptr_id
),
best_default_policy AS (
    SELECT station_id, supported_format, owner
    FROM default_station_policies
    WHERE rank = 1
),
active_policy AS (
    SELECT station_id, supported_format, owner, 1 AS priority
    FROM best_holiday_policy
    UNION ALL
    SELECT station_id, supported_format, owner, 2 AS priority
    FROM best_regular_policy
    UNION ALL
    SELECT station_id, supported_format, owner, 3 AS priority
    FROM best_default_policy
    WHERE NOT EXISTS (SELECT 1 FROM best_holiday_policy)
      AND NOT EXISTS (SELECT 1 FROM best_regular_policy)
    UNION ALL
    SELECT
        19865 AS station_id,
        sg.supported_format,
        sg.owner,
        4 AS priority
    FROM (SELECT supported_format, owner FROM station_group_chain LIMIT 1) sg
    WHERE NOT EXISTS (SELECT 1 FROM best_holiday_policy)
      AND NOT EXISTS (SELECT 1 FROM best_regular_policy)
      AND NOT EXISTS (SELECT 1 FROM best_default_policy)
),
selected_policy AS (
    SELECT * FROM active_policy ORDER BY priority LIMIT 1
),
effective_medialist AS (
    SELECT supported_format, owner, station_id AS media_list_id
    FROM selected_policy
),
station_tree AS (
    SELECT
        em.supported_format,
        em.owner,
        em.media_list_id,
        sm.base_station_id,
        sm.station_id
    FROM effective_medialist em
    JOIN scheduler_mixpolicy sm ON sm.base_station_id = em.media_list_id
    UNION ALL
    SELECT
        st.supported_format,
        st.owner,
        st.media_list_id,
        sm.base_station_id,
        sm.station_id
    FROM scheduler_mixpolicy sm
    INNER JOIN station_tree st ON sm.base_station_id = st.station_id
),
all_medialist_ids AS (
    SELECT supported_format, owner, media_list_id AS id FROM effective_medialist
    UNION
    SELECT supported_format, owner, base_station_id FROM station_tree
    UNION
    SELECT supported_format, owner, station_id FROM station_tree
),
failsafe_tag AS (
    SELECT id AS tag_id FROM production_tag WHERE name = 'P_Failsafe2021' LIMIT 1
),
-- Fixed: also collect group-level exclusions from scheduler_artistexclusion
-- and scheduler_songexclusion (ref queries 12–14) which were missing entirely
group_level_banned_artists AS (
    SELECT ae.artist_id
    FROM scheduler_artistexclusion ae
    WHERE ae.group_id IN (SELECT basegroup_ptr_id FROM station_group_chain)
),
group_level_banned_songs AS (
    SELECT se.song_id
    FROM scheduler_songexclusion se
    WHERE se.group_id IN (SELECT basegroup_ptr_id FROM station_group_chain)
)
SELECT
    COALESCE(bit_or(ids.owner), 0)            AS owner_flag,
    COALESCE(bit_or(ids.supported_format), 0) AS media_flag,
    COALESCE(
        NULLIF(ARRAY_REMOVE(ARRAY_AGG(DISTINCT str.tag_id), NULL), '{}'),
        ARRAY(SELECT tag_id FROM failsafe_tag)
    )                                          AS include_tags,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT smlet.tag_id), NULL)   AS exclude_tags,
    -- Merge medialist-level + group-level banned artists
    ARRAY(
        SELECT DISTINCT x FROM UNNEST(
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT smlba.artist_id), NULL) ||
            ARRAY(SELECT artist_id FROM group_level_banned_artists)
        ) AS x WHERE x IS NOT NULL
    )                                          AS banned_artists,
    -- Merge medialist-level + group-level banned songs
    ARRAY(
        SELECT DISTINCT x FROM UNNEST(
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT smlbs.song_id), NULL) ||
            ARRAY(SELECT song_id FROM group_level_banned_songs)
        ) AS x WHERE x IS NOT NULL
    )                                          AS banned_songs,
    ARRAY_AGG(DISTINCT ids.id)                 AS all_medialist_ids,
    (SELECT media_list_id FROM effective_medialist LIMIT 1) AS effective_medialist_id,
    (SELECT priority FROM active_policy ORDER BY priority LIMIT 1) AS selected_priority
FROM all_medialist_ids ids
LEFT JOIN scheduler_tagrule str
    ON ids.id = str.media_list_id
LEFT JOIN scheduler_medialist_excluded_tags smlet
    ON ids.id = smlet.medialist_id
LEFT JOIN scheduler_medialist_banned_artists smlba
    ON ids.id = smlba.medialist_id
LEFT JOIN scheduler_medialist_banned_songs smlbs
    ON ids.id = smlbs.medialist_id;
`;

// ── SQL: Playable songs (mirrors aggregated rules fixes above) ───────────────
const GET_ALL_DATA_SQL = `
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
    SELECT sp.station_id, sg.supported_format, sg.owner, sp.weight, sp.id
    FROM station_group_chain sg
    JOIN scheduler_stationmediagroup_holiday_program smghp
        ON smghp.stationmediagroup_id = sg.basegroup_ptr_id
    JOIN scheduler_stationpolicy sp ON sp.id = smghp.stationpolicy_id
    WHERE sg.allow_holiday_override = TRUE
      AND (sp.start_date IS NULL OR sp.start_date <= CURRENT_DATE)
      AND (sp.end_date IS NULL OR sp.end_date >= CURRENT_DATE)
      AND (sp.day_of_week_mask IS NULL OR sp.day_of_week_mask = 0
           OR (sp.day_of_week_mask & (1 << EXTRACT(DOW FROM CURRENT_DATE)::int)) > 0)
),
best_holiday_policy AS (
    SELECT station_id, supported_format, owner
    FROM holiday_policies
    ORDER BY weight ASC, id DESC
    LIMIT 1
),
regular_policies AS (
    SELECT sp.station_id, sg.supported_format, sg.owner, sp.weight, sp.id
    FROM station_group_chain sg
    JOIN scheduler_stationpolicy sp ON sp.group_id = sg.basegroup_ptr_id
    WHERE NOT EXISTS (
            SELECT 1 FROM scheduler_stationmediagroup_holiday_program smghp
            WHERE smghp.stationpolicy_id = sp.id
          )
      AND (sp.start_date IS NULL OR sp.start_date <= CURRENT_DATE)
      AND (sp.end_date IS NULL OR sp.end_date >= CURRENT_DATE)
      AND (sp.day_of_week_mask IS NULL OR sp.day_of_week_mask = 0
           OR (sp.day_of_week_mask & (1 << EXTRACT(DOW FROM CURRENT_DATE)::int)) > 0)
),
best_regular_policy AS (
    SELECT station_id, supported_format, owner
    FROM regular_policies
    WHERE NOT EXISTS (SELECT 1 FROM best_holiday_policy)
    ORDER BY weight ASC, id DESC
    LIMIT 1
),
default_station_policies AS (
    SELECT
        dsp.station_id,
        sg.supported_format,
        sg.owner,
        dsp.combine_rule,
        ROW_NUMBER() OVER (ORDER BY dsp.combine_rule DESC, sg.basegroup_ptr_id ASC) AS rank
    FROM station_group_chain sg
    JOIN scheduler_defaultstationpolicy dsp ON dsp.group_id = sg.basegroup_ptr_id
),
best_default_policy AS (
    SELECT station_id, supported_format, owner
    FROM default_station_policies WHERE rank = 1
),
active_policy AS (
    SELECT station_id, supported_format, owner, 1 AS priority FROM best_holiday_policy
    UNION ALL
    SELECT station_id, supported_format, owner, 2 AS priority FROM best_regular_policy
    UNION ALL
    SELECT station_id, supported_format, owner, 3 AS priority FROM best_default_policy
    WHERE NOT EXISTS (SELECT 1 FROM best_holiday_policy)
      AND NOT EXISTS (SELECT 1 FROM best_regular_policy)
    UNION ALL
    SELECT 19865, sg.supported_format, sg.owner, 4
    FROM (SELECT supported_format, owner FROM station_group_chain LIMIT 1) sg
    WHERE NOT EXISTS (SELECT 1 FROM best_holiday_policy)
      AND NOT EXISTS (SELECT 1 FROM best_regular_policy)
      AND NOT EXISTS (SELECT 1 FROM best_default_policy)
),
selected_policy AS (SELECT * FROM active_policy ORDER BY priority LIMIT 1),
effective_medialist AS (
    SELECT supported_format, owner, station_id AS media_list_id FROM selected_policy
),
station_tree AS (
    SELECT em.supported_format, em.owner, em.media_list_id, sm.base_station_id, sm.station_id
    FROM effective_medialist em
    JOIN scheduler_mixpolicy sm ON sm.base_station_id = em.media_list_id
    UNION ALL
    SELECT st.supported_format, st.owner, st.media_list_id, sm.base_station_id, sm.station_id
    FROM scheduler_mixpolicy sm
    INNER JOIN station_tree st ON sm.base_station_id = st.station_id
),
all_medialist_ids AS (
    SELECT media_list_id AS id FROM effective_medialist
    UNION
    SELECT base_station_id FROM station_tree
    UNION
    SELECT station_id FROM station_tree
),
failsafe_tag AS (
    SELECT id AS tag_id FROM production_tag WHERE name = 'P_Failsafe2021' LIMIT 1
),
group_level_banned_artists AS (
    SELECT ae.artist_id
    FROM scheduler_artistexclusion ae
    WHERE ae.group_id IN (SELECT basegroup_ptr_id FROM station_group_chain)
),
group_level_banned_songs AS (
    SELECT se.song_id
    FROM scheduler_songexclusion se
    WHERE se.group_id IN (SELECT basegroup_ptr_id FROM station_group_chain)
),
aggregated_rules AS (
    SELECT
        COALESCE(
            NULLIF(ARRAY_REMOVE(ARRAY_AGG(DISTINCT str.tag_id), NULL), '{}'),
            ARRAY(SELECT tag_id FROM failsafe_tag)
        ) AS include_tags,
        ARRAY_REMOVE(ARRAY_AGG(DISTINCT smlet.tag_id), NULL) AS exclude_tags,
        ARRAY(
            SELECT DISTINCT x FROM UNNEST(
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT smlba.artist_id), NULL) ||
                ARRAY(SELECT artist_id FROM group_level_banned_artists)
            ) AS x WHERE x IS NOT NULL
        ) AS banned_artists,
        ARRAY(
            SELECT DISTINCT x FROM UNNEST(
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT smlbs.song_id), NULL) ||
                ARRAY(SELECT song_id FROM group_level_banned_songs)
            ) AS x WHERE x IS NOT NULL
        ) AS banned_songs
    FROM all_medialist_ids ids
    LEFT JOIN scheduler_tagrule str       ON ids.id = str.media_list_id
    LEFT JOIN scheduler_medialist_excluded_tags smlet ON ids.id = smlet.medialist_id
    LEFT JOIN scheduler_medialist_banned_artists smlba ON ids.id = smlba.medialist_id
    LEFT JOIN scheduler_medialist_banned_songs smlbs   ON ids.id = smlbs.medialist_id
)
SELECT DISTINCT pm.id, pm.name
FROM production_media pm
LEFT JOIN production_song ps ON ps.media_ptr_id = pm.id
CROSS JOIN aggregated_rules r
WHERE
    (r.banned_artists IS NULL OR ps.artist_primary_id <> ALL(r.banned_artists))
    AND (r.banned_songs IS NULL OR pm.id <> ALL(r.banned_songs))
    AND NOT EXISTS (
        SELECT 1
        FROM production_song_tags pst_ex
        WHERE pst_ex.song_id = pm.id
          AND r.exclude_tags IS NOT NULL
          AND pst_ex.tag_id = ANY(r.exclude_tags)
    )
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

const GET_TAG_NAMES_SQL    = `SELECT id, name FROM production_tag    WHERE id = ANY($1);`;
const GET_ARTIST_NAMES_SQL = `SELECT id, name FROM production_artist WHERE id = ANY($1);`;
const GET_SONG_NAMES_SQL   = `SELECT id, name FROM production_media   WHERE id = ANY($1);`;

// ── Fixed: walk scheduler_mediagroup hierarchy to find ordering_id via
// scheduler_mediapolicy (ref queries 5–6), not via the station group path.
// The original used stores_zone → station_media_group which is wrong for media ordering.
const GET_ORDER_ID_SQL = `
WITH RECURSIVE mediagroup_hierarchy AS (
    SELECT
        mg.basegroup_ptr_id,
        mg.parent_id
    FROM stores_zone sz
    JOIN scheduler_mediagroup mg ON mg.basegroup_ptr_id = sz.media_group_id
    WHERE sz.id = $1
    UNION ALL
    SELECT
        parent_mg.basegroup_ptr_id,
        parent_mg.parent_id
    FROM scheduler_mediagroup parent_mg
    INNER JOIN mediagroup_hierarchy mh
        ON parent_mg.basegroup_ptr_id = mh.parent_id
)
SELECT mp.ordering_id
FROM scheduler_mediapolicy mp
WHERE mp.group_id IN (SELECT basegroup_ptr_id FROM mediagroup_hierarchy)
  AND mp.ordering_id IS NOT NULL
  AND (mp.start_date IS NULL OR mp.start_date <= CURRENT_DATE)
  AND (mp.end_date IS NULL OR mp.end_date >= CURRENT_DATE)
  AND (
      mp.day_of_week_mask IS NULL
      OR mp.day_of_week_mask = 0
      OR (mp.day_of_week_mask & (1 << EXTRACT(DOW FROM CURRENT_DATE)::int)) > 0
  )
ORDER BY mp.weight ASC, mp.id DESC
LIMIT 1;
`;

// ── Fixed: use scheduler_orderingpolicy joined to scheduler_ordering (ref query 8 & 15)
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

// ── Helpers ──────────────────────────────────────────────────────────────────
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
  console.log(`${line}`);
}

function printKeyValue(key, value) {
  console.log(`  ${key.padEnd(28)}: ${value}`);
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function compareNestedMongoNames() {
  let mongo;
  try {
    // ── MongoDB ──────────────────────────────────────────────────────────────
    mongo = await MongoClient.connect(MONGO_URI, { serverSelectionTimeoutMS: 5000 });
    const docs = await mongo
      .db(MONGO_DB)
      .collection(MONGO_COLLECTION)
      .find({})
      .toArray();

    const mongoNames = new Set();
    for (const doc of docs) {
      if (!Array.isArray(doc.list)) continue;
      for (const item of doc.list) {
        if (item?.name) mongoNames.add(normalizeName(item.name));
      }
    }

    // ── Postgres ─────────────────────────────────────────────────────────────
    await pgClient.connect();

    // Pull aggregated rules for diagnostics
    const rulesRes = await pgClient.query(GET_AGGREGATED_RULES_SQL, [ZONE_ID]);
    const rules = rulesRes.rows[0] || {};
    const includeTags   = rules.include_tags   || [];
    const excludeTags   = rules.exclude_tags   || [];
    const bannedArtists = rules.banned_artists || [];
    const bannedSongs   = rules.banned_songs   || [];

    printSection("AGGREGATED RULES");
    printKeyValue("Selected priority",    rules.selected_priority ?? "n/a");
    printKeyValue("Effective medialist",  rules.effective_medialist_id ?? "n/a");
    printKeyValue("All medialist IDs",    (rules.all_medialist_ids || []).join(", ") || "none");
    printKeyValue("Include tags",         includeTags.length);
    printKeyValue("Exclude tags",         excludeTags.length);
    printKeyValue("Banned artists",       bannedArtists.length);
    printKeyValue("Banned songs",         bannedSongs.length);

    // Resolve names for diagnostics
    const tagIds = [...new Set([...includeTags, ...excludeTags])];
    if (tagIds.length > 0) {
      const tagRes = await pgClient.query(GET_TAG_NAMES_SQL, [tagIds]);
      const tagMap = Object.fromEntries(tagRes.rows.map((r) => [r.id, r.name]));
      console.log("  Include tag names : " + includeTags.map((id) => tagMap[id] ?? id).join(", "));
      console.log("  Exclude tag names : " + excludeTags.map((id) => tagMap[id] ?? id).join(", "));
    }
    if (bannedArtists.length > 0) {
      const artRes = await pgClient.query(GET_ARTIST_NAMES_SQL, [bannedArtists]);
      console.log("  Banned artists    : " + artRes.rows.map((r) => r.name).join(", "));
    }
    if (bannedSongs.length > 0) {
      const songRes = await pgClient.query(GET_SONG_NAMES_SQL, [bannedSongs]);
      console.log("  Banned songs      : " + songRes.rows.map((r) => r.name).join(", "));
    }

    // ── Run the main playable-songs query ────────────────────────────────────
    const pgRes = await pgClient.query(GET_ALL_DATA_SQL, [ZONE_ID]);
    const pgNames = new Set(
      pgRes.rows.map((r) => r.name).filter(Boolean).map(normalizeName)
    );

    // ── Ordering / scheduled media ────────────────────────────────────────────
    // Fixed: now walks media_group hierarchy via scheduler_mediapolicy
    const orderRes = await pgClient.query(GET_ORDER_ID_SQL, [ZONE_ID]);
    if (orderRes.rows.length > 0) {
      const orderingId = orderRes.rows[0].ordering_id;
      printKeyValue("\n  Ordering ID found", orderingId);

      const mediaListRes = await pgClient.query(GET_MEDIA_LIST_IDS_SQL, [orderingId]);
      const mediaListIds = mediaListRes.rows.map((r) => r.media_list_id).filter(Boolean);

      if (mediaListIds.length > 0) {
        printKeyValue("  Order medialist IDs", mediaListIds.join(", "));
        const mediaRes = await pgClient.query(GET_MEDIA_FROM_ORDER_SQL, [mediaListIds]);
        for (const row of mediaRes.rows) pgNames.add(normalizeName(row.name));
        printKeyValue("  Ordered tracks added", mediaRes.rows.length);
      }
    } else {
      console.log("\n  No ordering policy found for this zone.");
    }

    // ── Comparison ────────────────────────────────────────────────────────────
    const present = [];
    const missing = [];
    for (const name of mongoNames) {
      (pgNames.has(name) ? present : missing).push(name);
    }

    const matchPercentage =
      mongoNames.size === 0
        ? 0
        : ((present.length / mongoNames.size) * 100).toFixed(2);

    printSection("COMPARISON RESULT  (Mongo ↔ Postgres)");
    printKeyValue("Total Mongo names",    mongoNames.size);
    printKeyValue("Total Postgres names", pgNames.size);
    printKeyValue("Present in Postgres",  present.length);
    printKeyValue("Missing in Postgres",  missing.length);
    printKeyValue("Match percentage",     `${matchPercentage}%`);

    if (missing.length > 0) {
      console.log("\n  ✗ Missing tracks (first 30):");
      missing.slice(0, 30).forEach((n) => console.log(`    - ${n}`));
    }

    console.log("\n" + "─".repeat(60));
  } catch (err) {
    console.error("Comparison failed");
    console.error(err.message);
    console.error(err.stack);
  } finally {
    if (mongo) await mongo.close();
    await pgClient.end();
  }
}

compareNestedMongoNames();
