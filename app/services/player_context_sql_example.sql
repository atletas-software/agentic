-- Example PLAYER_CONTEXT_SQL (Sportal / MySQL). Paste into Admin → Player Memory → SQL.
-- Binds: :first_name, :last_name (single-player sync). Use reviewee_id (not player_id alias) for vectors.

SELECT
    -- Canonical athlete identity (reviewee_id required by sql_player_sync)
    e.reviewee_id AS reviewee_id,
    MAX(TRIM(SUBSTRING_INDEX(e.reviewee_name, ',', -1))) AS first_name,
    MAX(TRIM(SUBSTRING_INDEX(e.reviewee_name, ',', 1))) AS last_name,

    -- Profile
    MIN(p.id) AS profile_id,
    MAX(e.email) AS profile_email,

    -- Club
    MIN(c.id) AS club_id,
    MAX(c.name) AS club_name,

    -- Evaluation
    e.id AS evaluation_id,
    e.form_id,
    e.status,
    e.completion_date,
    e.reviewee_name,
    e.review_period_start,
    e.review_period_end,
    e.values_json,

    -- Video
    v.id AS video_id,
    v.name AS video_name,
    v.summary AS video_summary,
    v.description AS video_description,
    v.duration_sec,
    v.sport_id,
    v.sport_position_id,
    v.video_category_id

FROM sportal.evaluation AS e

LEFT JOIN sportal.profile AS p
    ON LOWER(p.email) = LOWER(e.email)

LEFT JOIN sportal.club AS c
    ON LOWER(c.email) = LOWER(e.email)

LEFT JOIN sportal.video AS v
    ON v.evaluation_id = e.id

WHERE LOWER(e.reviewee_name) LIKE LOWER(CONCAT('%', :first_name, '%'))
  AND LOWER(e.reviewee_name) LIKE LOWER(CONCAT('%', :last_name, '%'))
  AND e.status = 'COMPLETE'
  AND e.values_json IS NOT NULL

GROUP BY
    e.id,
    e.reviewee_id,
    e.reviewee_name,
    e.form_id,
    e.status,
    e.completion_date,
    e.review_period_start,
    e.review_period_end,
    e.values_json,
    v.id,
    v.name,
    v.summary,
    v.description,
    v.duration_sec,
    v.sport_id,
    v.sport_position_id,
    v.video_category_id

ORDER BY
    e.completion_date DESC;

-- Full-workspace sync: drop or change the :first_name / :last_name predicates as needed.
