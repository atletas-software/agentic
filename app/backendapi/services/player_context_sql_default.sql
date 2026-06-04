-- Default personal-context SQL (Sportal / MySQL).
-- Single-player sync binds: :player_user_id (required).
-- Optional full-workspace sync: replace the base subquery with your player list or remove the filter.

SELECT
    base.player_user_id,
    CONCAT(p.first_name, ' ', p.last_name) AS player_name,

    COALESCE(
        c.name,
        (
            SELECT cl.name
            FROM evaluation ex
            INNER JOIN club cl ON cl.id = ex.club_id
            WHERE ex.reviewee_id = base.player_user_id
              AND ex.deleted = 0
              AND ex.club_id IS NOT NULL
            ORDER BY ex.last_updated DESC
            LIMIT 1
        ),
        (
            SELECT GROUP_CONCAT(DISTINCT cl.name ORDER BY cl.name SEPARATOR ', ')
            FROM club_user cu
            INNER JOIN club cl ON cl.id = cu.club_members_id
            WHERE cu.user_id = base.player_user_id
        ),
        (
            SELECT GROUP_CONCAT(DISTINCT cl.name ORDER BY cl.name SEPARATOR ', ')
            FROM user_role ur
            INNER JOIN club cl ON cl.id = ur.club_id
            WHERE ur.user_id = base.player_user_id
              AND ur.club_id IS NOT NULL
        )
    ) AS club_name,

    (
        SELECT NULLIF(TRIM(p2.profile_text), '')
        FROM profile p2
        WHERE p2.user_id = base.player_user_id
          AND p2.profile_text IS NOT NULL
          AND TRIM(p2.profile_text) <> ''
        ORDER BY (p2.club_id IS NOT NULL) DESC, p2.last_updated DESC
        LIMIT 1
    ) AS profile_text,

    (
        SELECT COALESCE(JSON_ARRAYAGG(
            JSON_OBJECT(
                'summary',     NULLIF(TRIM(v.summary), ''),
                'description', NULLIF(TRIM(v.description), '')
            )
        ), JSON_ARRAY())
        FROM video v
        WHERE v.creator_id = base.player_user_id
          AND v.deleted = 0
          AND (
              (v.summary IS NOT NULL AND TRIM(v.summary) <> '')
              OR (v.description IS NOT NULL AND TRIM(v.description) <> '')
          )
    ) AS videos,

    JSON_OBJECT(
        'notes', COALESCE((
            SELECT JSON_ARRAYAGG(j.feedback_note)
            FROM (
                SELECT NULLIF(TRIM(po.content), '') AS feedback_note
                FROM evaluation e
                INNER JOIN post po
                    ON po.ref_type = 'com.sportal.model.content.Evaluation'
                   AND po.ref_id = CAST(e.id AS CHAR)
                WHERE e.reviewee_id = base.player_user_id
                  AND e.deleted = 0
                  AND po.content IS NOT NULL
                  AND TRIM(po.content) <> ''
                  AND TRIM(po.content) <> 'No notes'
                ORDER BY e.id, po.id
            ) j
        ), JSON_ARRAY()),

        'video_annotations', COALESCE((
            SELECT JSON_ARRAYAGG(j.feedback_text)
            FROM (
                SELECT NULLIF(TRIM(a.text), '') AS feedback_text
                FROM evaluation e
                INNER JOIN video v2
                    ON v2.evaluation_id = e.id
                   AND v2.deleted = 0
                INNER JOIN video_marker vm ON vm.video_id = v2.id
                INNER JOIN annotation a ON a.id = vm.annotation_id
                WHERE e.reviewee_id = base.player_user_id
                  AND e.deleted = 0
                  AND a.text IS NOT NULL
                  AND TRIM(a.text) <> ''
                  AND TRIM(a.text) <> 'Audio text'
                  AND TRIM(a.text) NOT LIKE 'https://%'
                ORDER BY e.id, vm.id
            ) j
        ), JSON_ARRAY())
    ) AS feedback

FROM (
    SELECT :player_user_id AS player_user_id
) base
INNER JOIN profile p ON p.user_id = base.player_user_id
LEFT JOIN club c ON c.id = p.club_id
GROUP BY
    base.player_user_id,
    p.first_name,
    p.last_name,
    c.name
ORDER BY base.player_user_id;
