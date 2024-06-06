
DROP TABLE IF EXISTS mauroalberelli_coderhouse.stage_spotify_new_releases_table;

CREATE TABLE stage_spotify_new_releases_table(
    Id              VARCHAR(200)
,   Album_type      VARCHAR(50)
,   Album_name      VARCHAR(50)
,   Artis_name      VARCHAR(50)
,   Total_tracks    VARCHAR(50)
,   Album_genre     VARCHAR(500)
,   Realese_date    DATE
,   Album_img       VARCHAR(200)
,   Album_link      VARCHAR(200)
,   Artist_link     VARCHAR(200)
,   Load_date       DATE


);
