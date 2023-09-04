def model_queries():
    """
    creates facts and dimension tables
    """
    dim_teams_query = """
        WITH teams AS (
        SELECT
            match_hometeam_id::INT AS id,
            match_hometeam_name AS team_name,
            team_home_badge AS badge,
            country_id::INT,
            country_name,
            MIN(match_date) AS first_match
        FROM apifootball.matches
        GROUP BY 1,2,3,4,5
        UNION ALL
        SELECT
            match_awayteam_id::INT AS id,
            match_awayteam_name AS team_name,
            team_away_badge AS badge,
            country_id::INT,
            country_name,
            MIN(match_date) AS first_match
        FROM apifootball.matches
        GROUP BY 1,2,3,4,5
    )
    SELECT DISTINCT ON (id)
        id,
        team_name,
        badge,
        country_id,
        country_name
    FROM teams
    ORDER BY id, first_match
    """

    fact_matches_query = """
    SELECT
        matches.match_id::BIGINT AS match_id,
        matches.country_id::BIGINT AS country_id,
        matches.league_id::BIGINT AS league_id,
        matches.match_status,
        matches.match_date::DATE AS match_date,
        matches.match_time::TIME AS match_time,
        matches.match_hometeam_id::BIGINT AS home_team_id,
        matches.match_awayteam_id::BIGINT AS away_team_id,
        matches.match_hometeam_score::BIGINT AS home_team_score,
        matches.match_awayteam_score::BIGINT AS away_team_score,
        matches.match_hometeam_halftime_score::BIGINT AS home_team_ht_score,
        matches.match_awayteam_halftime_score::BIGINT AS away_team_ht_score,
        matches.match_hometeam_extra_score::BIGINT AS home_team_et_score,
        matches.match_awayteam_extra_score::BIGINT AS away_team_et_score,
        matches.match_hometeam_penalty_score::BIGINT AS home_team_penalty_score,
        matches.match_awayteam_penalty_score::BIGINT AS away_team_penalty_score,
        matches.match_hometeam_ft_score::BIGINT AS home_team_ft_score,
        matches.match_awayteam_ft_score::BIGINT AS away_team_ft_score,
        matches.match_hometeam_system AS home_team_system,
        matches.match_awayteam_system AS away_team_system,
        matches.match_round::BIGINT as match_round,
        matches.match_stadium AS stadium,
        matches.match_referee AS referee,
        matches.league_year,
        home_coach.player_key::BIGINT AS home_coach_id,
        away_coach.player_key::BIGINT AS away_coach_id,
        CASE 
            WHEN matches.match_hometeam_score::BIGINT = matches.match_awayteam_score::BIGINT THEN 'D'
            WHEN matches.match_hometeam_score::BIGINT > matches.match_awayteam_score::BIGINT THEN 'HW'
            WHEN matches.match_hometeam_score::BIGINT < matches.match_awayteam_score::BIGINT THEN 'AW'
            END AS outcome
    FROM apifootball.matches matches
    LEFT JOIN apifootball.lineup_home_coach home_coach
        ON home_coach.match_id = matches.match_id
    LEFT JOIN apifootball.lineup_away_coach away_coach
            ON away_coach.match_id = matches.match_id
    """

    fact_match_goalscorers_query = """
    SELECT
        match_id || '-' || COALESCE(home_scorer_id, away_scorer_id)|| '-'  || score AS goal_id,
        match_id::BIGINT AS match_id,
        COALESCE(home_scorer_id, away_scorer_id)::BIGINT AS scorer_player_id,
        COALESCE(home_assist_id, away_assist_id)::BIGINT AS assist_player_id,
        score,
        CASE WHEN home_scorer LIKE '%(o.g)%' OR away_scorer LIKE '%(o.g)%' THEN True ELSE  FALSE END AS is_own_goal,
        CASE WHEN info = 'Penalty' THEN True ELSE false END AS is_penalty_kick,
        split_part(time, '+', 1)::INT AS goal_time,
        NULLIF(split_part(time, '+', 2), '')::INT AS goal_stoppage_time,
        NULLIF(split_part(time, '+', 2), '') IS NOT NULL AS is_stoppage_time_goal,
        CASE WHEN home_scorer_id IS NOT NULL THEN True
                WHEN away_scorer_id IS NOT NULL THEN False END AS is_home_goal,
        CASE WHEN score_info_time = '1st Half' THEN 1 WHEN score_info_time = '2nd Half' THEN 2 END AS half
    FROM apifootball.goalscorer
    """

    fact_match_player_lineups_query = """
    SELECT
        match_id || '-' || player_key AS player_lineup_id,
        match_id::BIGINT as match_id,
        player_key::BIGINT AS player_id,
        lineup_position::INT AS lineup_position,
        'STARTING' AS player_type,
        True AS is_home_lineup
    FROM apifootball.lineup_home_starting_lineups

    UNION ALL

    SELECT
        match_id || '-' || player_key AS player_lineup_id,
        match_id::BIGINT as match_id,
        player_key::BIGINT AS player_id,
        lineup_position::INT AS lineup_position,
        'STARTING' AS player_type,
        False AS is_home_lineup
    FROM apifootball.lineup_away_starting_lineups

    UNION ALL

    SELECT
        match_id || '-' || player_key AS player_lineup_id,
        match_id::BIGINT as match_id,
        player_key::BIGINT AS player_id,
        lineup_position::INT AS lineup_position,
        'SUBSTITUTE' AS player_type,
        True AS is_home_lineup
    FROM apifootball.lineup_home_substitutes

    UNION ALL

    SELECT
        match_id || '-' || player_key AS player_lineup_id,
        match_id::BIGINT as match_id,
        player_key::BIGINT AS player_id,
        lineup_position::INT AS lineup_position,
        'SUBSTITUTE' AS player_type,
        False AS is_home_lineup
    FROM apifootball.lineup_away_substitutes
    """

    dim_players_query = """
    WITH players AS (
            SELECT
            player_key,
            lineup_player,
            lineup_number,
            match_hometeam_id AS team_id,
            MAX(match_date) AS last_match_date
        FROM apifootball.lineup_home_starting_lineups
        JOIN apifootball.matches
            USING (match_id)
        GROUP BY 1,2,3,4

        UNION ALL

            SELECT
                player_key,
                lineup_player,
                lineup_number,
                match_hometeam_id AS team_id,
                MAX(match_date) AS last_match_date
            FROM apifootball.lineup_home_substitutes
            JOIN apifootball.matches
                USING (match_id)
            GROUP BY 1,2,3,4

            UNION ALL

            SELECT
                player_key,
                lineup_player,
                lineup_number,
                match_awayteam_id AS team_id,
                MAX(match_date) AS last_match_date
            FROM apifootball.lineup_away_starting_lineups
            JOIN apifootball.matches
                USING (match_id)
            GROUP BY 1,2,3,4

            UNION ALL

            SELECT
                player_key,
                lineup_player,
                lineup_number,
                match_awayteam_id AS team_id,
                MAX(match_date) AS last_match_date
            FROM apifootball.lineup_away_substitutes
            JOIN apifootball.matches
                USING (match_id)
            GROUP BY 1,2,3,4
    )
    SELECT DISTINCT ON (player_key)
        player_key::BIGINT AS player_id,
        lineup_player AS player_name,
        lineup_number::INT AS last_known_shirt_number,
        team_id::BIGINT AS last_known_team_id
    FROM players
    ORDER BY player_key, last_match_date DESC
    """

    dim_coaches_query = """
    WITH coaches AS (
        SELECT
            player_key,
            lineup_player,
            match_hometeam_id AS team_id,
            MAX(match_date) AS last_match_date
        FROM apifootball.lineup_home_coach
                JOIN apifootball.matches
                    USING (match_id)
        GROUP BY 1,2,3

        UNION ALL

        SELECT
            player_key,
            lineup_player,
            match_awayteam_id AS team_id,
            MAX(match_date) AS last_match_date
        FROM apifootball.lineup_away_coach
                JOIN apifootball.matches
                    USING (match_id)
        GROUP BY 1,2,3
    )
    SELECT DISTINCT ON (player_key)
        player_key::BIGINT AS coach_id,
        lineup_player AS coach_name,
        team_id AS last_known_team_id
    FROM coaches
    ORDER BY player_key, last_match_date DESC    
    """

    fact_match_cards_query = """
    SELECT
        COALESCE(match_id || '-' || COALESCE(away_player_id, home_player_id) || '-' || time || '-'|| card) AS card_id,
        match_id::BIGINT,
        COALESCE(away_player_id, home_player_id)::BIGINT AS player_id,
        card,
        CASE WHEN score_info_time = '1st Half' THEN 1 WHEN score_info_time = '2nd Half' THEN 2 END AS half,
        time
    FROM apifootball.cards    
"""
    output = {
        "fact_matches": fact_matches_query,
        "fact_match_goalscorers": fact_match_goalscorers_query,
        "fact_match_player_lineups": fact_match_player_lineups_query,
        "dim_teams": dim_teams_query,
        "dim_players": dim_players_query,
        "dim_coaches": dim_coaches_query,
        "fact_match_cards": fact_match_cards_query
    }

    return output