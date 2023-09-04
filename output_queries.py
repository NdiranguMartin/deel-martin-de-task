def analytics_queries():
    query_a = """
        WITH home AS (
        SELECT
            home_team_id AS team_id,
            COUNT(match_id) AS matches_played,
            COUNT(*) FILTER ( WHERE outcome = 'HW') AS won,
            COUNT(*) FILTER ( WHERE outcome = 'D') draw,
            COUNT(*) FILTER ( WHERE outcome = 'AW') AS lost,
            SUM(home_team_score) AS goals_scored,
            SUM(away_team_score) AS goals_conceded,
            SUM(CASE WHEN outcome = 'HW' THEN 3
                WHEN outcome = 'D' THEN 1 END) AS points
        FROM analytics.fact_matches
        WHERE
            match_status = 'Finished'
        GROUP BY 1
        ),
        away AS (
            SELECT
                away_team_id AS team_id,
                COUNT(match_id) AS matches_played,
                COUNT(*) FILTER ( WHERE outcome = 'AW') AS won,
                COUNT(*) FILTER ( WHERE outcome = 'D') draw,
                COUNT(*) FILTER ( WHERE outcome = 'HW') AS lost,
                SUM(away_team_score) AS goals_scored,
                SUM(home_team_score) AS goals_conceded,
                SUM(CASE WHEN outcome = 'AW' THEN 3
                        WHEN outcome = 'D' THEN 1 END) AS points
            FROM analytics.fact_matches
            WHERE
                    match_status = 'Finished'
            GROUP BY 1
        ),
        base AS (
            SELECT
                dim_teams.team_name,
                home.matches_played + away.matches_played AS matches_played,
                home.won + away.won AS won,
                home.draw + away.draw AS draw,
                home.lost + away.lost AS lost,
                home.goals_scored + away.goals_scored AS goals_scored,
                home.goals_conceded + away.goals_conceded AS goals_conceded,
                home.points + away.points AS points
            FROM analytics.dim_teams
            JOIN home ON home.team_id = dim_teams.id
            JOIN away ON away.team_id = dim_teams.id
        )
        SELECT
            row_number() over (ORDER BY points DESC, goals_scored - goals_conceded DESC, goals_scored DESC, goals_conceded ASC, won DESC) AS position,
            team_name,
            matches_played,
            won,
            draw,
            lost,
            goals_scored,
            goals_conceded,
            points
        FROM base
        ORDER BY 1    
    """

    query_b = """
        SELECT
            dim_teams.team_name,
            SUM(away_team_score) AS goals
        FROM analytics.dim_teams
        JOIN analytics.fact_matches ON dim_teams.id = fact_matches.away_team_id
        WHERE match_status = 'Finished'
        GROUP BY 1
        ORDER BY goals DESC, team_name
        """


    query_c = """
        SELECT
            referee,
            COUNT(card_id)
        FROM analytics.fact_match_cards cards
        JOIN analytics.fact_matches matches USING(match_id)
        GROUP BY 1
        ORDER BY 2 DESC, 1
        """
    query_d = """
        SELECT
            players.player_name,
            teams.team_name,
            COUNT(scorers.goal_id) AS goals
        FROM analytics.fact_matches matches
        JOIN analytics.fact_match_goalscorers scorers USING (match_id)
        JOIN analytics.dim_players players ON players.player_id = scorers.scorer_player_id
        JOIN analytics.dim_teams teams ON teams.id = players.last_known_team_id
        WHERE
            matches.match_round <= 14
            AND NOT scorers.is_own_goal
        GROUP BY
            1,2
        ORDER BY 3 DESC, 1
        LIMIT 3
        """

    output = {
        "query_a": query_a,
        "query_b": query_b,
        "query_c": query_c,
        "query_d": query_d
    }

    return output