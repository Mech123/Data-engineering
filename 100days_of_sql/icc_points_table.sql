select team_name, count(1) as no_of_matches_played, sum(win_flag) as no_of_matches_won, count(1) - sum(win_flag) as no_of_losses
from (
    select team_1 as team_name, case when team_1=winner then 1 else 0 end as win_flag
    from icc_world_cup
    union all
    select team_2 as team_name, case when team_2=winner then 1 else 0 end as win_flag
    from icc_world_cup
) A
group by team_name
order by no_of_matches_won desc;