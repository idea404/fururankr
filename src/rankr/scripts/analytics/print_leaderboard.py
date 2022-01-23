import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from rankr.db import create_db_session_from_cfg


def add_emoji(frame_row) -> str:
    accuracy_val = frame_row["accuracy"]
    return_val = frame_row["average_profit"]

    emojis = frame_row["emoji"]
    if accuracy_val > 0.80:
        emojis += "🎯"

    if return_val > 0.80:
        emojis += "💰"

    return emojis


def format_percent_as_str(percentage: float) -> str:
    s = str(int(round(percentage, 2) * 100))
    if percentage >= 0:
        return "+" + s + "%"
    return s + "%"


def format_days_as_str(days_float: float) -> str:
    rounded = round(days_float)
    return str(rounded) + "d"


def get_leaderboard(dbsess: Session) -> pd.DataFrame:
    query = text(
        """
                SELECT
                       f.handle,
                       f.accuracy,
                       f.performance_score,
                       f.total_trades_measured,
                       f.average_profit,
                       f.average_loss,
                       f.average_holding_period_days
                FROM furu f
                WHERE f.accuracy > 0.55 
                    AND f.performance_score > 0.3 
                    AND f.total_trades_measured > 30 
                    AND average_holding_period_days > 12
                ORDER BY performance_score DESC
            """
    )
    frame = pd.read_sql(sql=query, con=dbsess.bind)

    frame["emoji"] = ["🥇", "🥈", "🥉"] + ["" for _ in range(3, len(frame))]
    frame["emoji"] = frame.apply(lambda x: add_emoji(x), axis=1)

    frame["handle"] = "@" + frame["handle"]
    frame[["accuracy", "average_profit", "average_loss"]] = frame[
        ["accuracy", "average_profit", "average_loss"]
    ].applymap(format_percent_as_str)
    frame["average_holding_period_days"] = frame["average_holding_period_days"].apply(
        format_days_as_str
    )

    frame["lines"] = (
        frame["handle"]
        + " 💰:"
        + frame["average_profit"]
        + " 🎯:"
        + frame["accuracy"]
        + " 📆:"
        + frame["average_holding_period_days"]
        + " "
        + frame["emoji"]
    )

    return frame


if __name__ == "__main__":
    dbsess = create_db_session_from_cfg(echo=False)
    df = get_leaderboard(dbsess)
    # df.to_csv(path_or_buf='outputs/leaderboard_output.csv', sep=' ', index=False, header=False)
    # df.to_excel('outputs/leaderboard_output.xlsx')

    print(
        df[
            [
                "handle",
                "accuracy",
                "performance_score",
                "total_trades_measured",
                "average_profit",
                "average_loss",
                "average_holding_period_days",
            ]
        ].to_string(index=False)
    )
