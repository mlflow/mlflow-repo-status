import logging
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.INFO)


def count_by_month(df, datetime_col):
    first_col = df.columns[0]
    df = (
        (
            df.groupby([df[datetime_col].dt.year, df[datetime_col].dt.month])
            .count()[[first_col]]
            .rename(columns={first_col: "count"})
        )
        .pipe(
            lambda df_: (df_.set_index(df_.index.map(lambda year_month: datetime(*year_month, 1))))
        )
        .reset_index()
        .rename(columns={"index": "date"})
    )
    return df


def make_plot(*traces, title):
    fig = go.Figure()
    fig.update_layout(title={"text": title, "font": dict(size=25)}, yaxis_title="Count")
    fig.add_traces(traces)
    return fig


def main():
    pd.options.plotting.backend = "plotly"
    dist_dir = Path("dist")
    if dist_dir.exists():
        shutil.rmtree(dist_dir)
    dist_assets = dist_dir.joinpath("assets")
    plots_dir = dist_assets.joinpath("plots")
    plots_dir.mkdir(parents=True, exist_ok=True)
    db_path = Path("github.sqlite")

    now = datetime.now()
    this_month = datetime(now.year, now.month, 1)
    half_year_ago = this_month - relativedelta(months=6)

    with sqlite3.connect(db_path) as conn:
        # Contributors
        commits = pd.read_sql("SELECT * FROM commits", conn)
        users = pd.read_sql("SELECT * FROM users", conn)
        # commits = commits.merge(users, left_on="user_id", right_on="id")
        # commits = commits[commits["is_mlflow_maintainer"] == 0]
        commits["date"] = pd.to_datetime(commits["date"])
        commits = commits.sort_values("date").groupby("user_id").head(1)
        contributors_by_month = count_by_month(commits, "date")
        contributors_by_month = contributors_by_month
        contributors_by_month = contributors_by_month[
            contributors_by_month["date"] >= half_year_ago
        ]
        contributors_plot_path = plots_dir.joinpath("contributors.html")
        make_plot(
            go.Scatter(
                x=contributors_by_month["date"],
                y=contributors_by_month["count"],
                mode="lines+markers",
            ),
            title="Contributors (excluding MLflow maintainers)",
        ).write_html(contributors_plot_path, include_plotlyjs="cdn")

        # Discussions
        discussions = pd.read_sql("SELECT * FROM discussions", conn)
        discussions["created_at"] = pd.to_datetime(discussions["created_at"])
        discussions["updated_at"] = pd.to_datetime(discussions["updated_at"])
        discussions_by_month = count_by_month(discussions, "created_at")
        discussions_by_month = discussions_by_month[discussions_by_month["date"] >= half_year_ago]
        discussions_plot_path = plots_dir.joinpath("discussions.html")
        make_plot(
            go.Scatter(
                x=discussions_by_month["date"],
                y=discussions_by_month["count"],
                mode="lines+markers",
            ),
            title="Discussions",
        ).write_html(discussions_plot_path, include_plotlyjs="cdn")

        issues = pd.read_sql("SELECT * FROM issues", conn)
        issues["closed_at"] = pd.to_datetime(issues["closed_at"])
        issues["created_at"] = pd.to_datetime(issues["created_at"])
        issues["updated_at"] = pd.to_datetime(issues["updated_at"])

        # Issues
        opened_issues = issues[issues["is_pr"] == 0]
        opened_issues_by_month = count_by_month(opened_issues, "created_at")
        opened_issues_by_month = opened_issues_by_month[
            opened_issues_by_month["date"] >= half_year_ago
        ]
        closed_issues = opened_issues[opened_issues["state"] == "closed"]
        closed_issues_by_month = count_by_month(closed_issues, "closed_at")
        closed_issues_by_month = closed_issues_by_month[
            closed_issues_by_month["date"] >= half_year_ago
        ]
        issues_plot_path = plots_dir.joinpath("issues.html")
        make_plot(
            go.Scatter(
                x=opened_issues_by_month["date"],
                y=opened_issues_by_month["count"],
                mode="lines+markers",
                name="Opened",
            ),
            go.Scatter(
                x=closed_issues_by_month["date"],
                y=closed_issues_by_month["count"],
                mode="lines+markers",
                name="Closed",
            ),
            title="Issues",
        ).write_html(issues_plot_path, include_plotlyjs="cdn")

        # Pull requests
        opened_pulls = issues[issues["is_pr"] == 1]
        opened_pulls_by_month = count_by_month(opened_pulls, "created_at")
        opened_pulls_by_month = opened_pulls_by_month[
            opened_pulls_by_month["date"] >= half_year_ago
        ]
        closed_pulls = opened_pulls[opened_pulls["state"] == "closed"]
        closed_pulls_by_month = count_by_month(closed_pulls, "closed_at")
        closed_pulls_by_month = closed_pulls_by_month[
            closed_pulls_by_month["date"] >= half_year_ago
        ]
        pulls_plot_path = plots_dir.joinpath("pulls.html")
        make_plot(
            go.Scatter(
                x=opened_pulls_by_month["date"],
                y=opened_pulls_by_month["count"],
                mode="lines+markers",
                name="Opened",
            ),
            go.Scatter(
                x=closed_pulls_by_month["date"],
                y=closed_pulls_by_month["count"],
                mode="lines+markers",
                name="Closed",
            ),
            title="Pull Requests",
        ).write_html(pulls_plot_path, include_plotlyjs="cdn")

        iframe_html_template = """
<iframe
  style="border: none"
  src="{src}"
  width="50%"
  height="500px"
></iframe>"""
        index_html_template = """
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta http-equiv="X-UA-Compatible" content="IE=edge" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Document</title>
  </head>
  <body>
    <div style="text-align: center">
      <a href="https://github.com/mlflow/mlflow">
        <img src="{logo}" alt="logo" height="100px" />
      </a>
      <h1 style="font-size: 36px; font-family: Arial;">
        Repository Status (updated at {updated_at})
      </h1>
    </div>
    {plots}
  </body>
</html>
"""
    plot_tile = [
        [issues_plot_path, pulls_plot_path],
        [contributors_plot_path, discussions_plot_path],
    ]

    plots_html = ""
    for plots in plot_tile:
        iframes = []
        for plot in plots:
            iframes.append(iframe_html_template.format(src=plot.relative_to(dist_dir)))
        plots_html += '<div style="display: flex">{plots}</div>'.format(plots="".join(iframes))

    logo = Path("assets", "MLflow-logo-final-black.png")
    logo_dst = dist_assets.joinpath(logo.name)
    shutil.copyfile(logo, logo_dst)
    index_html = dist_dir.joinpath("index.html")
    index_html.write_text(
        index_html_template.format(
            logo=logo_dst.relative_to(dist_dir),
            updated_at=now.strftime("%Y-%m-%d %H:%M:%S"),
            plots=plots_html,
        )
    )


if __name__ == "__main__":
    main()
