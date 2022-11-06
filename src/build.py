import logging
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
import itertools


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
            lambda df_: (
                df_.set_index(
                    df_.index.map(lambda year_month: datetime(year_month[0], year_month[1], 1))
                )
            )
        )
        .reset_index()
        .rename(columns={"index": "date"})
    )
    return df


def get_y_axis_range(*ys):
    return [0, int(max(itertools.chain.from_iterable(ys)) * 1.125)]


def make_plot(*traces, title, x_tick_vals, x_axis_range, y_axis_range):
    fig = go.Figure()
    fig.update_layout(
        title=dict(text=title, font=dict(size=25)),
        yaxis=dict(
            title="Count",
            range=y_axis_range,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        xaxis=dict(
            tickformat="%b %Y",
            tickangle=-45,
            tickvals=x_tick_vals,
            range=x_axis_range,
        ),
    )
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
    firs_commit_date = datetime(2018, 6, 5)
    x_tick_vals = pd.date_range(
        firs_commit_date,
        this_month,
        freq="MS",
        inclusive="both",
    )
    year_ago = this_month - relativedelta(months=12)
    x_axis_range = pd.date_range(
        year_ago,
        this_month,
        freq="MS",
        inclusive="both",
    )
    x_axis_range = [
        x_axis_range[0] + relativedelta(days=-15),
        x_axis_range[-1] + relativedelta(days=15),
    ]

    with sqlite3.connect(db_path) as conn:
        # set dataframe display width
        pd.set_option("display.max_colwidth", 300)
        # Contributors
        raw_commits = pd.read_sql("SELECT * FROM commits", conn)
        raw_commits["date"] = pd.to_datetime(raw_commits["date"])
        users = pd.read_sql("SELECT * FROM users", conn)
        mlflow_org_members = pd.read_sql("SELECT * FROM mlflow_org_members", conn)
        # Filter out commits from mlflow org members
        commits = raw_commits.merge(
            mlflow_org_members.rename(columns={"id": "user_id"}).drop("login", axis=1),
            on="user_id",
            how="outer",
            indicator=True,
        )
        commits = commits[(commits._merge == "left_only")].drop("_merge", axis=1)
        commits = commits.merge(users.rename(columns={"id": "user_id"}), on="user_id")
        first_commits = commits.sort_values("date").groupby("user_name").head(1)
        contributors_by_month = count_by_month(first_commits, "date")
        contributors_plot_path = plots_dir.joinpath("contributors.html")
        make_plot(
            go.Scatter(
                x=contributors_by_month["date"],
                y=contributors_by_month["count"],
                mode="lines+markers",
            ),
            title="First-time contributors (excluding maintainers)",
            x_tick_vals=x_tick_vals,
            x_axis_range=x_axis_range,
            y_axis_range=get_y_axis_range(
                contributors_by_month[contributors_by_month["date"] >= year_ago]["count"]
            ),
        ).write_html(contributors_plot_path, include_plotlyjs="cdn")

        first_commits = raw_commits.sort_values("date").groupby("user_name").head(1)
        total_contributors_by_month = count_by_month(first_commits, "date")
        total_contributors_by_month["count"] = total_contributors_by_month["count"].cumsum()
        total_contributors_path = plots_dir.joinpath("total_contributors.html")
        make_plot(
            go.Scatter(
                x=total_contributors_by_month["date"],
                y=total_contributors_by_month["count"],
                mode="lines+markers",
            ),
            title="Contributors",
            x_tick_vals=x_tick_vals,
            x_axis_range=x_axis_range,
            y_axis_range=get_y_axis_range(
                total_contributors_by_month[total_contributors_by_month["date"] >= year_ago]["count"]
            ),
        ).write_html(total_contributors_path, include_plotlyjs="cdn")

        # Discussions
        stargazers = pd.read_sql("SELECT * FROM stargazers", conn)
        stargazers["starred_at"] = pd.to_datetime(stargazers["starred_at"])
        stargazers_by_month = count_by_month(stargazers, "starred_at")
        stargazers_plot_path = plots_dir.joinpath("stargazers.html")
        make_plot(
            go.Scatter(
                x=stargazers_by_month["date"],
                y=stargazers_by_month["count"],
                mode="lines+markers",
            ),
            title="Stargazers",
            x_tick_vals=x_tick_vals,
            x_axis_range=x_axis_range,
            y_axis_range=get_y_axis_range(
                stargazers_by_month[stargazers_by_month["date"] >= year_ago]["count"]
            ),
        ).write_html(stargazers_plot_path, include_plotlyjs="cdn")

        # Discussions
        discussions = pd.read_sql("SELECT * FROM discussions", conn)
        discussions["created_at"] = pd.to_datetime(discussions["created_at"])
        discussions["updated_at"] = pd.to_datetime(discussions["updated_at"])
        discussions_by_month = count_by_month(discussions, "created_at")
        discussions_plot_path = plots_dir.joinpath("discussions.html")
        make_plot(
            go.Scatter(
                x=discussions_by_month["date"],
                y=discussions_by_month["count"],
                mode="lines+markers",
            ),
            title="Discussions",
            x_tick_vals=x_tick_vals,
            x_axis_range=x_axis_range,
            y_axis_range=get_y_axis_range(
                discussions_by_month[discussions_by_month["date"] >= year_ago]["count"]
            ),
        ).write_html(discussions_plot_path, include_plotlyjs="cdn")

        issues = pd.read_sql("SELECT * FROM issues", conn)
        issues["closed_at"] = pd.to_datetime(issues["closed_at"])
        issues["created_at"] = pd.to_datetime(issues["created_at"])
        issues["updated_at"] = pd.to_datetime(issues["updated_at"])

        # Issues
        opened_issues = issues[issues["is_pr"] == 0]
        opened_issues_by_month = count_by_month(opened_issues, "created_at")
        closed_issues = opened_issues[opened_issues["state"] == "closed"]
        closed_issues_by_month = count_by_month(closed_issues, "closed_at")
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
            x_tick_vals=x_tick_vals,
            x_axis_range=x_axis_range,
            y_axis_range=get_y_axis_range(
                opened_issues_by_month[opened_issues_by_month["date"] >= year_ago]["count"],
                closed_issues_by_month[closed_issues_by_month["date"] >= year_ago]["count"],
            ),
        ).write_html(issues_plot_path, include_plotlyjs="cdn")

        # Pull requests (maintainers)
        opened_pulls = issues[issues["is_pr"] == 1]
        opened_pulls = opened_pulls.merge(
            mlflow_org_members.rename(columns={"id": "user_id"}).drop("login", axis=1),
            on="user_id",
            how="outer",
            indicator=True,
        )
        opened_pulls = opened_pulls[(opened_pulls._merge == "both")].drop("_merge", axis=1)
        opened_pulls_by_month = count_by_month(opened_pulls, "created_at")
        closed_pulls = opened_pulls[opened_pulls["state"] == "closed"]
        closed_pulls_by_month = count_by_month(closed_pulls, "closed_at")
        pulls_maintainers_plot_path = plots_dir.joinpath("pulls_all.html")
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
            title="Pull Requests (maintainers)",
            x_tick_vals=x_tick_vals,
            x_axis_range=x_axis_range,
            y_axis_range=get_y_axis_range(
                opened_pulls_by_month[opened_pulls_by_month["date"] >= year_ago]["count"],
                closed_pulls_by_month[closed_pulls_by_month["date"] >= year_ago]["count"],
            ),
        ).write_html(pulls_maintainers_plot_path, include_plotlyjs="cdn")

        # Pull requests (non maintainers)
        opened_pulls = issues[issues["is_pr"] == 1]
        # Filter out commits from mlflow org members
        opened_pulls = opened_pulls.merge(
            mlflow_org_members.rename(columns={"id": "user_id"}).drop("login", axis=1),
            on="user_id",
            how="outer",
            indicator=True,
        )
        opened_pulls = opened_pulls[(opened_pulls._merge == "left_only")].drop("_merge", axis=1)
        opened_pulls_by_month = count_by_month(opened_pulls, "created_at")
        closed_pulls = opened_pulls[opened_pulls["state"] == "closed"]
        closed_pulls_by_month = count_by_month(closed_pulls, "closed_at")
        pulls_non_maintainers_plot_path = plots_dir.joinpath("pulls_non_maintainers.html")
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
            title="Pull Requests (non-maintainers)",
            x_tick_vals=x_tick_vals,
            x_axis_range=x_axis_range,
            y_axis_range=get_y_axis_range(
                opened_pulls_by_month[opened_pulls_by_month["date"] >= year_ago]["count"],
                closed_pulls_by_month[closed_pulls_by_month["date"] >= year_ago]["count"],
            ),
        ).write_html(pulls_non_maintainers_plot_path, include_plotlyjs="cdn")

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
    <link rel="icon" href="{favicon}" sizes="any" type="image/svg+xml">
    <title>MLflow Repository Status</title>
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
        [contributors_plot_path, total_contributors_path],
        [pulls_maintainers_plot_path, pulls_non_maintainers_plot_path],
        [stargazers_plot_path, issues_plot_path],
        [discussions_plot_path,]
    ]

    plots_html = ""
    for plots in plot_tile:
        iframes = []
        for plot in plots:
            iframes.append(iframe_html_template.format(src=plot.relative_to(dist_dir)))
        plots_html += '<div style="display: flex">{plots}</div>'.format(plots="".join(iframes))

    logo = Path("assets", "MLflow-logo-final-black.png")
    favicon = Path("assets", "icon.svg")
    logo_dst = dist_assets.joinpath(logo.name)
    favicon_dst = dist_assets.joinpath(favicon.name)
    shutil.copyfile(logo, logo_dst)
    shutil.copyfile(favicon, favicon_dst)
    index_html = dist_dir.joinpath("index.html")
    index_html.write_text(
        index_html_template.format(
            logo=logo_dst.relative_to(dist_dir),
            favicon=favicon_dst.relative_to(dist_dir),
            updated_at=now.strftime("%Y-%m-%d %H:%M:%S"),
            plots=plots_html,
        )
    )


if __name__ == "__main__":
    main()
