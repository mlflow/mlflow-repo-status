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


# Add avatar image to the table
def get_avatar_img(row):
    user_id = row["user_id"]
    return f'<img src="https://avatars.githubusercontent.com/u/{user_id}" width="20" height="20" />'


def main():
    pd.options.plotting.backend = "plotly"
    dist_dir = Path("dist")
    if dist_dir.exists():
        shutil.rmtree(dist_dir)
    dist_assets = dist_dir.joinpath("assets")
    plots_dir = dist_assets.joinpath("plots")
    tables_dir = dist_assets.joinpath("tables")
    plots_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    db_path = Path("github.sqlite")

    now = datetime.now()
    this_month = datetime(now.year, now.month, 1)
    firs_commit_date = datetime(2018, 6, 5)
    x_tick_vals = pd.date_range(
        firs_commit_date,
        this_month,
        freq="3MS",
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
        raw_commits["user_url"] = raw_commits["user_login"].apply(
            lambda login: f"https://github.com/{login}"
        )
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

        commits_url_template = (
            "https://github.com/mlflow/mlflow/commits?author={author}&since={since}&until={until}"
        )
        anchor_template = '<a href="{url}">{text}</a>'
        six_month_ago = now - relativedelta(months=6)
        active_contributors = (
            commits[commits["date"] >= six_month_ago]
            .groupby(["user_url", "user_login", "user_id"])
            # Latest commit
            .agg({"date": "max", "id": "count"})
            # .count()
            .sort_values("id", ascending=False)
            .head(10)[["id", "date"]]
            .rename(columns={"id": "PRs", "date": "last_commit_date"})
            .reset_index()
            .assign(
                commits=lambda df: df.apply(
                    lambda row: commits_url_template.format(
                        author=row["user_login"],
                        since=six_month_ago.strftime("%Y-%m-%d"),
                        until=now.strftime("%Y-%m-%d"),
                    ),
                    axis=1,
                )
            )
            .assign(
                user=lambda df: df.apply(
                    lambda row: anchor_template.format(url=row["user_url"], text=row["user_login"]),
                    axis=1,
                ),
                PRs=lambda df: df.apply(
                    lambda row: anchor_template.format(url=row["commits"], text=row["PRs"]),
                    axis=1,
                ),
            )
            .assign(avatar=lambda df: df.apply(lambda row: get_avatar_img(row), axis=1))
            .assign(
                last_commit_date=lambda df: df.apply(
                    lambda row: row["last_commit_date"].strftime("%Y-%m-%d"), axis=1
                )
            )
            .drop(["user_login", "user_url", "commits"], axis=1)[
                ["user", "avatar", "PRs", "last_commit_date"]
            ]
            .rename(columns={"PRs": "PRs (within last 6 months)"})
        )

        active_contributors_path = tables_dir.joinpath("active_contributors.html")
        active_contributors.to_html(
            active_contributors_path,
            escape=False,
            index=False,
            justify="center",
        )

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
            title="Contributors (including maintainers)",
            x_tick_vals=x_tick_vals,
            x_axis_range=x_axis_range,
            y_axis_range=get_y_axis_range(
                total_contributors_by_month[total_contributors_by_month["date"] >= year_ago][
                    "count"
                ]
            ),
        ).write_html(total_contributors_path, include_plotlyjs="cdn")

        # Number of commits
        commits_count = (
            raw_commits.groupby(raw_commits["date"].dt.to_period("M"))
            .count()
            .rename(columns={"id": "count"})[["count"]]
            .reset_index()
        )
        commits_count["date"] = commits_count["date"].dt.start_time
        commits_count["count"] = commits_count["count"].cumsum()
        commits_count_path = plots_dir.joinpath("commits.html")
        make_plot(
            go.Scatter(
                x=commits_count["date"],
                y=commits_count["count"],
                mode="lines+markers",
            ),
            title="Commits (on master branch)",
            x_tick_vals=x_tick_vals,
            x_axis_range=x_axis_range,
            y_axis_range=get_y_axis_range(
                commits_count[commits_count["date"] >= year_ago]["count"]
            ),
        ).write_html(commits_count_path, include_plotlyjs="cdn")

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
    <style>
      table {{
        margin: auto;
        border-collapse: collapse;
        border: 1px solid black;
      }}
        th, td {{
          border: 1px solid black;
          padding: 5px;
        }}
    </style>
  </head>
  <body>
    <div style="text-align: center">
      <a href="https://github.com/mlflow/mlflow">
        <img src="{logo}" alt="logo" height="100px" />
      </a>
      <h1 style="font-family: Arial;">
        Repository Status (updated at {updated_at})
      </h1>
    </div>
    <div style="text-align: center">
      <h2 style="font-family: Arial;">
        Thank you for your contributions!
      </h2>
      <div>{active_contributors_table}</div>
    </div>
    {plots}
  </body>
</html>
"""
    plot_tile = [
        [contributors_plot_path, total_contributors_path],
        [pulls_maintainers_plot_path, pulls_non_maintainers_plot_path],
        [stargazers_plot_path, issues_plot_path],
        [discussions_plot_path, commits_count_path],
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
            active_contributors_table=active_contributors_path.read_text(),
        )
    )


if __name__ == "__main__":
    main()
