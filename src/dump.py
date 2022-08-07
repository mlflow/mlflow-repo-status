import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from pprint import pprint
from typing import NamedTuple

import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import models as M
from client import GitHubApiClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Repo(NamedTuple):
    owner: str
    repo: str


class HashableDict(dict):
    def __hash__(self):
        return hash(frozenset(self))


def main():
    db_path = Path("github.sqlite")
    url = f"sqlite:///{db_path}"
    if db_path.exists():
        logger.info(f"Removing {db_path}")
        db_path.unlink()

    engine = create_engine(url)
    M.Base.metadata.create_all(engine)
    Session = sessionmaker(engine)

    repo = Repo("mlflow", "mlflow")

    with Session.begin() as session:
        g = GitHubApiClient(per_page=100)
        pprint(g.get_rate_limit())
        since = datetime(1970, 1, 1)
        # since = datetime(2022, 7, 1)

        logger.info("Collecting commits")
        commits = g.get_commits(
            *repo,
            params={
                "since": since,
            },
        )
        session.add_all(M.Commit.from_gh_objects(commits))

        logger.info("Collecting contributors")
        contributors = g.get_contributors(*repo)
        session.add_all(M.User.from_gh_objects(contributors))

        logger.info("Collecting mlflow org members")
        mlflow_org_members = set(
            HashableDict(id=m["id"], login=m["login"]) for m in g.get_organization_members("mlflow")
        )
        collaborators = set(
            HashableDict(id=c["id"], login=c["login"]) for c in g.get_collaborators(*repo)
        )
        session.add_all(M.MlflowOrgMember.from_gh_objects(mlflow_org_members.union(collaborators)))

        logger.info("Collecting issues")
        issues = g.get_issues(
            *repo,
            params={
                "state": "all",
                "since": since,
            },
        )
        session.add_all(M.Issue.from_gh_objects(issues))

        logger.info("Collecting discussions")
        discussions = g.get_discussions(*repo)
        session.add_all(M.Discussion.from_gh_objects(discussions))

        logger.info("Collecting stargazers")
        stargazers = g.get_stargazers(*repo)
        session.add_all(M.Stargazer.from_gh_objects(stargazers))

        pprint(g.get_rate_limit())

    with sqlite3.connect(db_path) as conn:
        print(pd.read_sql("SELECT * FROM commits", conn).head())
        print(pd.read_sql("SELECT * FROM users", conn).head())
        print(pd.read_sql("SELECT * FROM mlflow_org_members", conn).head())
        print(pd.read_sql("SELECT * FROM issues", conn).head())
        print(pd.read_sql("SELECT * FROM discussions", conn).head())
        print(pd.read_sql("SELECT * FROM stargazers", conn).head())


if __name__ == "__main__":
    main()
