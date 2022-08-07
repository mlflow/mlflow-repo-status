import logging
import os

import requests

GITHUB_TOKEN_ENV_VAR = "GITHUB_TOKEN"

logger = logging.getLogger(__name__)


class GitHubApiClient:
    def __init__(self, per_page=100):
        if GITHUB_TOKEN_ENV_VAR not in os.environ:
            raise Exception(f"{GITHUB_TOKEN_ENV_VAR} must be set")
        self.per_page = per_page
        self.sess = requests.Session()
        self.sess.headers.update(
            {
                "User-Agent": __name__,
                "Accept": "application/vnd.github.v3.star+json",
                "Authorization": "token " + os.getenv(GITHUB_TOKEN_ENV_VAR),
            }
        )

    def get(self, end_point, *args, **kwargs):
        resp = self.sess.get("https://api.github.com" + end_point, *args, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def run_graphql_query(self, query):
        resp = self.sess.post("https://api.github.com/graphql", json={"query": query})
        resp.raise_for_status()
        return resp.json()

    def get_paginate(self, end_point, params=None):
        page = 1
        while True:
            logger.info(f"{end_point} {page}")
            res = self.get(
                end_point, params={**(params or {}), "page": page, "per_page": self.per_page}
            )
            yield from res
            if len(res) < self.per_page:
                break
            page += 1

    def get_commits(self, owner, repo, params=None):
        return self.get_paginate(f"/repos/{owner}/{repo}/commits", params)

    def get_contributors(self, owner, repo, params=None):
        return self.get_paginate(f"/repos/{owner}/{repo}/contributors", params)

    def get_collaborators(self, owner, repo, params=None):
        return self.get_paginate(f"/repos/{owner}/{repo}/collaborators", params)

    def get_stargazers(self, owner, repo, params=None):
        return self.get_paginate(f"/repos/{owner}/{repo}/stargazers", params)

    def get_issues(self, owner, repo, params=None):
        return self.get_paginate(f"/repos/{owner}/{repo}/issues", params)

    def get_organization_members(self, org, params=None):
        return self.get_paginate(f"/orgs/{org}/members", params)

    def get_rate_limit(self):
        return self.get("/rate_limit")

    def get_discussions(self, owner, repo):
        query = """
query {
  repository(owner: "%s", name: "%s") {
    discussions(first: %d) {
      totalCount

      pageInfo {
        endCursor
        hasNextPage
      }

      nodes {
        id
        number
        url
        title
        body
        createdAt
        updatedAt
      }
    }
  }
}
""" % (
            owner,
            repo,
            self.per_page,
        )

        query_with_cursor = """
query {
  repository(owner: "%s", name: "%s") {
    discussions(first: %d, after: "AFTER") {
      totalCount

      pageInfo {
        endCursor
        hasNextPage
      }

      nodes {
        id
        number
        url
        title
        body
        createdAt
        updatedAt
      }
    }
  }
}
""" % (
            owner,
            repo,
            self.per_page,
        )
        after = None
        while True:
            q = query if after is None else query_with_cursor.replace("AFTER", after)
            data = self.run_graphql_query(q)

            discussions = data["data"]["repository"]["discussions"]
            yield from discussions["nodes"]

            page_info = discussions["pageInfo"]
            after = page_info["endCursor"]
            if not page_info["hasNextPage"]:
                break
