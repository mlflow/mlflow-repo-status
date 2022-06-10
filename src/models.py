from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


def parse_datetime(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


class BaseModel(Base):
    __abstract__ = True

    @classmethod
    def from_gh_objects(cls, objs, *args):
        result = []
        for obj in objs:
            model = cls.from_gh_object(obj, *args)
            if model:
                result.append(model)
        return result

    @classmethod
    def from_gh_object(cls, obj, *args):
        raise NotImplementedError("Must be implemented")


class User(BaseModel):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    login = Column(String, unique=True)
    is_mlflow_maintainer = Column(Boolean)

    @classmethod
    def from_gh_objects(cls, objs, mlflow_maintainers):
        result = []
        for obj in objs:
            model = cls.from_gh_object(obj, mlflow_maintainers)
            if model:
                result.append(model)
        return result

    @classmethod
    def from_gh_object(cls, user, mlflow_maintainers):
        return cls(
            id=user["id"],
            login=user["login"],
            is_mlflow_maintainer=user["id"] in mlflow_maintainers,
        )


class Commit(BaseModel):
    __tablename__ = "commits"

    id = Column(String(40), primary_key=True)
    html_url = Column(String)
    url = Column(String)
    user_id = Column(Integer, ForeignKey("users.id"))
    date = Column(DateTime)

    @classmethod
    def from_gh_object(cls, commit):
        if not commit["author"]:
            return
        return cls(
            id=commit["sha"],
            url=commit["url"],
            html_url=commit["html_url"],
            user_id=commit["author"]["id"],
            date=parse_datetime(commit["commit"]["committer"]["date"]),
        )


class Issue(BaseModel):
    __tablename__ = "issues"

    id = Column(Integer, primary_key=True)
    number = Column(Integer)
    title = Column(String)
    body = Column(String)
    state = Column(String)
    closed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    html_url = Column(String)
    is_pr = Column(Boolean)

    @classmethod
    def from_gh_object(cls, issue):
        closed_at = issue.get("closed_at")
        return cls(
            id=issue["id"],
            number=issue["number"],
            title=issue["title"],
            body=issue["body"],
            state=issue["state"],
            closed_at=closed_at and parse_datetime(closed_at),
            created_at=parse_datetime(issue["created_at"]),
            updated_at=parse_datetime(issue["updated_at"]),
            html_url=issue["html_url"],
            is_pr="pull_request" in issue,
        )


class Discussion(BaseModel):
    __tablename__ = "discussions"

    id = Column(String, primary_key=True)
    number = Column(Integer)
    url = Column(String)
    title = Column(String)
    body = Column(String)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    @classmethod
    def from_gh_object(cls, discussion):
        return cls(
            id=discussion["id"],
            number=discussion["number"],
            url=discussion["url"],
            title=discussion["title"],
            body=discussion["body"],
            created_at=parse_datetime(discussion["createdAt"]),
            updated_at=parse_datetime(discussion["updatedAt"]),
        )
