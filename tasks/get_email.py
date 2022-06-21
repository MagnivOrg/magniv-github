from magniv.core import task

import requests
import json
import time
from sqlalchemy import create_engine
import os
import redis


@task(
    schedule="@daily",
    description="Get new repos from prepare list and add them to the github list",
)
def prepare():
    r = redis.from_url(os.environ.get("REDIS_URL"))
    next_repo = r.spop("github_repos")
    while next_repo:
        next_repo = next_repo.decode()
        users = []
        _get_star_gazers(
            next_repo,
            os.environ.get("GITHUB_CLIENT_ID"),
            os.environ.get("GITHUB_CLIENT_SECRET"),
            user_profiles=users,
        )
        f = "ghost_list/1_{}_.json".format(next_repo.replace("/", "_"))
        for user in users:
            user_info = {"github_info": user, "file": f}
            r.sadd("github_list", json.dumps(user_info))
        next_repo = r.spop("github_repos")


@task(
    schedule="0 */2 * * *",
    description="Get emails from the github_list on Redis and then add them to finished_profiles",
)
def get_email():
    r = redis.from_url(os.environ.get("REDIS_URL"))
    # first step is SPOP
    print("starting task ...")
    email_count = 0
    i = 0
    while True:
        i += 1
        profile = r.spop("github_list")
        if profile is not None:
            profile = profile.decode()
        else:
            break
        profile = json.loads(profile)
        resp = requests.get(
            profile["github_info"]["url"],
            auth=(
                os.environ("GITHUB_CLIENT_ID"),
                os.environ.get("GITHUB_CLIENT_SECRET"),
            ),
        )
        if resp.status_code == 200:
            response = resp.json()
            profile["github_user_profile"] = response
            r.sadd("finished_profiles", json.dumps(profile))
            if response["email"] is not None:
                email_count += 1
            time.sleep(0.3)
        elif resp.status_code == 404:
            pass
        else:
            r.sadd("github_list", json.dumps(profile))
            print("failed --- i ", i, email_count)
            print(resp.status_code)
            print(resp.json())
            # save
            break


@task(
    schedule="@daily",
    description="Task to clean up the finished_profiles redis set and move things into a DB",
)
def clean_redis_set():
    engine = create_engine(os.environ.get("DB_CONNECTION_STRING"))
    connection = engine.connect()
    r = redis.from_url(os.environ.get("REDIS_URL"))
    profile = r.spop("finished_profiles")
    while profile:
        profile = json.loads(profile.decode())
        # move this into the SQL DB
        try:
            _create_new_lead(
                connection,
                source_file=profile["file"],
                source_repo=_get_repo(profile["file"]),
                gh_login=profile["github_user_profile"]["login"],
                gh_id=profile["github_user_profile"]["id"],
                gh_name=profile["github_user_profile"]["name"],
                gh_email=profile["github_user_profile"]["email"],
                gh_twitter=profile["github_user_profile"]["twitter_username"],
                gh_public_repos=profile["github_user_profile"]["public_repos"],
                gh_followers=profile["github_user_profile"]["followers"],
                gh_created_at=profile["github_user_profile"]["created_at"],
                gh_updated_at=profile["github_user_profile"]["updated_at"],
                raw=json.dumps(profile),
            )
        except:
            print("failed on ", profile)
            r.sadd("finished_profiles_error", json.dumps(profile))
        profile = r.spop("finished_profiles")
    connection.close()


def _create_new_lead(
    connection,
    source_file=None,
    source_repo=None,
    gh_login=None,
    gh_id=None,
    gh_name=None,
    gh_email=None,
    gh_twitter=None,
    gh_public_repos=None,
    gh_followers=None,
    gh_created_at=None,
    gh_updated_at=None,
    raw=None,
):
    connection.execute(
        "insert into github_leads (source_file, source_repo, gh_login, gh_id, gh_name, gh_email, gh_twitter, gh_public_repos, gh_followers, gh_created_at, gh_updated_at, raw) values ('{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, '{}', '{}', '{}')".format(
            source_file,
            source_repo,
            gh_login,
            gh_id,
            gh_name,
            gh_email,
            gh_twitter,
            gh_public_repos,
            gh_followers,
            gh_created_at,
            gh_updated_at,
            raw,
        )
    )


def _get_star_gazers(repo, client_id, client_secret, page=1, user_profiles=[]):
    url = "https://api.github.com/repos/{}/stargazers?per_page=100&page={}".format(
        repo, page
    )
    resp = requests.get(url, auth=(client_id, client_secret))
    if resp.status_code == 200:
        response = resp.json()
        user_profiles.extend(response)
        print(len(user_profiles))
        if len(response) < 100:
            # we are done
            print("we are done")
            return user_profiles
        else:
            print("we are going to the next page ", page)
            time.sleep(1)
            _get_star_gazers(
                repo,
                client_id,
                client_secret,
                page=page + 1,
                user_profiles=user_profiles,
            )
    else:
        print("failed --- page: ", page, " repo ", repo)
        # save
        return user_profiles


def _get_repo(file_name):
    main_name = "_".join(file_name.split("/")[1].split("_")[1:])
    github_repo_name = "{}/{}".format(main_name.split("_")[0], main_name.split("_")[1])
    return github_repo_name


if __name__ == "__main__":
    pass

