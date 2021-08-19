import random
from hacker_news.solids.user_story_matrix import IndexedCooMatrix
from hacker_news.resources.hn_resource import HNAPIClient
from pandas import DataFrame, Series
from sklearn.decomposition import TruncatedSVD
import numpy as np
from scipy.sparse import coo_matrix
from dagster import asset, build_assets_job, load_assets, repository


@asset
def items_asset():
    """Fetch items from Hacker News API"""
    start_id, end_id = 0, 1000000
    hn_client = HNAPIClient()

    rows = []
    for item_id in range(start_id, end_id):
        rows.append(hn_client.fetch_item_by_id(item_id))

    non_none_rows = [row for row in rows if row is not None]

    return DataFrame(non_none_rows).drop_duplicates(subset=["id"])


@asset(inputs=[items_asset])
def comments_asset(items):
    return items.where(items["type"] == "comment")


@asset(inputs=[items_asset])
def stories_asset(items):
    return items.where(items["type"] == "story")


@asset(inputs=[comments_asset, stories_asset])
def comment_stories_asset(comments, stories):
    """Traverse the comment tree to link each comment to its root story."""
    comments.rename(columns={"by": "commenter_id", "id": "comment_id"}, inplace=True)
    comments = comments.set_index("comment_id")[["commenter_id", "parent"]]
    stories = stories.set_index("id")[[]]

    comment_stories = DataFrame(
        index=Series(name="comment_id", dtype="int"),
        data={"story_id": Series(dtype="int"), "commenter_id": Series(dtype="object")},
    )
    remaining_comments = comments.copy()

    max_depth = 10
    depth = 0
    while remaining_comments.shape[0] > 0 and depth < max_depth:
        depth += 1
        # join comments with stories and remove all comments that match a story
        cur_comment_stories = remaining_comments.merge(stories, left_on="parent", right_index=True)
        cur_comment_stories.rename(columns={"parent": "story_id"}, inplace=True)
        comment_stories = comment_stories.append(cur_comment_stories)
        remaining_comments = remaining_comments.drop(cur_comment_stories.index)

        # join comments with comments and replace comments with that
        remaining_comments = remaining_comments.merge(
            comments[["parent"]], left_on="parent", right_index=True
        )
        remaining_comments = remaining_comments[["parent_y", "commenter_id"]]
        remaining_comments.rename(columns={"parent_y": "parent"}, inplace=True)

    return comment_stories


@asset(inputs=[comment_stories_asset])
def user_story_matrix_asset(comment_stories):
    """Build a sparse matrix where the rows are users, the columns are stories, and the values
    are whether the user commented on the story."""
    deduplicated = comment_stories[["story_id", "commenter_id"]].drop_duplicates().dropna()

    users = deduplicated["commenter_id"].drop_duplicates()
    user_row_indices = Series(index=users, data=list(range(len(users))))
    stories = deduplicated["story_id"].drop_duplicates()
    story_col_indices = Series(index=stories, data=list(range(len(stories))))

    sparse_rows = user_row_indices[deduplicated["commenter_id"]]
    sparse_cols = story_col_indices[deduplicated["story_id"]]
    sparse_data = np.ones(len(sparse_rows))

    return IndexedCooMatrix(
        matrix=coo_matrix(
            (sparse_data, (sparse_rows, sparse_cols)), shape=(len(users), len(stories))
        ),
        row_index=Series(user_row_indices.index.values, index=user_row_indices),
        col_index=Series(story_col_indices.index.values, index=story_col_indices),
    )


@asset(inputs=[user_story_matrix_asset])
def recommender_model_asset(user_story_matrix):
    """Train an SVD model for collaborative filtering-based recommendation."""
    n_components = random.randint(90, 110)
    model = TruncatedSVD(n_components=n_components)
    model.fit(user_story_matrix.matrix)

    return model


@asset(inputs=[stories_asset, recommender_model_asset, user_story_matrix_asset])
def component_top_stories_asset(stories, model, user_story_matrix):
    """For each component in the collaborative filtering model, find the titles of the top stories
    it's associated with."""
    n_stories = 10

    components_column = []
    titles_column = []

    story_titles = stories.set_index("id")

    for i in range(model.components_.shape[0]):
        component = model.components_[i]
        top_story_indices = component.argsort()[-n_stories:][::-1]
        top_story_ids = user_story_matrix.col_index[top_story_indices]
        top_story_titles = story_titles.loc[top_story_ids]

        for title in top_story_titles["title"]:
            components_column.append(i)
            titles_column.append(title)

    return DataFrame({"component_index": Series(components_column), "title": Series(titles_column)})


if __name__ == "__main__":
    assets = load_assets(globals())
    build_assets_job(assets).execute_in_process()


@repository
def prod_repo():
    return [
        build_assets_job(
            load_assets(globals()), resoruce_defs={"io_manager": production_io_manager}
        )
    ]
