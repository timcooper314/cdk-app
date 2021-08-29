import json
from pathlib import Path
import pytest
from hello_cdk.spotify_preprocessor import spotify_data_preprocessor


@pytest.fixture
def top_track_raw_data():
    file_path = "./top_tracks.json"
    file = Path(__file__).parent.resolve() / file_path
    with file.open() as fp:
        return json.load(fp)


@pytest.fixture
def expected_top_track_3():
    return {"Joke or a Lie": "Sharon Van Etten"}


def test_should_process_raw_top_data(top_track_raw_data,
                                     expected_top_track_3):
    processed_json = spotify_data_preprocessor(top_track_raw_data)
    assert processed_json[3] == expected_top_track_3
