import json
from pathlib import Path
import pytest
from unittest.mock import MagicMock
from hello_cdk.spotify_preprocessor import SpotifyDataPreprocessor


@pytest.fixture
def top_track_raw_data():
    file_path = "./top_tracks.json"
    file = Path(__file__).parent.resolve() / file_path
    with file.open() as fp:
        return json.load(fp)


@pytest.fixture
def expected_top_track_3():
    return {"Joke or a Lie": "Sharon Van Etten"}


@pytest.fixture
def obj():
    obj = SpotifyDataPreprocessor.__new__(SpotifyDataPreprocessor)
    obj.s3 = MagicMock()


def test_should_process_raw_top_data(obj, top_track_raw_data,
                                     expected_top_track_3):
    processed_json = SpotifyDataPreprocessor().process_top_data(top_track_raw_data)
    assert processed_json[3] == expected_top_track_3
