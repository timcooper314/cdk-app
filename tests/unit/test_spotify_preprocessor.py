import json
from pathlib import Path
import pytest
from unittest.mock import MagicMock
from api_ingestion.spotify_preprocessor import _process_top_data


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
def tracks_endpoint_details():
    return {
        "endpoint": "tracks",
        "s3_key_format": "spotify/tracks/%Y/%m/top_tracks%Y%m%d.json",
    }


def test_should_process_raw_top_data(
    top_track_raw_data, tracks_endpoint_details, expected_top_track_3
):
    processed_json = _process_top_data(top_track_raw_data, tracks_endpoint_details)
    assert processed_json[3] == expected_top_track_3
