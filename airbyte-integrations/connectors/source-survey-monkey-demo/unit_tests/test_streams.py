#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from source_survey_monkey_demo.source import SurveyMonkeyBaseStream


_STREAM_NAME = "test_stream"
_PATH= "v0/example_endpoint"
_PRIMARY_KEY = "id"
_DATA_FIELD = "data"


def test_request_params():
    stream = SurveyMonkeyBaseStream(_STREAM_NAME, _PATH, _PRIMARY_KEY, _DATA_FIELD)
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_params = {"include": "response_count,date_created,date_modified,language,question_count,analyze_url,preview,collect_stats", "per_page": 1000}
    assert stream.request_params(**inputs) == expected_params