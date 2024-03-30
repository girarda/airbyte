#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from urllib.parse import urlparse

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator, TokenAuthenticator
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from requests import HTTPError, codes, exceptions  # type: ignore[import]
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.models import FailureType, SyncMode, AirbyteMessage
from requests import HTTPError, codes, exceptions  # type: ignore[import]
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
import datetime

_START_DATE = datetime.datetime(2020,1,1, 0,0,0).timestamp()
_PAGE_SIZE: int = 1000
_SLICE_RANGE = 365
_OUTGOING_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_INCOMING_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
class SurveyMonkeyBaseStream(HttpStream, ABC):
    def __init__(self, name: str, path: str, primary_key: Union[str, List[str]], data_field: Optional[str], cursor_field: Optional[str],
**kwargs: Any) -> None:
        self._name = name
        self._path = path
        self._primary_key = primary_key
        self._data_field = data_field
        self._cursor_field = cursor_field
        super().__init__(**kwargs)


    url_base = "https://api.surveymonkey.com"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
            links = response.json().get("links", {})
            if "next" in links:
                return {"next_url": links["next"]}
            else:
                return {}


    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if next_page_token:
            return urlparse(next_page_token["next_url"]).query
        else:
            return {"include": "response_count,date_created,date_modified,language,question_count,analyze_url,preview,collect_stats",
                    "per_page": _PAGE_SIZE,
                    "sort_by": "date_modified", "sort_order": "ASC",
                    "start_modified_at": datetime.datetime.strftime(datetime.datetime.fromtimestamp(stream_slice["start_date"]), _OUTGOING_DATETIME_FORMAT), 
                    "end_modified_at": datetime.datetime.strftime(datetime.datetime.fromtimestamp(stream_slice["end_date"]), _OUTGOING_DATETIME_FORMAT)
                    }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        # https://api.surveymonkey.com/v3/docs?shell#error-codes
        if response_json.get("error") in (1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018):
            internal_message = "Unauthorized credentials. Response: {response_json}"
            external_message = "Can not get metadata with unauthorized credentials. Try to re-authenticate in source settings."
            raise AirbyteTracedException(
                message=external_message, internal_message=internal_message, failure_type=FailureType.config_error
            )
        elif self._data_field:
            yield from response_json[self._data_field]
        else:
            yield from response_json

    @property
    def name(self) -> str:
        return self._name

    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return self._path

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return self._primary_key

    @property
    def availability_strategy(self) -> Optional[AvailabilityStrategy]:
        return None
    
    @property
    def cursor_field(self) -> Optional[str]:
        return self._cursor_field

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        state_value = max(current_stream_state.get(self.cursor_field, 0), datetime.datetime.strptime(latest_record.get(self._cursor_field, ""), _INCOMING_DATETIME_FORMAT).timestamp())
        return {self._cursor_field: state_value} 

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        start_ts = stream_state.get(self._cursor_field, _START_DATE) if stream_state else _START_DATE
        now_ts = datetime.datetime.now().timestamp()
        if start_ts >= now_ts:
            yield from []
            return
        for start, end in self.chunk_dates(start_ts, now_ts):
            yield {"start_date": start, "end_date": end}

    def chunk_dates(self, start_date_ts: int, end_date_ts: int) -> Iterable[Tuple[int, int]]:
        step = int(_SLICE_RANGE * 24 * 60 * 60)
        after_ts = start_date_ts
        while after_ts < end_date_ts:
            before_ts = min(end_date_ts, after_ts + step)
            yield after_ts, before_ts
            after_ts = before_ts + 1

class SurveyMonkeySubstream(HttpStream, ABC):

    def __init__(self, name: str, path: str, primary_key: Union[str, List[str]], parent_stream: Stream, **kwargs: Any) -> None:
        self._name = name
        self._path = path
        self._primary_key = primary_key
        self._parent_stream = parent_stream
        super().__init__(**kwargs)

    url_base = "https://api.surveymonkey.com"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        links = response.json().get("links", {})
        if "next" in links:
            return {"next_url": links["next"]}
        else:
            return {}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if next_page_token:
            return urlparse(next_page_token["next_url"]).query
        else:
            return {"per_page": _PAGE_SIZE}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield from response.json().get("data", [])

    @property
    def name(self) -> str:
        return self._name

    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        try:
            return self._path.format(stream_slice=stream_slice)
        except Exception as e:
            raise e

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return self._primary_key

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for _slice in self._parent_stream.stream_slices():
            for parent_record in self._parent_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=_slice):
                yield parent_record

# Source
class SourceSurveyMonkeyDemo(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        first_stream = next(iter(self.streams(config)))

        stream_slice = next(iter(first_stream.stream_slices(sync_mode=SyncMode.full_refresh)))

        try:
            read_stream = first_stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slice)
            first_record = None
            while not first_record:
                first_record = next(read_stream)
                if isinstance(first_record, AirbyteMessage):
                    if first_record.type == "RECORD":
                        first_record = first_record.record
                        return True, None
                    else:
                        first_record = None
            return True, None
        except Exception as e:
            return False, f"Unable to connect to the API with the provided credentials - {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["access_token"])
        surveys = SurveyMonkeyBaseStream(name="surveys", path="/v3/surveys", primary_key="id", data_field="data", cursor_field="date_modified", authenticator=auth)
        survey_responses = SurveyMonkeySubstream(name="survey_responses", path="/v3/surveys/{stream_slice[id]}/responses/", primary_key="id", authenticator=auth, parent_stream=surveys)
        return [
            surveys,
            survey_responses
            ]