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

_PAGE_SIZE = 1000

class SurveyMonkeyBaseStream(HttpStream, ABC):
    def __init__(self, name: str, path: str, primary_key: Union[str, List[str]], data_field: Optional[str], **kwargs: Any) -> None:
        self._name = name
        self._path = path
        self._primary_key = primary_key
        self._data_field = data_field
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
                    "per_page": _PAGE_SIZE}

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
        return [SurveyMonkeyBaseStream(name="surveys", path="v3/surveys", primary_key="id", data_field="data", authenticator=auth)]