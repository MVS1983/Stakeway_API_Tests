import random

import pytest
from assertpy import assert_that

from utils.assert_filters import (
    assert_response_status,
    assert_filter_correctness,
    assert_ge_filter,
    assert_lt_filter,
    assert_sorted_ascending,
    assert_sorted_descending,
    assert_response_object_count,
    assert_tx_hash_filter,
    assert_checking_the_eth_address_and_filter,
    assert_sorted_ascending_with_hexadecimal_values,
    assert_sorted_descending_with_hexadecimal_values
)
from utils.fetch import fetch_get
from utils.random_data_limit_offset import get_random_limit
from routes.indexer_endpoints import BASE_URL

URL_1 = f"{BASE_URL}api/v1/events/GetByFiltersValidatorsRootUpdatedsIdx1"
URL_2 = f"{BASE_URL}api/v1/events/GetByFiltersValidatorsRootUpdatedsIdx2"
URL_3 = f"{BASE_URL}api/v1/events/GetByFiltersValidatorsRootUpdatedsIdx3"


@pytest.fixture(params=[URL_1, URL_2, URL_3])
def extract_values_from_response(request):
    """
    Extract specific values from the response.
    """
    url = request.param  # Access the parameterized value
    resp, body = fetch_get(url, params=[])

    # Extract relevant fields from the response
    extracted_values = [
        (
            int(value["blockNumber"]),
            int(value["blockTs"]),
            str(value["txHash"]),
            int(value["indexedAt"]),
            int(value["logIndex"]),
            str(value["caller"]),
            str(value["validatorsRoot"])
        )
        for value in body["values"]
    ]

    # Get a random sample of 3 values, ensuring there are at least 1 values to sample
    if len(extracted_values) < 1:
        reason = "Skipping test: Not enough values in the response to extract a sample of 1."
        print(reason)  # Log the reason
        pytest.skip(reason)

    random_values = random.choice(extracted_values)
    several_values = random.sample(extracted_values, k=min(5, len(extracted_values)))
    return url, random_values, several_values


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_caller_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[5]
    resp, body = fetch_get(url, params=[f'caller={expected_value}&limit=50'])

    assert_response_status(resp, 200)
    assert_checking_the_eth_address_and_filter(body, 'caller', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_caller_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["callerSortAsc=True"])

    assert_response_status(resp, 200)
    assert_sorted_ascending_with_hexadecimal_values(body, 'caller')


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_caller_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["callerSortDesc=True"])

    assert_response_status(resp, 200)
    assert_sorted_descending_with_hexadecimal_values(body, 'caller')


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_validators_root_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    validatorsRoot = random_values[6]
    resp, body = fetch_get(url, params=[f'validatorsRoot={validatorsRoot}&limit=10000'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"validatorsRoot: {obj['validatorsRoot']}")
        assert_that((obj['validatorsRoot'])).is_equal_to(validatorsRoot).described_as(
            f"The validatorsRoot filter is not working correctly."
            f"Expected validatorsRoot: '{validatorsRoot}' in the params, but got validatorsRoot: '{(obj['validatorsRoot'])}' in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_validators_root_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["validatorsRootSortAsc=True"])

    assert_response_status(resp, 200)

    values = [int(obj["validatorsRoot"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] <= values[i + 1], "validatorsRoot are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_validators_root_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["validatorsRootSortDesc=True"])

    assert_response_status(resp, 200)

    values = [int(obj["validatorsRoot"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] >= values[i + 1], "validatorsRoot are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_block_number_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'blockNumber'
    expected_value = random_values[0]
    resp, body = fetch_get(url, params=[f"{test_key}={expected_value}"])

    assert_response_status(resp, 200)
    assert_filter_correctness(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_block_number_value_ge(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'blockNumber'
    expected_value = random_values[0]
    resp, body = fetch_get(url, params=[f'blockNumberFilterGe={expected_value}'])

    assert_response_status(resp, 200)
    assert_ge_filter(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_block_number_value_lt(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'blockNumber'
    expected_value = random_values[0]
    resp, body = fetch_get(url, params=[f'blockNumberFilterLt={expected_value}'])

    assert_response_status(resp, 200)
    assert_lt_filter(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_block_number_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['blockNumberSortAsc=True'])

    assert_response_status(resp, 200)
    assert_sorted_ascending(body, 'blockNumber')


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_block_ts_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'blockTs'
    expected_value = random_values[1]
    resp, body = fetch_get(url, params=[f'blockTs={expected_value}'])

    assert_response_status(resp, 200)
    assert_filter_correctness(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_block_ts_value_ge(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[1]
    resp, body = fetch_get(url, params=[f'blockTsFilterGe={expected_value}'])

    assert_response_status(resp, 200)
    assert_ge_filter(body, 'blockTs', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_block_ts_value_lt(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[1]
    resp, body = fetch_get(url, params=[f'blockTsFilterLt={expected_value}'])

    assert_response_status(resp, 200)
    assert_lt_filter(body, 'blockTs', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_block_ts_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['blockTsSortAsc=True'])

    assert_response_status(resp, 200)
    assert_sorted_ascending(body, 'blockTs')


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_block_ts_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['blockTsSortDesc=True'])

    assert_response_status(resp, 200)
    assert_sorted_descending(body, 'blockTs')


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_tx_hash_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    txHash = random_values[2]
    resp, body = fetch_get(url, params=[f'txHash={txHash}'])

    assert_response_status(resp, 200)
    assert_tx_hash_filter(body, txHash)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_log_index_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[4]
    resp, body = fetch_get(url, params=[f'logIndex={expected_value}&limit={10000}'])

    assert_response_status(resp, 200)
    assert_filter_correctness(body, 'logIndex', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_indexed_at_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[3]
    resp, body = fetch_get(url, params=[f'indexedAt={expected_value}'])

    assert_response_status(resp, 200)
    assert_filter_correctness(body, 'indexedAt', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2, URL_3], indirect=True)
def test_random_limit(extract_values_from_response):
    url, _, _ = extract_values_from_response
    random_limit = get_random_limit()
    resp, body = fetch_get(url, params=[f'limit={random_limit}'])

    assert_response_status(resp, 200)
    assert_response_object_count(body, random_limit)


def extract_field_value(obj, filter_name):
    """Extracts the relevant field value from a response object based on the filter name."""
    field_map = {
        "txHashFilterIn": obj["txHash"],
        "blockNumberFilterIn": int(obj["blockNumber"]),
        "callerFilterIn": obj["caller"],
        "validatorsRootFilterIn": obj["validatorsRoot"],
    }
    return field_map[filter_name]


@pytest.mark.parametrize(
    "filter_name, extract_values_from_response",
    [
        ("txHashFilterIn", URL_1),
        ("blockNumberFilterIn", URL_1),
        ("callerFilterIn", URL_1),
        ("validatorsRootFilterIn", URL_1),
        ("txHashFilterIn", URL_2),
        ("blockNumberFilterIn", URL_2),
        ("callerFilterIn", URL_2),
        ("validatorsRootFilterIn", URL_2),
        ("txHashFilterIn", URL_3),
        ("blockNumberFilterIn", URL_3),
        ("callerFilterIn", URL_3),
        ("validatorsRootFilterIn", URL_3)
    ], indirect=["extract_values_from_response"]
)
def test_filter_in(filter_name, extract_values_from_response):
    url, _, several_values = extract_values_from_response
    # Extract corresponding field values for the current filter
    field_map = {
        "txHashFilterIn": 2,
        "blockNumberFilterIn": 0,
        "callerFilterIn": 5,
        "validatorsRootFilterIn": 6,
    }
    values = [item[field_map[filter_name]] for item in several_values]
    filter_value = ','.join(map(str, values))

    resp, body = fetch_get(url, params=[f"{filter_name}={filter_value}&limit=10000"])

    assert_response_status(resp, 200)

    objs = body["values"]
    obj_value_list = [extract_field_value(obj, filter_name) for obj in objs]

    # Assert the filter works correctly
    assert_that(set(map(str, obj_value_list))).is_equal_to(set(map(str, values))).described_as(
        f"The {filter_name} is not working correctly. "
        f"Expected: '{set(values)}', but got: '{set(obj_value_list)}'."
    )
    print(f"Values from params: {set(values)}, values from response: {set(obj_value_list)}")

    # Assert 'total' matches the number of objects
    assert_that(len(objs)).is_equal_to(int(body["total"])).described_as(
        f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)}).")
