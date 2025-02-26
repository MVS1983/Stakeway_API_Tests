import random

import pytest
from assertpy import assert_that

from utils.fetch import fetch_get
from utils.random_data_limit_offset import get_random_limit

URL = "https://stakeway2.indexer-test.gateway.fm/api/v1/events/GetByFiltersMetadataUpdateds"


@pytest.fixture()
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
            str(value["metadataIpfsHash"])
        )
        for value in body["values"]
    ]

    # Get a random sample of 3 values, ensuring there are at least 1 values to sample
    if len(extracted_values) < 1:
        raise ValueError("Not enough values in the response to extract a sample of 1.")

    random_values = random.choice(extracted_values)
    several_values = random.sample(extracted_values, k=1)
    return url, random_values, several_values


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_caller_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    caller = random_values[5]
    resp, body = fetch_get(url, params=[f'caller={caller}&limit=10000'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"caller: {obj['caller']}")
        assert_that((obj['caller'])).is_equal_to(caller).described_as(
            f"The caller filter is not working correctly."
            f"Expected caller: '{caller}' in the params, but got Caller: '{(obj['caller'])}' in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_caller_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["callerSortAsc=True"])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    values = [int(obj["caller"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] <= values[i + 1], "Callers are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_caller_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["callerSortDesc=True"])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    values = [int(obj["caller"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] >= values[i + 1], "Callers are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_metadata_ipfs_hash_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    metadataIpfsHash = random_values[6]
    resp, body = fetch_get(url, params=[f'metadataIpfsHash={metadataIpfsHash}&limit=10000'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"metadataIpfsHash: {obj['metadataIpfsHash']}")
        assert_that((obj['metadataIpfsHash'])).is_equal_to(metadataIpfsHash).described_as(
            f"The metadataIpfsHash filter is not working correctly."
            f"Expected metadataIpfsHash: '{metadataIpfsHash}' in the params, but got metadataIpfsHash: '{(obj['metadataIpfsHash'])}' in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_metadata_ipfs_hash_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["metadataIpfsHashSortAsc=True"])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    values = [obj["metadataIpfsHash"] for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] <= values[i + 1], "MetadataIpfsHashs are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_metadata_ipfs_hash_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["metadataIpfsHashSortDesc=True"])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    values = [obj["metadataIpfsHash"] for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] >= values[i + 1], "MetadataIpfsHashs are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_block_number_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    blockNumber = random_values[0]
    resp, body = fetch_get(url, params=[f'blockNumber={blockNumber}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"blockNumber: {int(obj['blockNumber'])}")
        assert_that(int(obj['blockNumber'])).is_equal_to(blockNumber).described_as(
            f"The blockNumber filter is not working correctly."
            f"Expected blockNumber: '{blockNumber}' in the params, but got blockNumber: '{int(obj['blockNumber'])}'"
            f" in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_block_number_value_ge(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    blockNumber = random_values[0]
    resp, body = fetch_get(url, params=[f'blockNumberFilterGe={blockNumber}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"blockNumber: {int(obj['blockNumber'])}")
        assert_that(int(obj['blockNumber'])).is_greater_than_or_equal_to(blockNumber)


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_block_number_value_lt(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    blockNumber = random_values[0]
    resp, body = fetch_get(url, params=[f'blockNumberFilterLt={blockNumber}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"blockNumber: {int(obj['blockNumber'])}")
        assert_that(int(obj['blockNumber'])).is_less_than(blockNumber)


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_block_number_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['blockNumberSortAsc=true'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    # Convert all values to integers
    values = [int(obj['blockNumber']) for obj in objs]

    # Check if values are sorted in ascending order
    for i in range(len(values) - 1):
        print(f"blockNumber: {values[i]}")
        assert values[i] <= values[i + 1], f"Values are not sorted in ascending order: {values}"

    print("All values are sorted in ascending order.")


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_block_ts_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    blockTs = random_values[1]
    resp, body = fetch_get(url, params=[f'blockTs={blockTs}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"blockTs: {obj['blockTs']}")
        assert_that(obj['blockTs']).is_equal_to(blockTs).described_as(
            f"The blockTs filter is not working correctly."
            f"Expected blockTs: '{blockTs}' in the params, but got blockTs: '{obj['blockTs']}'"
            f" in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_block_ts_value_ge(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    blockTs = random_values[1]
    resp, body = fetch_get(url, params=[f'blockTsFilterGe={blockTs}'])
    objs = body['values']
    for obj in objs:
        print(f"blockTs: {obj['blockTs']}")
        assert_that(obj['blockTs']).is_greater_than_or_equal_to(blockTs)


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_block_ts_value_lt(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    blockTs = random_values[1]
    resp, body = fetch_get(url, params=[f'blockTsFilterLt={blockTs}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"blockTs: {obj['blockTs']}")
        assert_that(obj['blockTs']).is_less_than(blockTs)


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_block_ts_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['blockTsSortAsc=true'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    # Convert all values to integers
    values = [int(obj['blockTs']) for obj in objs]

    # Check if values are sorted in ascending order
    for i in range(len(values) - 1):
        print(f"blockTs: {values[i]}")
        assert values[i] <= values[i + 1], f"blockTs values are not sorted in ascending order: {values}"

    print("All blockTs values are sorted in ascending order.")


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_block_ts_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['blockTsSortDesc=true'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']

    # Convert all values to integers
    values = [int(obj['blockTs']) for obj in objs]

    # Check if values are sorted in descending order
    for i in range(len(values) - 1):
        print(f"blockTs: {values[i]}")
        assert values[i] >= values[i + 1], f"blockTs values are not sorted in descending order: {values}"

    print("All blockTs values are sorted in descending order.")


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_tx_hash_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    txHash = random_values[5]
    resp, body = fetch_get(url, params=[f'txHash={txHash}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"txHash: {obj['txHash']}")
        assert_that(obj['txHash']).is_equal_to(txHash).described_as(
            f"The txHash filter is not working correctly."
            f"Expected txHash: '{txHash}' in the params, but got txHash: '{obj['txHash']}'"
            f" in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_log_index_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    logIndex = random_values[4]
    resp, body = fetch_get(url, params=[f'logIndex={logIndex}&limit={10000}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"logIndex: {obj['logIndex']}")
        assert_that(int(obj['logIndex'])).is_equal_to(logIndex).described_as(
            f"The logIndex filter is not working correctly."
            f"Expected logIndex: '{logIndex}' in the params, but got logIndex: '{obj['logIndex']}'"
            f" in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")
    print(f"Total: {body['total']}")


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_indexed_at_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    indexedAt = random_values[3]
    resp, body = fetch_get(url, params=[f'indexedAt={indexedAt}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"indexedAt: {obj['indexedAt']}")
        assert_that(obj['indexedAt']).is_equal_to(indexedAt).described_as(
            f"The indexedAt filter is not working correctly."
            f"Expected indexedAt: '{indexedAt}' in the params, but got indexedAt: '{obj['indexedAt']}'"
            f" in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL],
                         indirect=True)
def test_random_limit(extract_values_from_response):
    url, _, _ = extract_values_from_response

    # Generate a random limit value
    random_limit = get_random_limit()

    resp, body = fetch_get(url, params=[f'limit={random_limit}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']

    print(f"URL: {url}, Limit: {random_limit}, Object Count: {len(objs)}")

    assert_that(len(objs)).is_less_than_or_equal_to(random_limit).described_as(
        f"The number of objects returned in the response "
        f"{len(objs)} does not match the expected value {random_limit}")


def extract_field_value(obj, filter_name):
    """Extracts the relevant field value from a response object based on the filter name."""
    field_map = {
        "txHashFilterIn": obj["txHash"],
        "blockNumberFilterIn": int(obj["blockNumber"]),
        "callerFilterIn": obj["caller"],
        "metadataIpfsHashFilterIn": obj["metadataIpfsHash"],
    }
    return field_map[filter_name]


@pytest.mark.parametrize(
    "filter_name, extract_values_from_response",
    [
        ("txHashFilterIn", URL),
        ("blockNumberFilterIn", URL),
        ("callerFilterIn", URL),
        ("metadataIpfsHashFilterIn", URL)
    ], indirect=["extract_values_from_response"]
)
def test_filter_in(filter_name, extract_values_from_response):
    url, _, several_values = extract_values_from_response
    # Extract corresponding field values for the current filter
    field_map = {
        "txHashFilterIn": 2,
        "blockNumberFilterIn": 0,
        "callerFilterIn": 5,
        "metadataIpfsHashFilterIn": 6,
    }
    values = [item[field_map[filter_name]] for item in several_values]
    filter_value = ','.join(map(str, values))

    resp, body = fetch_get(url, params=[f"{filter_name}={filter_value}&limit=10000"])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

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
