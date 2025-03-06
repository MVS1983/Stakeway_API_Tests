import random

import pytest
from assertpy import assert_that

from utils.assert_filters import (
    assert_response_status,
    assert_filter_correctness,
    assert_ge_filter,
    assert_gt_filter,
    assert_le_filter,
    assert_lt_filter,
    assert_sorted_ascending,
    assert_sorted_descending,
    assert_response_object_count,
    assert_tx_hash_filter,
    assert_checking_the_eth_address_and_filter
)
from utils.fetch import fetch_get
from utils.random_data_limit_offset import get_random_limit
from routes.indexer_endpoints import BASE_URL

URL_1 = f"{BASE_URL}api/v1/events/GetByFiltersDepositedsIdx1"
URL_2 = f"{BASE_URL}api/v1/events/GetByFiltersDepositedsIdx2"


@pytest.fixture(params=[URL_1, URL_2])
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
            str(value["shares"]),
            str(value["assets"]),
            str(value["txHash"]),
            int(value["indexedAt"]),
            int(value["logIndex"]),
            str(value["caller"]),
            str(value["receiver"]),
            str(value["referrer"]),
            str(value["user"]),
            str(value["vaultAddress"]),
        )
        for value in body["values"]
    ]

    # Get a random sample of 3 values, ensuring there are at least 1 values to sample
    if len(extracted_values) < 1:
        raise ValueError("Not enough values in the response to extract a sample of 1.")

    random_values = random.choice(extracted_values)
    several_values = random.sample(extracted_values, k=min(5, len(extracted_values)))
    return url, random_values, several_values


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_caller_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[7]
    resp, body = fetch_get(url, params=[f'caller={expected_value}&limit=50'])

    assert_response_status(resp, 200)
    assert_checking_the_eth_address_and_filter(body, 'caller', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_caller_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["callerSortAsc=True"])

    assert_response_status(resp, 200)

    values = [int(obj["caller"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] <= values[i + 1], "Callers are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_caller_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["callerSortDesc=True"])

    assert_response_status(resp, 200)

    values = [int(obj["caller"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] >= values[i + 1], "Callers are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_receiver_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[8]
    resp, body = fetch_get(url, params=[f'receiver={expected_value}&limit=10000'])

    assert_response_status(resp, 200)
    assert_checking_the_eth_address_and_filter(body, 'receiver', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_receiver_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["receiverSortAsc=True"])

    assert_response_status(resp, 200)

    values = [int(obj["receiver"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] <= values[i + 1], "Receiver are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_receiver_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["receiverSortDesc=True"])

    assert_response_status(resp, 200)

    values = [int(obj["receiver"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] >= values[i + 1], "Receiver are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_referrer_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[9]
    resp, body = fetch_get(url, params=[f'referrer={expected_value}&limit=10000'])

    assert_response_status(resp, 200)
    assert_checking_the_eth_address_and_filter(body, 'referrer', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_referrer_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["referrerSortAsc=True"])

    assert_response_status(resp, 200)

    values = [int(obj["referrer"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] <= values[i + 1], "Referrer are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_referrer_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["referrerSortDesc=True"])

    assert_response_status(resp, 200)

    values = [int(obj["referrer"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] >= values[i + 1], "Referrer are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_shares_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'shares'
    expected_value = random_values[2]
    resp, body = fetch_get(url, params=[f"{test_key}={expected_value}&limit=10000"])

    assert_response_status(resp, 200)
    assert_filter_correctness(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_shares_gt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'shares'
    expected_value = random_values[2]
    resp, body = fetch_get(url, params=[f'sharesFilterGt={expected_value}'])

    assert_response_status(resp, 200)
    assert_gt_filter(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_shares_ge_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'shares'
    expected_value = random_values[2]
    resp, body = fetch_get(url, params=[f'sharesFilterGe={expected_value}'])

    assert_response_status(resp, 200)
    assert_ge_filter(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_shares_lt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'shares'
    expected_value = random_values[2]
    resp, body = fetch_get(url, params=[f'sharesFilterLt={expected_value}'])

    assert_response_status(resp, 200)
    assert_lt_filter(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_shares_le_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'shares'
    expected_value = random_values[2]
    resp, body = fetch_get(url, params=[f'sharesFilterLe={expected_value}'])

    assert_response_status(resp, 200)
    assert_le_filter(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_shares_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    test_key = 'shares'
    resp, body = fetch_get(url, params=['sharesSortAsc=True'])

    assert_response_status(resp, 200)
    assert_sorted_ascending(body, test_key)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_shares_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    test_key = 'shares'
    resp, body = fetch_get(url, params=['sharesSortDesc=True'])

    assert_response_status(resp, 200)
    assert_sorted_descending(body, test_key)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_block_number_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'blockNumber'
    expected_value = random_values[0]
    resp, body = fetch_get(url, params=[f"{test_key}={expected_value}"])

    assert_response_status(resp, 200)
    assert_filter_correctness(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_block_number_value_ge(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'blockNumber'
    expected_value = random_values[0]
    resp, body = fetch_get(url, params=[f'blockNumberFilterGe={expected_value}'])

    assert_response_status(resp, 200)
    assert_ge_filter(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_block_number_value_lt(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'blockNumber'
    expected_value = random_values[0]
    resp, body = fetch_get(url, params=[f'blockNumberFilterLt={expected_value}'])

    assert_response_status(resp, 200)
    assert_lt_filter(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_block_number_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['blockNumberSortAsc=True'])

    assert_response_status(resp, 200)
    assert_sorted_ascending(body, 'blockNumber')


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_block_ts_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'blockTs'
    expected_value = random_values[1]
    resp, body = fetch_get(url, params=[f'blockTs={expected_value}'])

    assert_response_status(resp, 200)
    assert_filter_correctness(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_block_ts_value_ge(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[1]
    resp, body = fetch_get(url, params=[f'blockTsFilterGe={expected_value}'])

    assert_response_status(resp, 200)
    assert_ge_filter(body, 'blockTs', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_block_ts_value_lt(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[1]
    resp, body = fetch_get(url, params=[f'blockTsFilterLt={expected_value}'])

    assert_response_status(resp, 200)
    assert_lt_filter(body, 'blockTs', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_block_ts_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['blockTsSortAsc=True'])

    assert_response_status(resp, 200)
    assert_sorted_ascending(body, 'blockTs')


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_block_ts_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['blockTsSortDesc=True'])

    assert_response_status(resp, 200)
    assert_sorted_descending(body, 'blockTs')


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_assets_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'assets'
    expected_value = random_values[3]
    resp, body = fetch_get(url, params=[f"{test_key}={expected_value}&limit=10000"])

    assert_response_status(resp, 200)
    assert_filter_correctness(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_assets_gt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'assets'
    expected_value = random_values[3]
    resp, body = fetch_get(url, params=[f'assetsFilterGt={expected_value}'])

    assert_response_status(resp, 200)
    assert_gt_filter(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_assets_ge_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'assets'
    expected_value = random_values[3]
    resp, body = fetch_get(url, params=[f'assetsFilterGe={expected_value}'])

    assert_response_status(resp, 200)
    assert_ge_filter(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_assets_lt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'assets'
    expected_value = random_values[3]
    resp, body = fetch_get(url, params=[f'assetsFilterLt={expected_value}'])

    assert_response_status(resp, 200)
    assert_lt_filter(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_assets_le_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    test_key = 'assets'
    expected_value = random_values[3]
    resp, body = fetch_get(url, params=[f'assetsFilterLe={expected_value}'])

    assert_response_status(resp, 200)
    assert_le_filter(body, test_key, expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_assets_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['assetsSortAsc=True'])

    assert_response_status(resp, 200)
    assert_sorted_ascending(body, 'assets')


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_assets_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['assetsSortDesc=True'])

    assert_response_status(resp, 200)
    assert_sorted_descending(body, 'assets')


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_tx_hash_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    txHash = random_values[4]
    resp, body = fetch_get(url, params=[f'txHash={txHash}'])

    assert_response_status(resp, 200)
    assert_tx_hash_filter(body, txHash)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_log_index_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[6]
    resp, body = fetch_get(url, params=[f'logIndex={expected_value}&limit={10000}'])

    assert_response_status(resp, 200)
    assert_filter_correctness(body, 'logIndex', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_indexed_at_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[5]
    resp, body = fetch_get(url, params=[f'indexedAt={expected_value}'])

    assert_response_status(resp, 200)
    assert_filter_correctness(body, 'indexedAt', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_user_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[10]
    resp, body = fetch_get(url, params=[f'user={expected_value}&limit=10000'])

    assert_response_status(resp, 200)
    assert_checking_the_eth_address_and_filter(body, 'user', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_user_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["userSortAsc=True"])

    assert_response_status(resp, 200)

    values = [int(obj["user"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] <= values[i + 1], "Users are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_user_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["userSortDesc=True"])

    assert_response_status(resp, 200)

    values = [int(obj["user"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] >= values[i + 1], "Users are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_vault_address_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    expected_value = random_values[11]
    resp, body = fetch_get(url, params=[f'vaultAddress={expected_value}&limit=10000'])

    assert_response_status(resp, 200)
    assert_checking_the_eth_address_and_filter(body, 'vaultAddress', expected_value)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_vault_address_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["vaultAddressSortAsc=True"])

    assert_response_status(resp, 200)

    values = [int(obj["vaultAddress"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] <= values[i + 1], "VaultAddresses are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_vault_address_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["vaultAddressSortDesc=True"])

    assert_response_status(resp, 200)

    values = [int(obj["vaultAddress"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] >= values[i + 1], "VaultAddresses are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_random_limit(extract_values_from_response):
    url, _, _ = extract_values_from_response
    random_limit = get_random_limit()
    resp, body = fetch_get(url, params=[f'limit={random_limit}'])

    assert_response_status(resp, 200)
    assert_response_object_count(body, random_limit)


def extract_field_value(obj, filter_name):
    """Extracts the relevant field value from a response object based on the filter name."""
    field_map = {
        "assetsFilterIn": int(obj["assets"]),
        "sharesFilterIn": obj["shares"],
        "txHashFilterIn": obj["txHash"],
        "blockNumberFilterIn": int(obj["blockNumber"]),
        "callerFilterIn": obj["caller"],
        "receiverFilterIn": obj["receiver"],
        "referrerFilterIn": obj["referrer"],
        "userFilterIn": obj["user"],
        "vaultAddressFilterIn": obj["vaultAddress"],
    }
    return field_map[filter_name]


@pytest.mark.parametrize(
    "filter_name, extract_values_from_response",
    [
        ("assetsFilterIn", URL_1),
        ("assetsFilterIn", URL_2),
        ("sharesFilterIn", URL_1),
        ("sharesFilterIn", URL_2),
        ("txHashFilterIn", URL_1),
        ("txHashFilterIn", URL_2),
        ("blockNumberFilterIn", URL_1),
        ("blockNumberFilterIn", URL_2),
        ("callerFilterIn", URL_1),
        ("callerFilterIn", URL_2),
        ("receiverFilterIn", URL_1),
        ("receiverFilterIn", URL_2),
        ("referrerFilterIn", URL_1),
        ("referrerFilterIn", URL_2),
        ("userFilterIn", URL_1),
        ("userFilterIn", URL_2),
        ("vaultAddressFilterIn", URL_1),
        ("vaultAddressFilterIn", URL_2),

    ],
    indirect=["extract_values_from_response"]
)
def test_filter_in(filter_name, extract_values_from_response):
    url, _, several_values = extract_values_from_response
    # Extract corresponding field values for the current filter
    field_map = {
        "assetsFilterIn": 3,
        "sharesFilterIn": 2,
        "txHashFilterIn": 4,
        "blockNumberFilterIn": 0,
        "callerFilterIn": 7,
        "receiverFilterIn": 8,
        "referrerFilterIn": 9,
        "userFilterIn": 10,
        "vaultAddressFilterIn": 11
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
