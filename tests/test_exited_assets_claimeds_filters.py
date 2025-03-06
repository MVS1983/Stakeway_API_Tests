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
    assert_checking_the_eth_address_and_filter
)
from utils.fetch import fetch_get
from utils.random_data_limit_offset import get_random_limit
from routes.indexer_endpoints import BASE_URL

URL_1 = f"{BASE_URL}api/v1/events/GetByFiltersExitedAssetsClaimedsIdx1"
URL_2 = f"{BASE_URL}api/v1/events/GetByFiltersExitedAssetsClaimedsIdx1"


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
            str(value["prevPositionTicket"]),
            str(value["newPositionTicket"]),
            str(value["txHash"]),
            int(value["indexedAt"]),
            int(value["logIndex"]),
            str(value["withdrawnAssets"]),
            str(value["receiver"]),
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
def test_withdrawn_assets_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    withdrawnAssets = random_values[7]
    resp, body = fetch_get(url, params=[f'withdrawnAssets={withdrawnAssets}&limit=10000'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"withdrawnAssets: {obj['withdrawnAssets']}")
        assert_that((obj['withdrawnAssets'])).is_equal_to(withdrawnAssets).described_as(
            f"The withdrawnAssets filter is not working correctly."
            f"Expected withdrawnAssets: '{withdrawnAssets}' in the params, but got withdrawnAssets: '{(obj['withdrawnAssets'])}' in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_withdrawn_assets_gt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    withdrawnAssets = random_values[7]
    resp, body = fetch_get(url, params=[f'withdrawnAssetsFilterGt={withdrawnAssets}'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"withdrawnAssets: {int(obj['withdrawnAssets'])}")
        assert_that(int(obj['withdrawnAssets'])).is_greater_than(int(withdrawnAssets))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_withdrawn_assets_ge_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    withdrawnAssets = random_values[7]
    resp, body = fetch_get(url, params=[f'withdrawnAssetsFilterGe={withdrawnAssets}'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"withdrawnAssets: {int(obj['withdrawnAssets'])}")
        assert_that(int(obj['withdrawnAssets'])).is_greater_than_or_equal_to(int(withdrawnAssets))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_withdrawn_assets_lt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    withdrawnAssets = random_values[7]
    resp, body = fetch_get(url, params=[f'withdrawnAssetsFilterLt={withdrawnAssets}'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"withdrawnAssets: {int(obj['withdrawnAssets'])}")
        assert_that(int(obj['withdrawnAssets'])).is_less_than(int(withdrawnAssets))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_withdrawn_assets_le_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    withdrawnAssets = random_values[7]
    resp, body = fetch_get(url, params=[f'withdrawnAssetsFilterLe={withdrawnAssets}'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"withdrawnAssets: {int(obj['withdrawnAssets'])}")
        assert_that(int(obj['withdrawnAssets'])).is_less_than_or_equal_to(int(withdrawnAssets))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_withdrawn_assets_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['withdrawnAssetsSortAsc=true'])

    assert_response_status(resp, 200)

    objs = body['values']
    # Convert all values to integers
    values = [int(obj['withdrawnAssets']) for obj in objs]

    # Check if values are sorted in ascending order
    for i in range(len(values) - 1):
        assert values[i] <= values[i + 1], f"withdrawnAssets are not sorted in ascending order: {values}"

    print("All withdrawnAssets are sorted in ascending order.")


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_withdrawn_assets_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['withdrawnAssetsSortDesc=true'])

    assert_response_status(resp, 200)

    objs = body['values']
    # Convert all values to integers
    values = [int(obj['withdrawnAssets']) for obj in objs]

    # Check if values are sorted in descending order
    for i in range(len(values) - 1):
        assert values[i] >= values[i + 1], f"withdrawnAssets are not sorted in descending order: {values}"

    print("All withdrawnAssets are sorted in descending order.")


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
def test_prev_position_ticket_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    prevPositionTicket = random_values[2]
    resp, body = fetch_get(url, params=[f'prevPositionTicket={prevPositionTicket}&limit=10000'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"prevPositionTicket: {int(obj['prevPositionTicket'])}")
        assert_that(int(obj['prevPositionTicket'])).is_equal_to(int(prevPositionTicket)).described_as(
            f"The prevPositionTicket filter is not working correctly."
            f"Expected prevPositionTicket: '{prevPositionTicket}' in the params, but got prevPositionTicket: '{int(obj['prevPositionTicket'])}' in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_prev_position_ticket_gt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    prevPositionTicket = random_values[2]
    resp, body = fetch_get(url, params=[f'prevPositionTicketFilterGt={prevPositionTicket}'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"prevPositionTicket: {int(obj['prevPositionTicket'])}")
        assert_that(int(obj['prevPositionTicket'])).is_greater_than(int(prevPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_prev_position_ticket_ge_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    prevPositionTicket = random_values[2]
    resp, body = fetch_get(url, params=[f'prevPositionTicketFilterGe={prevPositionTicket}'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"prevPositionTicket: {int(obj['prevPositionTicket'])}")
        assert_that(int(obj['prevPositionTicket'])).is_greater_than_or_equal_to(int(prevPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_prev_position_ticket_lt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    prevPositionTicket = random_values[2]
    resp, body = fetch_get(url, params=[f'prevPositionTicketFilterLt={prevPositionTicket}'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"prevPositionTicket: {int(obj['prevPositionTicket'])}")
        assert_that(int(obj['prevPositionTicket'])).is_less_than(int(prevPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_prev_position_ticket_le_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    prevPositionTicket = random_values[2]
    resp, body = fetch_get(url, params=[f'prevPositionTicketFilterLe={prevPositionTicket}'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"prevPositionTicket: {int(obj['prevPositionTicket'])}")
        assert_that(int(obj['prevPositionTicket'])).is_less_than_or_equal_to(int(prevPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_prev_position_ticket_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['prevPositionTicketSortAsc=true'])

    assert_response_status(resp, 200)

    objs = body['values']
    # Convert all values to integers
    values = [int(obj['prevPositionTicket']) for obj in objs]

    # Check if values are sorted in ascending order
    for i in range(len(values) - 1):
        assert values[i] <= values[i + 1], f"prevPositionTickets are not sorted in ascending order: {values}"

    print("All prevPositionTickets are sorted in ascending order.")


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_prev_position_ticket_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['prevPositionTicketSortDesc=true'])

    assert_response_status(resp, 200)

    objs = body['values']
    # Convert all values to integers
    values = [int(obj['prevPositionTicket']) for obj in objs]

    # Check if values are sorted in descending order
    for i in range(len(values) - 1):
        assert values[i] >= values[i + 1], f"prevPositionTickets are not sorted in descending order: {values}"

    print("All prevPositionTickets are sorted in descending order.")


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
def test_new_position_ticket_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    newPositionTicket = random_values[3]
    resp, body = fetch_get(url, params=[f'newPositionTicket={newPositionTicket}&limit=10000'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"newPositionTicket: {int(obj['newPositionTicket'])}")
        assert_that(int(obj['newPositionTicket'])).is_equal_to(int(newPositionTicket)).described_as(
            f"The newPositionTicket filter is not working correctly."
            f"Expected newPositionTicket: '{newPositionTicket}' in the params, but got newPositionTicket: '{int(obj['newPositionTicket'])}' in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_new_position_ticket_gt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    newPositionTicket = random_values[3]
    resp, body = fetch_get(url, params=[f'newPositionTicketFilterGt={newPositionTicket}'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"newPositionTicket: {int(obj['newPositionTicket'])}")
        assert_that(int(obj['newPositionTicket'])).is_greater_than(int(newPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_new_position_ticket_ge_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    newPositionTicket = random_values[3]
    resp, body = fetch_get(url, params=[f'newPositionTicketFilterGe={newPositionTicket}'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"newPositionTicket: {int(obj['newPositionTicket'])}")
        assert_that(int(obj['newPositionTicket'])).is_greater_than_or_equal_to(int(newPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_new_position_ticket_lt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    newPositionTicket = random_values[3]
    resp, body = fetch_get(url, params=[f'newPositionTicketFilterLt={newPositionTicket}'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"newPositionTicket: {int(obj['newPositionTicket'])}")
        assert_that(int(obj['newPositionTicket'])).is_less_than(int(newPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_new_position_ticket_le_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    newPositionTicket = random_values[3]
    resp, body = fetch_get(url, params=[f'newPositionTicketFilterLe={newPositionTicket}'])

    assert_response_status(resp, 200)

    objs = body['values']
    for obj in objs:
        print(f"newPositionTicket: {int(obj['newPositionTicket'])}")
        assert_that(int(obj['newPositionTicket'])).is_less_than_or_equal_to(int(newPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_new_position_ticket_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['newPositionTicketSortAsc=true'])

    assert_response_status(resp, 200)

    objs = body['values']
    # Convert all values to integers
    values = [int(obj['newPositionTicket']) for obj in objs]

    # Check if values are sorted in ascending order
    for i in range(len(values) - 1):
        assert values[i] <= values[i + 1], f"newPositionTickets are not sorted in ascending order: {values}"

    print("All newPositionTickets are sorted in ascending order.")


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_new_position_ticket_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['newPositionTicketSortDesc=true'])

    assert_response_status(resp, 200)

    objs = body['values']
    # Convert all values to integers
    values = [int(obj['newPositionTicket']) for obj in objs]

    # Check if values are sorted in descending order
    for i in range(len(values) - 1):
        assert values[i] >= values[i + 1], f"newPositionTickets are not sorted in descending order: {values}"

    print("All newPositionTickets are sorted in descending order.")


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
    expected_value = random_values[9]
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
    expected_value = random_values[10]
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
        "newPositionTicketFilterIn": int(obj["newPositionTicket"]),
        "prevPositionTicketFilterIn": obj["prevPositionTicket"],
        "txHashFilterIn": obj["txHash"],
        "blockNumberFilterIn": int(obj["blockNumber"]),
        "withdrawnAssetsFilterIn": obj["withdrawnAssets"],
        "receiverFilterIn": obj["receiver"],
        "userFilterIn": obj["user"],
        "vaultAddressFilterIn": obj["vaultAddress"],
    }
    return field_map[filter_name]


@pytest.mark.parametrize(
    "filter_name, extract_values_from_response",
    [
        ("newPositionTicketFilterIn", URL_1),
        ("newPositionTicketFilterIn", URL_2),
        ("prevPositionTicketFilterIn", URL_1),
        ("prevPositionTicketFilterIn", URL_2),
        ("txHashFilterIn", URL_1),
        ("txHashFilterIn", URL_2),
        ("blockNumberFilterIn", URL_1),
        ("blockNumberFilterIn", URL_2),
        ("withdrawnAssetsFilterIn", URL_1),
        ("withdrawnAssetsFilterIn", URL_2),
        ("receiverFilterIn", URL_1),
        ("receiverFilterIn", URL_2),
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
        "newPositionTicketFilterIn": 3,
        "prevPositionTicketFilterIn": 2,
        "txHashFilterIn": 4,
        "blockNumberFilterIn": 0,
        "withdrawnAssetsFilterIn": 7,
        "receiverFilterIn": 8,
        "userFilterIn": 9,
        "vaultAddressFilterIn": 10,
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
