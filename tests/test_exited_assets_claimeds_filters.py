import random

import pytest
from assertpy import assert_that

from utils.fetch import fetch_get
from utils.random_data_limit_offset import get_random_limit

URL_1 = "https://stakeway2.indexer-test.gateway.fm/api/v1/events/GetByFiltersExitedAssetsClaimedsIdx1"
URL_2 = "https://stakeway2.indexer-test.gateway.fm/api/v1/events/GetByFiltersExitedAssetsClaimedsIdx1"


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

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

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

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200

    objs = body['values']
    for obj in objs:
        print(f"withdrawnAssets: {int(obj['withdrawnAssets'])}")
        assert_that(int(obj['withdrawnAssets'])).is_greater_than(int(withdrawnAssets))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_withdrawn_assets_ge_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    withdrawnAssets = random_values[7]
    resp, body = fetch_get(url, params=[f'withdrawnAssetsFilterGe={withdrawnAssets}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"withdrawnAssets: {int(obj['withdrawnAssets'])}")
        assert_that(int(obj['withdrawnAssets'])).is_greater_than_or_equal_to(int(withdrawnAssets))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_withdrawn_assets_lt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    withdrawnAssets = random_values[7]
    resp, body = fetch_get(url, params=[f'withdrawnAssetsFilterLt={withdrawnAssets}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"withdrawnAssets: {int(obj['withdrawnAssets'])}")
        assert_that(int(obj['withdrawnAssets'])).is_less_than(int(withdrawnAssets))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_withdrawn_assets_le_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    withdrawnAssets = random_values[7]
    resp, body = fetch_get(url, params=[f'withdrawnAssetsFilterLe={withdrawnAssets}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"withdrawnAssets: {int(obj['withdrawnAssets'])}")
        assert_that(int(obj['withdrawnAssets'])).is_less_than_or_equal_to(int(withdrawnAssets))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_withdrawn_assets_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['withdrawnAssetsSortAsc=true'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

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

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

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
    receiver = random_values[8]
    resp, body = fetch_get(url, params=[f'receiver={receiver}&limit=10000'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"receiver: {obj['receiver']}")
        assert_that((obj['receiver'])).is_equal_to(receiver).described_as(
            f"The receiver filter is not working correctly."
            f"Expected receiver: '{receiver}' in the params, but got Receiver: '{(obj['receiver'])}' in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_receiver_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["receiverSortAsc=True"])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    values = [int(obj["receiver"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] <= values[i + 1], "Receiver are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_receiver_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["receiverSortDesc=True"])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    values = [int(obj["receiver"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] >= values[i + 1], "Receiver are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_prev_position_ticket_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    prevPositionTicket = random_values[2]
    resp, body = fetch_get(url, params=[f'prevPositionTicket={prevPositionTicket}&limit=10000'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

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

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200

    objs = body['values']
    for obj in objs:
        print(f"prevPositionTicket: {int(obj['prevPositionTicket'])}")
        assert_that(int(obj['prevPositionTicket'])).is_greater_than(int(prevPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_prev_position_ticket_ge_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    prevPositionTicket = random_values[2]
    resp, body = fetch_get(url, params=[f'prevPositionTicketFilterGe={prevPositionTicket}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"prevPositionTicket: {int(obj['prevPositionTicket'])}")
        assert_that(int(obj['prevPositionTicket'])).is_greater_than_or_equal_to(int(prevPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_prev_position_ticket_lt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    prevPositionTicket = random_values[2]
    resp, body = fetch_get(url, params=[f'prevPositionTicketFilterLt={prevPositionTicket}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"prevPositionTicket: {int(obj['prevPositionTicket'])}")
        assert_that(int(obj['prevPositionTicket'])).is_less_than(int(prevPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_prev_position_ticket_le_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    prevPositionTicket = random_values[2]
    resp, body = fetch_get(url, params=[f'prevPositionTicketFilterLe={prevPositionTicket}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"prevPositionTicket: {int(obj['prevPositionTicket'])}")
        assert_that(int(obj['prevPositionTicket'])).is_less_than_or_equal_to(int(prevPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_prev_position_ticket_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['prevPositionTicketSortAsc=true'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

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

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

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


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
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


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
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


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
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


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
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


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_block_ts_value_ge(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    blockTs = random_values[1]
    resp, body = fetch_get(url, params=[f'blockTsFilterGe={blockTs}'])
    objs = body['values']
    for obj in objs:
        print(f"blockTs: {obj['blockTs']}")
        assert_that(obj['blockTs']).is_greater_than_or_equal_to(blockTs)


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
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


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
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


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
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


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_new_position_ticket_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    newPositionTicket = random_values[3]
    resp, body = fetch_get(url, params=[f'newPositionTicket={newPositionTicket}&limit=10000'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

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

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200

    objs = body['values']
    for obj in objs:
        print(f"newPositionTicket: {int(obj['newPositionTicket'])}")
        assert_that(int(obj['newPositionTicket'])).is_greater_than(int(newPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_new_position_ticket_ge_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    newPositionTicket = random_values[3]
    resp, body = fetch_get(url, params=[f'newPositionTicketFilterGe={newPositionTicket}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"newPositionTicket: {int(obj['newPositionTicket'])}")
        assert_that(int(obj['newPositionTicket'])).is_greater_than_or_equal_to(int(newPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_new_position_ticket_lt_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    newPositionTicket = random_values[3]
    resp, body = fetch_get(url, params=[f'newPositionTicketFilterLt={newPositionTicket}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"newPositionTicket: {int(obj['newPositionTicket'])}")
        assert_that(int(obj['newPositionTicket'])).is_less_than(int(newPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_new_position_ticket_le_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    newPositionTicket = random_values[3]
    resp, body = fetch_get(url, params=[f'newPositionTicketFilterLe={newPositionTicket}'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"newPositionTicket: {int(obj['newPositionTicket'])}")
        assert_that(int(obj['newPositionTicket'])).is_less_than_or_equal_to(int(newPositionTicket))


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_new_position_ticket_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=['newPositionTicketSortAsc=true'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

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

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

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


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_log_index_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    logIndex = random_values[6]
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


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_indexed_at_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    indexedAt = random_values[5]
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


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_user_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    user = random_values[9]
    resp, body = fetch_get(url, params=[f'user={user}&limit=10000'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"user: {obj['user']}")
        assert_that((obj['user'])).is_equal_to(user).described_as(
            f"The user filter is not working correctly."
            f"Expected user: '{user}' in the params, but got user: '{(obj['user'])}' in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_user_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["userSortAsc=True"])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    values = [int(obj["user"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] <= values[i + 1], "Users are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_user_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["userSortDesc=True"])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    values = [int(obj["user"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] >= values[i + 1], "Users are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_vault_address_filter(extract_values_from_response):
    url, random_values, _ = extract_values_from_response
    vaultAddress = random_values[10]
    resp, body = fetch_get(url, params=[f'vaultAddress={vaultAddress}&limit=10000'])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    objs = body['values']
    for obj in objs:
        print(f"vaultAddress: {obj['vaultAddress']}")
        assert_that((obj['vaultAddress'])).is_equal_to(vaultAddress).described_as(
            f"The vaultAddress filter is not working correctly."
            f"Expected vaultAddress: '{vaultAddress}' in the params, but got vaultAddress: '{(obj['vaultAddress'])}' in the response objects.")
        assert_that(len(objs)).is_equal_to(int(body['total'])).described_as(
            f"Expected 'total' in response body ({body['total']}) to match the length of 'objs' ({len(objs)})")


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_vault_address_sort_asc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["vaultAddressSortAsc=True"])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    values = [int(obj["vaultAddress"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] <= values[i + 1], "VaultAddresses are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
def test_vault_address_sort_desc(extract_values_from_response):
    url, _, _ = extract_values_from_response
    resp, body = fetch_get(url, params=["vaultAddressSortDesc=True"])

    print(f"Status code: {resp.status_code}")
    assert resp.status_code == 200, f"Expected status code 200, but got {resp.status_code} instead."

    values = [int(obj["vaultAddress"], 16) for obj in body["values"]]
    for i in range(len(values) - 1):
        print(values[i])
        assert values[i] >= values[i + 1], "VaultAddresses are not sorted in ascending order."


@pytest.mark.parametrize("extract_values_from_response", [URL_1, URL_2], indirect=True)
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
