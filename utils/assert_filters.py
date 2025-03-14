from assertpy import assert_that


def assert_response_status(resp, expected_status):
    """Asserts that the response status code is as expected."""
    print(f"Status code: {resp.status_code}")
    assert_that(resp.status_code, f"Expected status code {expected_status}, but got {resp.status_code}").is_equal_to(expected_status)


def assert_filter_correctness(body, test_key, expected_value):
    """Asserts that all returned objects have the expected value for the given test_key."""
    objs = body.get('values', [])
    total = body.get('total', 0)

    for obj in objs:
        print(f"{test_key}: {obj.get(test_key)}")
        actual_value = int(obj.get(test_key, -1))  # Default to -1 to catch missing keys
        assert_that(actual_value).is_equal_to(int(expected_value)).described_as(
            f"The {test_key} filter is not working correctly.\n"
            f"Expected {test_key}: '{expected_value}', but got '{actual_value}'.")

    assert_that(len(objs)).is_equal_to(int(total)).described_as(
        f"Expected 'total' in response body ({total}) to match the length of 'objs' ({len(objs)}).")


def assert_gt_filter(body, test_key, expected_value):
    """
    Validate that the specified key in the response body is greater than to expected_value.

    param body: The JSON response body.
    param test_key: The test_key whose values should be Validated.
    param expected_value: The expected value pass to params.
    """
    objs = body.get('values', [])

    for obj in objs:
        print(f"{test_key}: {int(obj.get(test_key))}")
        assert_that(int(obj.get(test_key))).is_greater_than(int(expected_value))


def assert_ge_filter(body, test_key, expected_value):
    """
    Validate that the specified key in the response body is greater or equal to expected_value.

    param body: The JSON response body.
    param test_key: The test_key whose values should be Validated.
    param expected_value: The expected value pass to params.
    """
    objs = body.get('values', [])

    for obj in objs:
        print(f"{test_key}: {int(obj.get(test_key))}")
        assert_that(int(obj.get(test_key))).is_greater_than_or_equal_to(int(expected_value))


def assert_lt_filter(body, test_key, expected_value):
    """
    Validate that the specified key in the response body is less than to expected_value.

    param body: The JSON response body.
    param test_key: The test_key whose values should be Validated.
    param expected_value: The expected value pass to params.
    """
    objs = body.get('values', [])

    for obj in objs:
        print(f"{test_key}: {int(obj.get(test_key))}")
        assert_that(int(obj.get(test_key))).is_less_than(int(expected_value))


def assert_le_filter(body, test_key, expected_value):
    """
    Validate that the specified key in the response body is less than or equal to expected_value.

    param body: The JSON response body.
    param test_key: The test_key whose values should be Validated.
    param expected_value: The expected value pass to params.
    """
    objs = body.get('values', [])

    for obj in objs:
        print(f"{test_key}: {int(obj.get(test_key))}")
        assert_that(int(obj.get(test_key))).is_less_than_or_equal_to(int(expected_value))


def assert_sorted_ascending(body, test_key):
    """
    Validate that the specified key in the response body is sorted in ascending order.

    param body: The JSON response body.
    param test_key: The key whose values should be sorted.
    (True for ascending order).
    """
    objs = body['values']
    values = [int(obj[test_key]) for obj in objs]

    for i in range(len(values) - 1):
        print(f"{test_key}: {values[i]}")
        assert values[i] <= values[i + 1], f"Values for {test_key} are not sorted in ascending order: {values}"

    print(f"All values for {test_key} are sorted in ascending order.")


def assert_sorted_descending(body, test_key):
    """
    Validate that the specified key in the response body is sorted in descending order.

    param body: The JSON response body.
    param test_key: The key whose values should be sorted.
    (True for descending order).
    """
    objs = body['values']
    values = [int(obj[test_key]) for obj in objs]

    for i in range(len(values) - 1):
        print(f"{test_key}: {values[i]}")
        assert values[i] >= values[i + 1], f"Values for {test_key} are not sorted in descending order: {values}"

    print(f"All values for {test_key} are sorted in descending order.")


def assert_response_object_count(body, limit):
    """
    Validate that the number of objects in the response does not exceed the given limit.

    param body: The JSON response body.
    param limit: The expected maximum number of objects.
    """
    objs = body['values']
    obj_count = len(objs)

    print(f"Limit: {limit}, Object Count: {obj_count}")

    assert_that(obj_count).is_less_than_or_equal_to(limit).described_as(
        f"The number of objects returned in the response "
        f"{obj_count} does not match the expected value {limit}"
    )


def assert_tx_hash_filter(body, expected_value):
    """
    Validates that all objects in the response have the expected transaction hash (txHash).

    Args:
        body (dict): The API response body containing the 'values' and 'total' fields.
        expected_value (str): The expected transaction hash used in the request.

    Raises:
        AssertionError: If any object has a mismatched txHash or if the 'total' count is incorrect.
    """
    objs = body.get('values', [])

    # Validate txHash format
    tx_hash_pattern = r"^0x[a-fA-F0-9]{64}$"
    assert_that(expected_value).matches(tx_hash_pattern).described_as(
        f"Invalid txHash format: {expected_value}. Expected a valid Ethereum transaction hash."
    )

    for obj in objs:
        actual_tx_hash = obj.get('txHash', '')

        print(f"txHash: {actual_tx_hash}")
        assert_that(actual_tx_hash).is_equal_to(expected_value).described_as(
            f"The txHash filter is not working correctly."
            f" Expected txHash: '{expected_value}' in the params, but got txHash: '{actual_tx_hash}'"
            f" in the response objects."
        )

    assert_that(len(objs)).is_equal_to(int(body.get('total', 0))).described_as(
        f"Expected 'total' in response body ({body.get('total')}) to match the length of 'objs' ({len(objs)})"
    )


def assert_checking_the_eth_address_and_filter(body, test_key, expected_value):
    """
    Validates that all objects in the response have the expected Ethereum Address (40 characters or (20 bytes) value).

    Args:
        body (dict): The API response body containing the 'values' and 'total' fields.
        test_key: str
        expected_value: (eth address) The expected value used in the request.

    Raises:
        AssertionError: If any object has a mismatched Ethereum Address or if the 'total' count is incorrect.
    """
    objs = body.get('values', [])

    # Validate txHash format
    eth_address_pattern = r"^0x[a-fA-F0-9]{40}$"
    assert_that(expected_value).matches(eth_address_pattern).described_as(
        f"Invalid eth address format: {expected_value}. Expected a valid Ethereum address."
    )

    for obj in objs:
        actual_address = obj.get(test_key, '')

        print(f"{test_key}: {actual_address}")
        assert_that(actual_address).is_equal_to(expected_value).described_as(
            f"The {test_key} filter is not working correctly."
            f" Expected {test_key}: '{expected_value}' in the params, but got {test_key}: '{actual_address}'"
            f" in the response objects."
        )

    assert_that(len(objs)).is_equal_to(int(body.get('total', 0))).described_as(
        f"Expected 'total' in response body ({body.get('total')}) to match the length of 'objs' ({len(objs)})"
    )


def assert_sorted_ascending_with_hexadecimal_values(body, test_key):
    """
    Validate that the specified key in the response body is sorted in ascending order.

    param body: The JSON response body.
    param test_key: The key whose values should be sorted.
    (True for ascending order).
    """
    objs = body['values']
    values = [int(obj[test_key], 16) for obj in objs]

    for i in range(len(values) - 1):
        print(f"{test_key}: {values[i]}")
        assert values[i] <= values[i + 1], f"Values for {test_key} are not sorted in ascending order: {values}"

    print(f"All values for {test_key} are sorted in ascending order.")


def assert_sorted_descending_with_hexadecimal_values(body, test_key):
    """
    Validate that the specified key in the response body is sorted in ascending order.

    param body: The JSON response body.
    param test_key: The key whose values should be sorted.
    (True for ascending order).
    """
    objs = body['values']
    values = [int(obj[test_key], 16) for obj in objs]

    for i in range(len(values) - 1):
        print(f"{test_key} as integer: {values[i]}")
        assert values[i] >= values[i + 1], f"Values for {test_key} are not sorted in descending order: {values}"
    for elem in objs:
        print(f"All values for {elem[test_key]} are sorted in descending order.")
