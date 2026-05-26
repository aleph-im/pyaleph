from aleph.types.content_format import ContentFormat


def test_content_format_values():
    assert ContentFormat.FULL.value == "full"
    assert ContentFormat.HEADERS.value == "headers"
    assert ContentFormat.NONE.value == "none"
    # str-enum: compares equal to its raw value
    assert ContentFormat.HEADERS == "headers"
