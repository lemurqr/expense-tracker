from expense_tracker import format_currency_whole_dollars


def test_format_currency_whole_dollars_rounding_and_commas():
    assert format_currency_whole_dollars(7142.92) == "$7,143"
    assert format_currency_whole_dollars(1008.80) == "$1,009"
    assert format_currency_whole_dollars(1234567.89) == "$1,234,568"
    assert format_currency_whole_dollars(0) == "$0"


def test_format_currency_whole_dollars_negative_and_blank_values():
    assert format_currency_whole_dollars(-35.93) == "-$36"
    assert format_currency_whole_dollars(None) == "$0"
    assert format_currency_whole_dollars("") == "$0"
