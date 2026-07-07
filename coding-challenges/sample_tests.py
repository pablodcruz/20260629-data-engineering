"""Simple checks for the coding challenges.

These tests assume completed `starter_code.py`.
Run from this folder:

    python sample_tests.py
"""

from starter_code import (
    BankAccount,
    calculate_total,
    count_words,
    fibonacci,
    is_palindrome,
    is_prime,
    read_scores,
    reverse_string,
    safe_divide,
)


def run_tests():
    assert reverse_string("python") == "nohtyp"
    assert reverse_string("") == ""

    assert is_palindrome("Race car") is True
    assert is_palindrome("hello") is False

    assert is_prime(7) is True
    assert is_prime(10) is False
    assert is_prime(1) is False

    assert fibonacci(0) == []
    assert fibonacci(1) == [0]
    assert fibonacci(6) == [0, 1, 1, 2, 3, 5]

    words = ["apple", "banana", "apple", "orange", "banana", "apple"]
    assert count_words(words) == {"apple": 3, "banana": 2, "orange": 1}
    assert count_words([]) == {}

    assert calculate_total([10.00, 5.00], 0.08) == 16.2
    assert calculate_total([], 0.08) == 0

    account = BankAccount("Ava", 100)
    account.deposit(50)
    assert account.withdraw(30) is True
    assert account.get_balance() == 120
    assert account.withdraw(200) is False
    assert account.get_balance() == 120

    assert read_scores("sample_data/scores.txt") == 90.0

    assert safe_divide(10, 2) == 5.0
    assert safe_divide(10, 0) == "Cannot divide by zero"

    print("All sample tests passed.")


if __name__ == "__main__":
    run_tests()

