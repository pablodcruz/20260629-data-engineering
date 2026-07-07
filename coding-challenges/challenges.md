# Basic Coding Assessment Challenges

Each challenge should be solved in Python using only the fundamentals covered during training.
Focus on readable code, clear variable names, and a simple explanation of the approach.

## 1. Reverse a String

Write a function named `reverse_string(text)` that returns the input string in reverse order.

Example:

```python
reverse_string("python")
```

Expected result:

```python
"nohtyp"
```

Things to consider:

* Empty strings should return an empty string.
* Spaces and punctuation should stay in their reversed positions.

## 2. Palindrome Check

Write a function named `is_palindrome(text)` that returns `True` if the input is a palindrome and `False` otherwise.

A palindrome reads the same forward and backward.
For this challenge, ignore capitalization and spaces.

Examples:

```python
is_palindrome("Race car")
is_palindrome("hello")
```

Expected results:

```python
True
False
```

Things to consider:

* Convert text to lowercase.
* Remove spaces before checking.

## 3. Prime Number

Write a function named `is_prime(number)` that returns `True` if the number is prime and `False` otherwise.

A prime number is greater than 1 and is divisible only by 1 and itself.

Examples:

```python
is_prime(7)
is_prime(10)
is_prime(1)
```

Expected results:

```python
True
False
False
```

Things to consider:

* Numbers less than 2 are not prime.
* Use a loop to check possible divisors.

## 4. Fibonacci Series

Write a function named `fibonacci(count)` that returns a list containing the first `count` Fibonacci numbers.

The Fibonacci series starts with `0` and `1`.
Each next number is the sum of the two previous numbers.

Examples:

```python
fibonacci(1)
fibonacci(6)
```

Expected results:

```python
[0]
[0, 1, 1, 2, 3, 5]
```

Things to consider:

* If `count` is `0`, return an empty list.
* Do not print the series; return it.

## 5. List and Dictionary Operations

Write a function named `count_words(words)` that accepts a list of words and returns a dictionary showing how many times each word appears.

Examples:

```python
count_words(["apple", "banana", "apple", "orange", "banana", "apple"])
```

Expected result:

```python
{"apple": 3, "banana": 2, "orange": 1}
```

Things to consider:

* Use a dictionary to store counts.
* The input list may be empty.

## 6. Functions

Write a function named `calculate_total(prices, tax_rate)` that returns the total cost after applying tax.

`prices` is a list of numbers.
`tax_rate` is a decimal, such as `0.08` for 8%.

Example:

```python
calculate_total([10.00, 5.00], 0.08)
```

Expected result:

```python
16.2
```

Things to consider:

* Add all prices first.
* Apply tax once to the subtotal.
* Round the final result to 2 decimal places.

## 7. Basic Object-Oriented Programming

Create a class named `BankAccount`.

The class should:

* Store the account owner's name.
* Store the account balance.
* Have a `deposit(amount)` method that adds money to the balance.
* Have a `withdraw(amount)` method that subtracts money from the balance if enough money is available.
* Have a `get_balance()` method that returns the current balance.

Example:

```python
account = BankAccount("Ava", 100)
account.deposit(50)
account.withdraw(30)
account.get_balance()
```

Expected result:

```python
120
```

Things to consider:

* Do not allow the balance to go below zero.
* Return `True` from `withdraw` when the withdrawal succeeds.
* Return `False` from `withdraw` when there is not enough money.

## 8. File Handling

Write a function named `read_scores(file_path)` that reads a text file containing one score per line and returns the average score.

Example file contents:

```text
80
90
100
```

Expected result:

```python
90.0
```

Things to consider:

* Open the file safely using `with open(...)`.
* Convert each line from text to a number.
* Ignore blank lines.
* Return `0` if the file has no scores.

## 9. Exception Handling

Write a function named `safe_divide(a, b)` that divides `a` by `b`.

The function should return the result when division is possible.
If `b` is zero, return the message `"Cannot divide by zero"`.

Examples:

```python
safe_divide(10, 2)
safe_divide(10, 0)
```

Expected results:

```python
5.0
"Cannot divide by zero"
```

Things to consider:

* Use `try` and `except`.
* Handle the specific error caused by division by zero.

## Interviewer Evaluation Guide

Look for:

* Clear problem understanding.
* Simple and correct Python syntax.
* Good use of variables, loops, conditionals, and functions.
* Ability to test with examples.
* Ability to explain the solution in plain language.
* Willingness to adjust after feedback.

Avoid grading based on:

* Advanced algorithm knowledge.
* Memorized one-line tricks.
* Competitive programming style.

