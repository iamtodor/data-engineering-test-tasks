# Interview case

The African telecom market is mainly prepaid. This means that the Telco operators need to rely on point of sales (POS) in order to sell recharge to their customers. So it's crucial for the operators to make sure that their POS have enough stock for their daily operations.

This exercise is to detect which POS is recurrently out of stock so that our customers can act on them.

In this exercise, you are given the following inputs:

- a list of transactions per point of sales
- the stock level of every point of sale at some point in time

Data transactions

| Column             | type      |
|--------------------|-----------|
| date               | TIMESTAMP |
| terminal_id        | TEXT      |
| pos_id             | TEXT      |
| stock_balance      | DOUBLE    |

Stock level

| Column             | type      |
|--------------------|-----------|
| date               | TIMESTAMP |
| terminal_id        | TEXT      |
| pos_id             | TEXT      |
| transaction_amount | DOUBLE    |
| stock_balance      | DOUBLE    |

## Problem

The output of this exercise is to have a boolean flag on every POS on whether they are recurrently out of stock

For a POS to be considered recurrently out of stock, the following conditions are required:

- The POS has moved from with stock to out of stock at least twice
- It spent at least 4 hours out of stock

For a POS to be considered out of stock, its stock balance needs to be lower than 150% of its average daily sales
