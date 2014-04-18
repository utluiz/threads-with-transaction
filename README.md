Threads with Transaction
========================

An JavaSE example of multiples threads sharing a single JTA transaction using Atomikos.

There are two simple tests for success and failure.

Basically, this project connects to PostgreSQL and try to update a column of `test` table. 5 threads are started and each one try to update one row of the table.

In the success test, all columns are flagged with "ok".

The failure test try to flag "fail" but all the last one always fail, rolling back all changes.
