defmodule StmtTest do
  use ExUnit.Case, async: true
  doctest Stmt

  setup do
    table = :ets.new(:test_table, [:set, :named_table, :public])
    {:ok, table: table}
  end

  test "sync_merge nonexistent collections still works", %{table: table} do
    # Verify initial state, which is the table doesn't have the collections
    assert :ets.lookup(table, "foo") == []
    assert :ets.lookup(table, "bar") == []

    # now execute a sync_merge
    Stmt.execute(table, %Stmt{op: :"<=", lhs: "foo", rhs: "bar"})

    # ... and verify that "foo" is now an empty MapSet, but "bar" still doesn't exist
    assert :ets.lookup(table, "foo") == [{"foo", %MapSet{}}]
    assert :ets.lookup(table, "bar") == []
  end

  test "sync_merge full collections will merge into the lhs", %{table: table} do
    # Verify initial state, which is the table doesn't have the collections
    assert :ets.lookup(table, "name") == []
    assert :ets.lookup(table, "back_to_the_future") == []

    # seed the "back_to_the_future" table with some items
    set = MapSet.new(["Marty", "Doc"])
    :ets.insert(table, {"back_to_the_future", set})

    # now execute a sync_merge
    Stmt.execute(table, %Stmt{op: :"<=", lhs: "name", rhs: "back_to_the_future"})

    # ... and verify that the "name" table has been filled
    assert :ets.lookup(table, "name") == [{"name", set}]
    # also verify it didn't clear the "back_to_the_future" table
    assert :ets.lookup(table, "back_to_the_future") == [{"back_to_the_future", set}]
  end

  test "async_merge nonexistent collections still works", %{table: table} do
    # Verify initial state, which is the table doesn't have the collections
    assert :ets.lookup(table, "asyncfoo") == []
    assert :ets.lookup(table, "asyncbar") == []

    # now execute a async_merge
    Stmt.execute(table, %Stmt{op: :"<+", lhs: "asyncfoo", rhs: "asyncbar"})

    # ... and verify that "foo" is now an empty MapSet, but "bar" still doesn't exist
    assert :ets.lookup(table, "asyncfoo") == [{"asyncfoo", %MapSet{}}]
    assert :ets.lookup(table, "asyncbar") == []
  end

  test "async_merge full collections will merge into the lhs", %{table: table} do
    # Verify initial state, which is the table doesn't have the collections
    assert :ets.lookup(table, "asyncname") == []
    assert :ets.lookup(table, "asyncback_to_the_future") == []

    # seed the "back_to_the_future" table with some items
    set = MapSet.new(["Marty", "Doc"])
    :ets.insert(table, {"asyncback_to_the_future", set})

    # now execute a sync_merge
    Stmt.execute(table, %Stmt{op: :"<+", lhs: "asyncname", rhs: "asyncback_to_the_future"})

    # ... and verify that the "name" table has been filled
    assert :ets.lookup(table, "asyncname") == [{"asyncname", set}]
    # also verify it didn't clear the "back_to_the_future" table
    assert :ets.lookup(table, "asyncback_to_the_future") == [{"asyncback_to_the_future", set}]
  end

end
