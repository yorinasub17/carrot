defmodule MacaroniPenguinTest do
  use ExUnit.Case
  doctest MacaroniPenguin

  defmodule Penguin do
    use MacaroniPenguin

    def collections() do
      %{"foo" => :table,
        "bar" => :scratch}
    end

    def statements() do
      [%Stmt{op: :sync_merge, lhs: "foo", rhs: "bar"}]
    end
  end

  setup do
    process = Penguin.start()
    {:ok, process: process}
  end

  test "full integration test of loop", %{process: process} do
    # insert into bud collection
    Penguin.insert(process, "bar", "hello")
    :timer.sleep(100)

    # Now verify the merge statement was executed
    foo_data = :ets.lookup(:penguin_process_state, "foo")
    assert foo_data == [{"foo", MapSet.new(["hello"])}]
    bar_data = :ets.lookup(:penguin_process_state, "bar")
    assert bar_data == []  # bar is a scratch collection, so it is wiped at the end

    # Now insert some more data
    Penguin.insert(process, "bar", "world")
    :timer.sleep(100)

    # ... and verify that it was added to "foo" because of the merge statement
    foo_data = :ets.lookup(:penguin_process_state, "foo")
    assert foo_data == [{"foo", MapSet.new(["hello", "world"])}]
    bar_data = :ets.lookup(:penguin_process_state, "bar")
    assert bar_data == []  # bar is still a scratch collection, so it is wiped at the end
  end
end
