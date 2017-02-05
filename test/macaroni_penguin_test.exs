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
      [%Stmt{op: :"<=", lhs: "foo", rhs: "bar"}]
    end
  end

  defmodule LoopingPenguin do
    use MacaroniPenguin

    def collections() do
      %{"loopingzing" => :table,
        "loopingfoo" => :scratch,
        "loopingbar" => :scratch,
        "loopingbaz" => :scratch}
    end

    def statements() do
      # loopingfoo <= loopingbar
      # loopingbaz <= loopingfoo
      # loopingzing <= loopingbaz
      [%Stmt{op: :"<=", lhs: "loopingbaz", rhs: "loopingfoo"},
       %Stmt{op: :"<=", lhs: "loopingfoo", rhs: "loopingbar"},
       %Stmt{op: :"<=", lhs: "loopingzing", rhs: "loopingbaz"}]
    end
  end

  defmodule AsyncPenguin do
    use MacaroniPenguin

    def collections() do
      %{"asyncfoo" => :table,
        "asyncbar" => :scratch,
        "asyncbaz" => :scratch}
    end

    def statements() do
      # asyncbaz <= asyncbar
      # asyncfoo <+ asyncbaz
      [%Stmt{op: :"<=", lhs: "asyncbaz", rhs: "asyncbar"},
       %Stmt{op: :"<+", lhs: "asyncfoo", rhs: "asyncbaz"}]
    end
  end

  defmodule DeletePenguin do
    use MacaroniPenguin

    def collections() do
      %{"deletefoo" => :table,
        "deletebar" => :scratch}
    end

    def statements() do
      # deletefoo <- deletebar
      [%Stmt{op: :"<-", lhs: "deletefoo", rhs: "deletebar"}]
    end
  end

  test "full integration test of loop" do
    process = Penguin.start()

    # insert into bud collection
    Penguin.insert(process, "bar", "hello")
    :timer.sleep(100)

    # Now verify the merge statement was executed
    assert :ets.lookup(:penguin_process_state, "foo") == [{"foo", MapSet.new(["hello"])}]
    assert :ets.lookup(:penguin_process_state, "bar") == []  # bar is a scratch collection, so it is wiped at the end

    # Now insert some more data
    Penguin.insert(process, "bar", "world")
    :timer.sleep(100)

    # ... and verify that it was added to "foo" because of the merge statement
    assert :ets.lookup(:penguin_process_state, "foo") == [{"foo", MapSet.new(["hello", "world"])}]
    assert :ets.lookup(:penguin_process_state, "bar") == []  # bar is still a scratch collection, so it is wiped
  end

  test "looping synchronous statements" do
    process = LoopingPenguin.start()

    # insert into bud collection
    LoopingPenguin.insert(process, "loopingbar", "hello")
    :timer.sleep(100)

    # Verify the merge statements were executed properly
    assert :ets.lookup(:penguin_process_state, "loopingzing") == [{"loopingzing", MapSet.new(["hello"])}]

    # Verify all the scratch collections were emptied
    assert :ets.lookup(:penguin_process_state, "loopingfoo") == []
    assert :ets.lookup(:penguin_process_state, "loopingbar") == []
    assert :ets.lookup(:penguin_process_state, "loopingbaz") == []
  end

  test "asynchronous statements will merge data as well" do
    process = AsyncPenguin.start()

    # insert into bud collection
    AsyncPenguin.insert(process, "asyncbar", "hello")
    :timer.sleep(100)

    # Verify the merge statements were executed properly
    assert :ets.lookup(:penguin_process_state, "asyncfoo") == [{"asyncfoo", MapSet.new(["hello"])}]

    # Verify all the scratch collections were emptied
    assert :ets.lookup(:penguin_process_state, "asyncbar") == []
    assert :ets.lookup(:penguin_process_state, "asyncbaz") == []
  end

  test "delete statements are executed" do
    process = DeletePenguin.start()

    # setup to make sure there is already some data
    :ets.insert(:penguin_process_state, {"deletefoo", MapSet.new(["hello", "world"])})

    # now trigger a deletion
    DeletePenguin.insert(process, "deletebar", "hello")
    :timer.sleep(100)

    # Verify the deletion statement was executed
    assert :ets.lookup(:penguin_process_state, "deletefoo") == [{"deletefoo", MapSet.new(["world"])}]

    # Verify scratch collections were emptied
    assert :ets.lookup(:penguin_process_state, "deletebar") == []
  end
end
