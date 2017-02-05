defmodule Stmt do
  defstruct [
    :op,
    :lhs,
    :rhs
  ]

  # Merge (sync and async) statement
  # returns true if anything changed
  def execute(table, %Stmt{op: operator, lhs: left_name, rhs: right_name})
    when operator == :"<=" or operator == :"<+"
  do
    left = lookup_collection(table, left_name)
    right = lookup_collection(table, right_name)
    out = MapSet.union(left, right)
    :ets.insert(table, {left_name, out})

    # TODO: this is fairly inefficient - is there a way to detect this as part of the union above?
    if out == left do
      # the output is exactly the same as the original input, so nothing changed
      false
    else
      true
    end
  end

  # Delete statement
  def execute(table, %Stmt{op: :"<-", lhs: left_name, rhs: right_name}) do
    left = lookup_collection(table, left_name)
    right = lookup_collection(table, right_name)
    out = MapSet.difference(left, right)
    :ets.insert(table, {left_name, out})
  end

  ### Helpers
  defp lookup_collection(table, collection_name) do
    collection = :ets.lookup(table, collection_name)
    case collection do
      [] -> MapSet.new
      [{collection_name, x}] -> x
    end
  end
end
