defmodule Stmt do
  defstruct [
    :op,
    :lhs,
    :rhs
  ]

  def execute(table, %Stmt{op: :sync_merge, lhs: left_name, rhs: right_name}) do
    left = :ets.lookup(table, left_name)
    left = 
      case left do
        [] -> MapSet.new
        [{left_name, x}] -> x
      end
    right = :ets.lookup(table, right_name)
    right =
      case right do
        [] -> MapSet.new
        [{right_name, x}] -> x
      end
    out = MapSet.union(left, right)
    :ets.insert(table, {left_name, out})
  end
end
