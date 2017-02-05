defmodule Stmt do
  defstruct [
    :op,
    :lhs,
    :rhs
  ]

  # returns true if anything changed
  def execute(table, %Stmt{op: operator, lhs: left_name, rhs: right_name})
    when operator == :"<=" or operator == :"<+"
  do
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

    # TODO: this is fairly inefficient - is there a way to detect this as part of the union above?
    if out == left do
      # the output is exactly the same as the original input, so nothing changed
      false
    else
      true
    end
  end
end
