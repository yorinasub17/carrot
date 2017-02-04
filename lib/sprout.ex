defmodule Sprout do
  #####
  # These define Bloom relevant statements
  #####
  @callback collections() :: any
  @callback statements() :: any

  ### Example
  #defp collections() do
  #  %{"foo" => :table,
  #    "bar" => :scratch}
  #end

  #defp statements() do
  #  [%Stmt{op: :sync_merge, lhs: "foo", rhs: "bar"}]
  #end
  ###
end
