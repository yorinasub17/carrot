# all the program state is stored in ETS, via named variables that hold the list data.
# the only state that is stored in memory of the process are the named pointers and the buffer.
# therefore, state doesn't need to be explicitly updated here except for the buffer.
defmodule MacaroniPenguin do
  defmacro __using__(_opts) do
    quote do
      @behaviour Sprout

      defmodule State do
        defstruct [
          :table,
          :msg_buffer,
          :collections,
          :stmts
        ]
      end

      #####
      # Public API to access bloom process
      #####
      def start() do
        table = :ets.new(:penguin_process_state, [:set, :named_table, :public])
        initial_state = %State{
          table: table,
          msg_buffer: [],
          collections: collections(),
          stmts: statements()
        }
        spawn(fn -> loop(initial_state) end)
      end

      def ping(pid) do
        send(pid, :ping)
        :ok
      end

      def insert(pid, collection_name, value) do
        send(pid, {:insert, collection_name, value})
        :ok
      end
      #####
      # End public API
      #####

      #####
      # Bloom runtime
      #####

      # Main process loop: consume all messages from message box, and then process them
      defp loop(state = %State{msg_buffer: msg_buffer}) do
        timeout = 
          case msg_buffer do
            [] -> :infinity
            _ -> 0
          end

        receive do
          msg ->
            state = %{state | msg_buffer: [msg | state.msg_buffer]}
            loop(state)
        after
          timeout ->
            state = process(state)
            loop(state)
        end
      end

      # Main Bloom loop
      # 1. Consume and route all messages in buffer
      # 2. Execute the bloom statements
      # 3. Clear any scratch's
      # 4. Clear msg buffer
      defp process(state) do
        IO.puts("processing #{length(state.msg_buffer)}")
        new_buffer = consume_buffer(state.table, state.msg_buffer)
        execute_stmts(state.table, state.stmts)
        Enum.each(state.collections, fn(kv) -> clear_if_ephemeral(state.table, kv) end)
        %{state | msg_buffer: new_buffer}
      end

      # Helper to consume the msg buffer and route it
      defp consume_buffer(_table, []) do
        # base case
        []
      end
      defp consume_buffer(table, [msg | remaining]) do
        consume_msg(table, msg)
        consume_buffer(table, remaining)
      end

      # Msg router
      defp consume_msg(_table, :ping) do
        # do nothing
      end
      defp consume_msg(table, {:insert, collection_name, value}) do
        collection = :ets.lookup(table, collection_name)
        collection =
          case collection do
            [] -> MapSet.new
            [{collection_name, x}] -> x
          end
        :ets.insert(table, {collection_name, MapSet.put(collection, value)})
      end

      # Stmt executor
      defp execute_stmts(_table, []) do
        # base case
      end
      defp execute_stmts(table, [stmt | remaining]) do
        Stmt.execute(table, stmt)
        execute_stmts(table, remaining)
      end

      # Scratch clearer
      defp clear_if_ephemeral(table, {name, :scratch}) do
        :ets.delete(table, name)
      end
      defp clear_if_ephemeral(_, _) do
        # do nothing, because not collection is not ephemeral
      end
    end
  end
end
