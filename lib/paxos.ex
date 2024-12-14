defmodule Paxos do

    defmodule Participant do

        defmodule Proposer do

            def start(name) do
                pid = spawn(Proposer, :init, [name])
                case :global.re_register_name(name, pid) do
                    :yes ->
                        IO.puts("Registered proposer #{name}")
                        {:ok, pid}
                    :no ->
                        {:error, :registration_failed}
                end
            end

            def init(name) do
                IO.puts("Init function called!")
                run()
            end

            def run() do
                IO.puts("Run function called!")
            end

        end

        defmodule Acceptor do

            def start(name) do
                pid = spawn(Acceptor, :init, [name])
                case :global.re_register_name(name, pid) do
                    :yes ->
                        IO.puts("Registered proposer #{name}")
                        {:ok, pid}
                    :no ->
                        {:error, :registration_failed}
                end
            end

            def init(name) do
                IO.puts("Init function called!")
                run()
            end

            def run() do
                IO.puts("Run function called!")
            end

        end

        defmodule Learner do

            def start(name) do
                pid = spawn(Learner, :init, [name])
                case :global.re_register_name(name, pid) do
                    :yes ->
                        IO.puts("Registered proposer #{name}")
                        {:ok, pid}
                    :no ->
                        {:error, :registration_failed}
                end
            end

            def init(name) do
                IO.puts("Init function called!")
                run()
            end

            def run() do
                IO.puts("Run function called!")
            end

        end

    end

end
