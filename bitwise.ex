
defmodule BitwiseNif do

  @moduledoc """
  BitwiseNif: NIF example module showing different NIF scheduling issues

  This is an Elixir and Rust port of

  The oricinal C and Erlang code were originally written by Steve Vinoski
  for bitwise at https://github.com/vinoski/bitwise.
  This code was originally presented at Chicago Erlang on 22 Sep
  2014. Please see the PDF file in this repository for the presentation.

  The exor function variants here all take a binary and a byte value as
  arguments and return a binary and either the number of times the
  scheduler thread was yielded (if known) or the number of chunks of the
  binary that were processed. The returned binary is the same size as the
  binary argument, and its value is that of the binary argument with the
  byte argument xor'd with each byte of the binary. The idea is that if
  you pass in a large enough binary, you can get bad or good NIF behavior
  with respect to Erlang scheduler threads depending on which function
  variant you call, and different calls take different approaches to
  trying to avoid scheduler collapse and other scheduling problems.

  This code requires Erlang 17.3 or newer, built with dirty schedulers
  enabled.
  """

  @on_load :init

  # @TODO: There should be an attribute or something that has the module name.
  @module BitwiseNif

  @doc """
  With a large bin argument, `exor/2` and `exor_bad/2` take far too
  long for a NIF
  """
  def exor(bin, byte) when is_binary(bin) and byte >= 0 and byte < 256 do
    :erlang.nif_error({:nif_not_loaded, @module})
  end

  @doc """
  With a large bin argument, `exor/2` and `exor_bad/2` take far too
  long for a NIF
  """
  def exor_bad(bin, byte) when is_binary(bin) and byte >= 0 and byte < 256 do
    :erlang.nif_error({:nif_not_loaded, @module})
  end

  @doc """
  `exor_yield/2` processes bin in chunks and uses `enif_schedule_nif`
  to yield the scheduler thread between chunks.
  """
  def exor_yield(bin, byte) when is_binary(bin) and byte >= 0 and byte < 256 do
    :erlang.nif_error({:nif_not_loaded, @module})
  end

  @doc """
  exor_dirty processes bin on a dirty scheduler.
  """
  def exor_dirty(bin, byte) when is_binary(bin) and byte >= 0 and byte < 256 do
    :erlang.nif_error({:nif_not_loaded, @module})
  end

  @doc """
  Similar to `exor_yield/2` but do the chunking in Elixir.
  """
  def exor_chunks(bin, byte) when is_binary(bin) and byte >= 0 and byte < 256 do
    exor_chunks(bin, byte, 4194304, 0, <<>>)
  end

  def exor_chunks(bin, byte, chunk_size, yields, acc) do
    case byte_size(bin) do
        size when size > chunk_size ->
            <<chunk :: size(chunk_size), rest :: binary>> = bin
            {res, _} = exor_bad(chunk, byte)
            exor_chunks(rest, byte, chunk_size,
                        yields + 1, <<acc :: binary, res :: binary>>)
        _ ->
            {res, _} = exor_bad(bin, byte)
            {<<acc :: binary, res :: binary>>, yields}
    end
  end

  @doc """
  Count reductions and number of scheduler yields for `fun`. `fun` is
  assumed to be one of the above exor variants.
  """
  def reds(bin, byte, fun) when is_binary(bin) and byte >= 0 and byte < 256 do
    parent = self()
    pid = spawn(fn() ->
      self = self()
      start = :os.timestamp
      r0 = :erlang.process_info(self, :reductions)
      {_, yields} = fun.(bin, byte)
      r1 = :erlang.process_info(self, :reductions)
      # Use new time API
      t = :timer.now_diff(:os.timestamp, start)
      send(parent, {self, {t, yields, r0, r1}})
    end)

    receive do
        {^pid, result} ->
            result
    end
  end

  def init() do
    # so_name = :filename.join(case :code.priv_dir(@module) do
    #                            {:error, :bad_name} ->
    #                              dir = :code.which(@module)
    #                              :filename.join([:filename.dirname(dir),
    #                                              '..', 'priv'])
    #                            dir ->
    #                                 dir
    #                         end, :erlang.atom_to_list(@module) ++ '_nif'),
    so_name = 'target/release/libbitwise_nif'
    :erlang.load_nif(so_name, 0)
  end

end
