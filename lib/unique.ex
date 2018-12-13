
defmodule Unique do

  def flow_parse(file_path, chunk_size, output_file) do
    file_path
      |> File.stream!(read_ahead: chunk_size)
      |> Stream.drop(1)
      |> Flow.from_enumerable
      |> Flow.map(&String.split(&1, ",") |> List.first)
      |> Flow.partition
      |> Flow.uniq
      |> Flow.map(&"#{&1}\n")
      |> Stream.into(File.stream!(output_file, [:write, :utf8]))
      |> Stream.run
  end

  def stream_parse(file_path, chunk_size, output_file) do
    file_path
      |> File.stream!(read_ahead: chunk_size)
      |> Stream.drop(1)
      |> Stream.map(&String.split(&1, ",") |> List.first)
      |> Stream.uniq
      |> Stream.map(&"#{&1}\n")
      |> Stream.into(File.stream!(output_file, [:write, :utf8]))
      |> Stream.run
  end

  def alt_flow_parse_dir(path, out_file, chunk_size) do
    concat_unique =  File.open!(path <> "/" <> out_file, [:read, :utf8, :write])

    Path.wildcard(path <> "/*.csv")
      |> Flow.from_enumerable
      |> Flow.map(&append_to_file(&1, path, concat_unique, chunk_size))
      |> Flow.run

    File.close(concat_unique)
  end

  def append_to_file(filename, path, out_file, chunk_size) do
    file = filename
      |> String.split("/")
      |> Enum.take(-1)
      |> List.to_string
    path <> file
      |> File.stream!
      |> Stream.drop(1)
      |> Flow.from_enumerable
      |> Flow.map(&String.split(&1, ",") |> List.first)
      |> Flow.map(&String.trim(&1,"\n"))
      |> Flow.partition
      |> Stream.chunk(chunk_size, chunk_size, [])
      |> Flow.from_enumerable
      |> Flow.map(fn chunk ->
          chunk
            |> MapSet.new
            |> MapSet.to_list
            |> List.flatten
        end)
      |> Flow.map(fn line ->
          Enum.map(line, fn item ->
              IO.puts(out_file, item)
            end)
          end)
       |> Flow.run
  end

  # run first -> get a unique file for each file it reads. !!!! USE alt_flow_parse_dir
  def flow_parse_dir(path, chunk_size) do
    Path.wildcard(path <> "/*.csv")
      |> Flow.from_enumerable
      |> Flow.map(fn filename ->
          file = filename
            |> String.split("/")
            |> Enum.take(-1)
            |> List.to_string
          flow_parse(filename, chunk_size, path <> "unique_" <> file)
        end)
      |> Flow.run
  end

  # run second -> write contentes of all unique files to one file !!! USE alt_flow_parse_dir
  def concat_files(path, totol_unique_file_name) do
    sum_file =  File.open!(path <> "/" <> totol_unique_file_name, [:read, :utf8, :write])

    Path.wildcard(path <> "/*.csv")
      |> Stream.map(fn filename ->
          file = filename
            |> String.split("/")
            |> Enum.take(-1)
            |> List.to_string
           if String.contains?(file, "unique") do
             write_concat_of_unique_files(file, path, sum_file)
           end
        end)
      |> Stream.run

    File.close(sum_file)
  end

  # run third, fourth, 5th, till the final
  # resulting file has no more duplicates.
  def unique_column(path, file_name, chunk_size, output) do
    total_file = File.open!(path <> "/" <> output, [:read, :utf8, :write])

    path <> "/" <> file_name
      |> File.stream!(read_ahead: chunk_size)
      |> Stream.map(&String.trim(&1,"\n"))
      |> Stream.chunk(chunk_size, chunk_size, [])
      |> Flow.from_enumerable
      |> Flow.map(fn chunk ->
          chunk
            |> MapSet.new
            |> MapSet.to_list
            |> List.flatten
        end)
      |> Flow.partition
      |> Flow.map(fn line ->
          Enum.map(line, fn item ->
              IO.puts(total_file, item)
            end)
          end)
      |> Flow.run

    File.close(total_file)
  end

  # run check after unique_column
  def check_unique(file_path) do
    original_length = file_path
      |> File.stream!
      |> Enum.to_list

    unique_length = file_path
      |> File.stream!
      |> Stream.uniq
      |> Enum.to_list

    ^unique_length = original_length
  end

  def write_concat_of_unique_files(file, path, totol_unique_file_name) do
    # read in file contets line by line
    path <> "/" <> file
      |> File.stream!()
      |> Stream.map(&String.trim(&1,"\n"))
      |> Stream.map(fn line ->
          IO.puts(totol_unique_file_name, line)
        end)
      |> Stream.run
  end

end
